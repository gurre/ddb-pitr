// Package coordinator drives a restore: it loads and verifies the export, reads its
// data files into a pool of writers, checkpoints their progress and reports the outcome.
//
// Reading and writing are separate pools joined by a channel of decoded items. A data
// file holds one contiguous slice of the exported table's key space, so a batch built
// from one file is a batch aimed at one target partition; drawing every batch from all
// the files open at once is what lets a restore use the whole table rather than a few
// partitions of it.
package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gurre/ddb-pitr/checkpoint"
	"github.com/gurre/ddb-pitr/config"
	"github.com/gurre/ddb-pitr/itemimage"
	"github.com/gurre/ddb-pitr/manifest"
	"github.com/gurre/ddb-pitr/metrics"
	"github.com/gurre/ddb-pitr/writer"
)

// Streamer delivers the lines of one data file in order. The offset handed to fn is
// the line's position in the decompressed stream, counted from the start of the file,
// which is the only position the coordinator ever records or compares. The offset
// argument to Stream is a position in the stored object as S3 holds it; the
// coordinator always passes streamFromStart, because a decompressed position has no
// meaning there. See worker for why.
type Streamer interface {
	Stream(ctx context.Context, bucket, key string, offset int64, fn func(line []byte, offset int64) error) error
}

// streamFromStart is the only stored-object offset a worker asks for. Checkpoint
// offsets are decompressed positions; handing one to the streamer would range into the
// middle of a gzip member.
const streamFromStart int64 = 0

// noOffset is what progress reports for a file nothing has been written from yet.
// Zero cannot mean that, since the first line of every file sits at offset zero.
const noOffset int64 = -1

// WorkerStatus is what one goroutine of either pool has done, for the progress line.
// Readers report activity and errors; only writers ever add items and batches.
// Fields are ordered largest-to-smallest for optimal memory alignment.
type WorkerStatus struct {
	LastErrorTime time.Time // When the last error occurred (24 bytes)
	StartTime     time.Time // When the worker started (24 bytes)
	LastActive    time.Time // Last activity timestamp (24 bytes)
	LastError     error     // Last error encountered (16 bytes - interface)
	ItemsWritten  int64     // Number of items written (8 bytes)
	BatchesCount  int64     // Number of batches processed (8 bytes)
	ID            int       // Worker identifier (8 bytes on 64-bit)
	IsReader      bool      // Whether this is a reader rather than a writer (1 byte)
	// Busy is whether this member has work in hand: a file for a reader, a batch for a
	// writer. A member with nothing to do is not counted among the active ones, because
	// having nothing to do is the very thing the count is there to show.
	Busy bool
}

// ReportUploader uploads reports to S3.
type ReportUploader interface {
	UploadReport(ctx context.Context, uri string, report metrics.Report) error
}

// Backoffer paces the wait between attempts at a file whose stream failed.
// Wait reports false when the wait was cut short because the context ended,
// which callers must treat as "stop retrying".
type Backoffer interface {
	Wait(ctx context.Context, attempt int) bool
}

// errBackoffStopped stands in when a wait was cut short but the context reports no
// error. Returning the context's nil error there would let the worker fall through and
// record an unfinished file as complete, which a resume would then skip.
var errBackoffStopped = errors.New("coordinator: backoff stopped before the file was finished")

// ErrInterrupted is returned when the run stopped because the caller's context ended
// rather than because the export was finished. Progress has been recorded, so running
// the same command again carries on from where it stopped. It wraps the context's own
// error, so a caller can still tell a signal from a deadline.
var ErrInterrupted = errors.New("restore interrupted before it finished")

// ErrRecordsSkipped is returned when the restore ran to the end but skipped lines it
// could not decode. The table holds everything else the export contained, so this is
// told apart from a restore that did not finish; the two call for different responses.
var ErrRecordsSkipped = errors.New("restore finished but skipped records")

// stopRetrying reports why the retry loop is giving up. It never returns nil.
func stopRetrying(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errBackoffStopped
}

// Option adjusts optional Coordinator behaviour.
type Option func(*Coordinator)

// WithStreamBackoff replaces the pacing between attempts at a file whose stream failed.
// The default doubles from one second up to thirty.
// Example:
//
//	coord := coordinator.NewCoordinator(cfg, loader, streamer, parser, w, store, uploader, m,
//	    coordinator.WithStreamBackoff(writer.NewExponentialBackoff(time.Second, time.Minute)))
func WithStreamBackoff(b Backoffer) Option {
	return func(c *Coordinator) {
		c.backoff = b
	}
}

// WithBatchLinger replaces how long a partly filled batch waits for more items before
// it is written anyway. The default is 50ms. Shorter trades larger requests for lower
// latency at the tail of a restore; it may not be zero, which would send every item as
// its own request.
// Example:
//
//	coord := coordinator.NewCoordinator(cfg, loader, streamer, parser, w, store, uploader, m,
//	    coordinator.WithBatchLinger(10*time.Millisecond))
//
// withStampInterval replaces how often a reader reports that it is still working, so a
// test can drive that without a file slow enough to need it.
func withStampInterval(d time.Duration) Option {
	return func(c *Coordinator) {
		c.stamp = d
	}
}

// withConsole replaces where the progress line and the messages beside it are written,
// so a test can read what a restore reported.
func withConsole(progress, messages io.Writer) Option {
	return func(c *Coordinator) {
		c.console = newConsole(progress, messages)
	}
}

func WithBatchLinger(d time.Duration) Option {
	return func(c *Coordinator) {
		if d <= 0 {
			panic(fmt.Sprintf("coordinator: batch linger %s is not positive", d))
		}
		c.linger = d
	}
}

// Coordinator runs the restore: worker coordination, checkpointing and progress
// reporting.
type Coordinator struct {
	cfg            *config.Config
	manifest       manifest.Loader
	streamer       Streamer
	parser         itemimage.Decoder
	writer         writer.Writer
	store          checkpoint.Store
	metrics        *metrics.Metrics
	reportUploader ReportUploader
	backoff        Backoffer
	console        *console      // Owns the progress line and anything printed beside it
	linger         time.Duration // How long a partly filled batch waits for more items
	stamp          time.Duration // How often a reader reports that it is still working

	// progress owns what the restore has finished; the store is only ever written
	// from a snapshot of it, taken under saveMu so the object in S3 advances in the
	// same order the snapshots were taken.
	progress *progress
	saveMu   sync.Mutex

	// Per-goroutine status of both pools, for the progress line
	workerStatus map[statusKey]*WorkerStatus
	statusMu     sync.RWMutex

	// Progress tracking for percentage and throughput calculation
	corruptNamed       atomic.Int64 // Skipped lines named on stderr so far
	totalExpectedItems int64        // Total items expected from manifest
	totalFiles         int          // Data files the export contains
	lastReportTime     time.Time    // Last progress report timestamp
	lastReportItems    int64        // Items count at last report
	lastReportBytes    int64        // Bytes count at last report
}

// progress is the single owner of how far the restore has got. Workers report into
// it and it is the only source of what gets checkpointed, so two workers finishing
// different files cannot overwrite each other's progress the way independent writes
// to a shared store would.
// Fields are ordered largest-to-smallest for memory alignment.
type progress struct {
	exportID  string              // Export this progress belongs to
	completed map[string]struct{} // Files processed to the end
	offsets   map[string]int64    // Offset of the last line written, per file still in progress
	skipped   int64               // Lines that could not be decoded, over every run so far
	mu        sync.Mutex
}

// newProgress reopens the progress a previous run recorded for the given export.
func newProgress(exportID string, state checkpoint.State) *progress {
	p := &progress{
		completed: make(map[string]struct{}, len(state.Completed)),
		offsets:   make(map[string]int64, len(state.Offsets)),
		exportID:  exportID,
		skipped:   state.Skipped,
	}
	for _, key := range state.Completed {
		p.completed[key] = struct{}{}
	}
	for key, offset := range state.Offsets {
		p.offsets[key] = offset
	}
	return p
}

// resume reports the offset of the last line written from a file, noOffset when none
// has been, and whether the file is finished already.
func (p *progress) resume(key string) (offset int64, done bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.completed[key]; ok {
		return noOffset, true
	}
	if offset, ok := p.offsets[key]; ok {
		return offset, false
	}
	return noOffset, false
}

// record notes the offset of the last line written from a file. It never moves
// backwards: a retried stream re-delivers lines that were already written, and letting
// it lower the offset would make a later resume write them a third time.
func (p *progress) record(key string, offset int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if recorded, ok := p.offsets[key]; ok && recorded >= offset {
		return
	}
	p.offsets[key] = offset
}

// complete marks a file finished. Its offset is dropped, since a completed file is
// never resumed and keeping it would grow the checkpoint for the whole export.
func (p *progress) complete(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.offsets, key)
	p.completed[key] = struct{}{}
}

// skip counts a line that could not be decoded. The count spans runs, because the line
// is in a file a resume will not read again: once the file is finished, this is the only
// record left that the line was ever there.
func (p *progress) skip() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.skipped++
}

// completedCount reports how many data files the restore has finished, over every run
// so far: a resumed run inherits what an earlier one completed.
func (p *progress) completedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.completed)
}

// skipped reports how many lines this restore could not decode, over every run so far.
func (p *progress) skippedCount() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.skipped
}

// snapshot renders the current progress as the state to persist. Completed files are
// sorted so two checkpoints of the same progress are byte-identical.
func (p *progress) snapshot() checkpoint.State {
	p.mu.Lock()
	defer p.mu.Unlock()

	completed := make([]string, 0, len(p.completed))
	for key := range p.completed {
		completed = append(completed, key)
	}
	sort.Strings(completed)

	offsets := make(map[string]int64, len(p.offsets))
	for key, offset := range p.offsets {
		offsets[key] = offset
	}

	return checkpoint.State{ExportID: p.exportID, Completed: completed, Offsets: offsets, Skipped: p.skipped}
}

// NewCoordinator creates a new Coordinator instance with all required dependencies
func NewCoordinator(
	cfg *config.Config,
	manifest manifest.Loader,
	streamer Streamer,
	parser itemimage.Decoder,
	w writer.Writer,
	store checkpoint.Store,
	reportUploader ReportUploader,
	m *metrics.Metrics,
	opts ...Option,
) *Coordinator {
	c := &Coordinator{
		cfg:            cfg,
		manifest:       manifest,
		streamer:       streamer,
		parser:         parser,
		writer:         w,
		store:          store,
		metrics:        m,
		reportUploader: reportUploader,
		backoff:        writer.NewExponentialBackoff(time.Second, 30*time.Second),
		console:        newConsole(os.Stdout, os.Stderr),
		linger:         defaultBatchLinger,
		stamp:          defaultStampInterval,
		progress:       newProgress("", checkpoint.State{}),
		workerStatus:   make(map[statusKey]*WorkerStatus),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Run restores the export: it loads the manifest and the checkpoint, verifies the data
// files still to do, and drives the worker pool over them. The caller's context is
// what stops a run early; the caller owns any signal handling. However the run ends,
// the checkpoint is saved once the workers have stopped, within the configured
// shutdown timeout, so a resume picks up where this run got to.
func (c *Coordinator) Run(ctx context.Context) error {
	// The configuration is validated by the caller; a zero shutdown timeout is the
	// sign it was not, and would make the final save fail on every run.
	if c.cfg.ShutdownTimeout <= 0 {
		return fmt.Errorf("configuration was not validated: shutdown timeout is %s", c.cfg.ShutdownTimeout)
	}

	// Parse S3 URI to validate it
	u, err := url.Parse(c.cfg.ExportS3URI)
	if err != nil {
		return fmt.Errorf("invalid S3 URI: %w", err)
	}
	if u.Scheme != "s3" {
		return fmt.Errorf("invalid S3 URI scheme: %s", u.Scheme)
	}

	// Load manifest
	summary, err := c.manifest.Load(ctx, c.cfg.ManifestURI())
	if err != nil {
		return fmt.Errorf("failed to load manifest: %w", err)
	}

	// A checkpoint is tied to its export by the export's identity. A manifest without
	// one cannot be checked against any checkpoint, so it is refused before a run could
	// record progress that a later run of some other export would resume from.
	if summary.ExportARN == "" {
		return fmt.Errorf("manifest at %s names no export ARN", c.cfg.ManifestURI())
	}

	// Load checkpoint
	state, err := c.store.Load(ctx)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}
	// Resuming one export from another's progress would skip files by name collision
	// and report the result as complete, so a mismatch is a wiring error to stop on.
	if state.ExportID != "" && state.ExportID != summary.ExportARN {
		return fmt.Errorf("checkpoint records progress for export %s, not %s",
			state.ExportID, summary.ExportARN)
	}
	c.progress = newProgress(summary.ExportARN, state)

	// Progress is written once before anything else, which settles two questions while
	// the table is still untouched: whether the checkpoint can be written at all, and
	// whether another restore already owns it. Left until the first file finished, a
	// bucket that only grants reads, or a second restore sharing one checkpoint, would
	// be found out with the table part-way restored and hours of reading to redo.
	if saveErr := c.saveProgress(ctx); saveErr != nil {
		return fmt.Errorf("failed to record progress before starting: %w", saveErr)
	}

	// Verify the files still to do against what the manifest recorded, before writing
	// anything. A file that no longer matches means the restore would write data the
	// export never contained, which is worth failing on while the table is untouched.
	// Files a previous run finished are not checked again: they were checked before
	// they were read, and checking a copied file means reading it in full. The bucket
	// checked is the one the workers read from, not the one the manifest names; they
	// differ for an export that was copied since it was taken.
	remaining := summary
	remaining.DataFiles = make([]manifest.FileMeta, 0, len(summary.DataFiles))
	for _, file := range summary.DataFiles {
		if _, done := c.progress.resume(file.Key); !done {
			remaining.DataFiles = append(remaining.DataFiles, file)
		}
	}
	c.console.line("Verifying %d data files against the manifest", len(remaining.DataFiles))
	verification, err := c.manifest.VerifyChecksums(ctx, c.cfg.GetExportBucketName(), remaining)
	if err != nil {
		return fmt.Errorf("export failed verification: %w", err)
	}
	c.console.line("Verified %d of %d data files against the manifest",
		verification.Verified, len(remaining.DataFiles))
	if len(verification.Unverified) > 0 {
		c.console.line("%d data files carry no checksum that can be compared", len(verification.Unverified))
	}

	// Store what the export promised, for the progress line
	c.totalExpectedItems = summary.ItemCount
	c.totalFiles = len(summary.DataFiles)
	c.lastReportTime = time.Now()

	// The pool runs under its own cancellation so one worker's failure stops the rest.
	// A file that cannot be restored ends the run at once, while the checkpoint makes
	// resuming it cheap, rather than hours later with the failure buried in the log.
	// The first failure is what gets reported; the others are its consequence.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	var firstFailure error
	var failOnce sync.Once
	fail := func(err error) {
		failOnce.Do(func() { firstFailure = err })
		cancelRun()
	}

	tasks := make(chan manifest.FileMeta)
	// Buffered by one full batch per writer, so a reader that is ahead has somewhere to
	// put its items and the writers always have a batch's worth to choose from.
	items := make(chan item, c.cfg.MaxWorkers*c.cfg.BatchSize)
	var readers, writers sync.WaitGroup

	// The reporter is stopped and waited for below rather than left to notice the run
	// ending, so its last line cannot land in the middle of the final report.
	reportCtx, stopReporting := context.WithCancel(ctx)
	defer stopReporting()
	reporterDone := make(chan struct{})
	go func() {
		defer close(reporterDone)
		c.reportProgress(reportCtx)
	}()

	// The two pools are started together and stopped in order: the readers drain the
	// file list, then the item channel is closed, then the writers drain what is left
	// in it. Closing the item channel any earlier would drop decoded items on the floor.
	for i := 0; i < c.cfg.Readers; i++ {
		readers.Add(1)
		go func(readerID int) {
			defer readers.Done()
			c.initWorker(readerID, true)
			if err := c.readFiles(runCtx, readerID, tasks, items); err != nil {
				fail(fmt.Errorf("reader %d failed: %w", readerID, err))
			}
		}(i)
	}
	for i := 0; i < c.cfg.MaxWorkers; i++ {
		writers.Add(1)
		go func(writerID int) {
			defer writers.Done()
			c.initWorker(writerID, false)
			if err := c.writeItems(runCtx, writerID, items); err != nil {
				fail(fmt.Errorf("writer %d failed: %w", writerID, err))
			}
		}(i)
	}

	readersDone := make(chan struct{})
	go func() {
		readers.Wait()
		close(readersDone)
		close(items)
	}()
	writersDone := make(chan struct{})
	go func() {
		writers.Wait()
		close(writersDone)
	}()

	// Send tasks. Handing one to a pool that has already given up would block forever,
	// so a pool that has exited or been stopped ends dispatch. This is the only place a
	// file a previous run finished is dropped; the workers see only what is left to do.
dispatch:
	for _, file := range summary.DataFiles {
		if _, done := c.progress.resume(file.Key); done {
			continue
		}

		select {
		case tasks <- file:
		case <-readersDone:
			break dispatch
		case <-runCtx.Done():
			break dispatch
		}
	}
	close(tasks)

	<-readersDone
	<-writersDone
	stopReporting()
	<-reporterDone

	// Whatever the pools got through is saved once they have all stopped, however
	// the run is ending. An interrupted run's last batches would otherwise be lost
	// with the interval save they never reached. The caller's context may be the very
	// thing that ended the run, so the save gets a fresh one bounded by the shutdown
	// timeout, which is the budget an operator gave the run to stop cleanly.
	saveCtx, cancelSave := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.ShutdownTimeout)
	defer cancelSave()
	saveErr := c.saveProgress(saveCtx)

	// An interruption or a failure is reported ahead of a failed final save: the save
	// failing is a consequence the operator needs to know about, not the cause.
	if err := ctx.Err(); err != nil {
		// What an operator needs on being interrupted is how far the restore got and
		// that it need not start again, neither of which "context canceled" conveys.
		c.console.line("Interrupted with %d of %d data files finished; run the same command again to carry on",
			c.progress.completedCount(), c.totalFiles)
		return shutdownError(fmt.Errorf("%w (%w)", ErrInterrupted, err), saveErr)
	}
	if firstFailure != nil {
		return shutdownError(firstFailure, saveErr)
	}
	if saveErr != nil {
		return fmt.Errorf("failed to save final checkpoint: %w", saveErr)
	}

	// Generate and print report
	report := c.metrics.GenerateReport(c.progress.skippedCount())
	c.console.line("%s", report)

	// Upload report to S3 if configured
	if c.cfg.ReportS3URI != "" && c.reportUploader != nil {
		if err := c.reportUploader.UploadReport(ctx, c.cfg.ReportS3URI, report); err != nil {
			return fmt.Errorf("failed to upload report: %w", err)
		}
		c.console.line("Report uploaded to %s", c.cfg.ReportS3URI)
	}

	// Judged after the report is out, so the record of what was skipped survives the
	// failure it causes. The count is the restore's, not this run's: a resume does not
	// re-read the files the skipped lines are in, so judging on what this process saw
	// would turn the last run of an export that lost records into a clean one.
	if skipped := c.progress.skippedCount(); skipped > 0 {
		return fmt.Errorf("%w: %d lines could not be decoded; running again will not change that",
			ErrRecordsSkipped, skipped)
	}

	return nil
}

// corruptLinesNamed caps how many skipped lines are named on stderr. The count in the
// report covers the rest; naming every one of a badly damaged file would drown the log.
const corruptLinesNamed = 20

// skipCorrupt records a line that could not be decoded and names the first few, with
// the file and offset an operator needs to find them. The count goes to progress rather
// than to the metrics, because it has to outlive the process: only the lines this run
// read past are named here, while the count covers every run of the restore.
func (c *Coordinator) skipCorrupt(key string, offset int64, err error) {
	c.progress.skip()
	if c.corruptNamed.Add(1) <= corruptLinesNamed {
		c.console.notice("skipping %s at offset %d: %v", key, offset, err)
	}
}

// shutdownError reports why the run stopped, noting a final save that failed with it
// so the operator knows the checkpoint is behind what was written.
func shutdownError(cause, saveErr error) error {
	if saveErr != nil {
		return fmt.Errorf("%w (and the final checkpoint could not be saved: %w)", cause, saveErr)
	}
	return cause
}

// initWorker initializes a pool member's status tracking. Readers and writers are
// numbered separately, so a status is identified by its role as well as its id.
func (c *Coordinator) initWorker(id int, isReader bool) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	now := time.Now()
	c.workerStatus[statusKey{id: id, reader: isReader}] = &WorkerStatus{
		ID:         id,
		IsReader:   isReader,
		StartTime:  now,
		LastActive: now,
	}
}

// updateWorkerStatus applies fn to a pool member's status and stamps it as active.
// An id with no status is a wiring mistake: every pool member registers before it runs,
// and quietly dropping the update would lose an error report with it.
func (c *Coordinator) updateWorkerStatus(key statusKey, fn func(*WorkerStatus)) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	status, ok := c.workerStatus[key]
	if !ok {
		panic(fmt.Sprintf("coordinator: no status registered for %s %d", key.role(), key.id))
	}
	fn(status)
	status.LastActive = time.Now()
}

// workerIdleTimeout is how long a pool member may go without activity before the
// progress line stops counting it among the active ones.
const workerIdleTimeout = 10 * time.Second

// linesPerStampCheck is how many lines a reader gets through between consulting the
// clock about whether it is due to report that it is still working. Reading the clock
// per line is a cost the hot path does not need, and reading it per sixty-odd is free.
//
// A file holding fewer lines than this reports only when it is picked up, which covers
// it for workerIdleTimeout. That leaves a file of very large items, few enough in number
// and slow enough to read, showing its reader as stalled towards the end; the count is
// wrong there and nothing else is.
const linesPerStampCheck = 64

// defaultStampInterval is how often a reader that is making progress says so. It is
// well inside workerIdleTimeout, so a reader is never counted idle while it is working.
const defaultStampInterval = workerIdleTimeout / 4

// markActive stamps a pool member as still working, which is all the progress line
// needs from a member that has nothing else to report.
func (c *Coordinator) markActive(key statusKey) {
	c.updateWorkerStatus(key, func(*WorkerStatus) {})
}

// markBusy records whether a pool member has work in hand.
func (c *Coordinator) markBusy(key statusKey, busy bool) {
	c.updateWorkerStatus(key, func(s *WorkerStatus) { s.Busy = busy })
}

// bytesPerMB converts the byte counters into the megabytes the progress line reports.
const bytesPerMB = 1024 * 1024

// progressSnapshot is the point-in-time view of the restore that the progress line renders.
// Fields are ordered largest-to-smallest for memory alignment.
type progressSnapshot struct {
	ItemsPerSec  float64 // Items written per second since the previous snapshot
	MBPerSec     float64 // Megabytes of export read per second since the previous snapshot
	Percent      float64 // Share of the manifest's item count written so far, capped at 100
	Pace         float64 // Write capacity units per second the table is currently allowing
	ItemsWritten int64   // Items this run has written to the table
	TotalBatches int64   // Batches written since the restore started
	Throttles    int64
	Retries      int64
	LostItems    int64
	Errors       int64
	// FilesDone counts every data file the restore has finished, including ones an
	// earlier run finished, because that is what is left to do rather than what this
	// process has got through.
	FilesDone    int
	FilesTotal   int  // Data files the export contains
	ActiveReader int  // Readers that reported activity within workerIdleTimeout
	ActiveWriter int  // Writers that reported activity within workerIdleTimeout
	Paced        bool // Whether anything is limiting the write rate yet
}

// String renders the line an operator watches while a restore runs. Every number is
// labelled where it appears, because the throttle, retry, lost and error counts drive
// different responses and reading one as another sends the operator the wrong way.
//
// What has gone right leads and what has gone wrong follows, because a restore is
// mostly the former: the items written and the files finished are what say the restore
// is working, while a percentage alone says nothing an operator can act on and the
// failure counts on their own read as a restore in trouble.
//
// The two pool counts are shown apart because they say which end is the bottleneck:
// idle writers mean the export is not being read fast enough, idle readers mean the
// table is not accepting writes fast enough. The pace is what the table is currently
// allowing, and reads as "-" until something has limited the restore at all, since a
// restore nothing has throttled has no rate to report.
func (s progressSnapshot) String() string {
	pace := "-"
	if s.Paced {
		pace = fmt.Sprintf("%.0f WCU/s", s.Pace)
	}
	return fmt.Sprintf(
		"Progress: %.1f%% | %d items in %d batches | %d/%d files | %.0f/s, %.1f MB/s | "+
			"%d readers | %d writers | pace %s | %d throttles | %d retries | %d lost | %d errors",
		s.Percent, s.ItemsWritten, s.TotalBatches, s.FilesDone, s.FilesTotal,
		s.ItemsPerSec, s.MBPerSec, s.ActiveReader, s.ActiveWriter,
		pace, s.Throttles, s.Retries, s.LostItems, s.Errors)
}

// snapshot folds worker status and metrics into the numbers the progress line shows,
// then advances the rolling window to now.
//
// Rates are measured over the interval since the previous snapshot rather than since
// the start, so a restore that slows down says so immediately instead of being hidden
// behind a long average. Taking now as an argument keeps the rate over a known interval.
//
// Advancing the window is what makes this a single-caller method: it belongs to
// reportProgress. Worker status and the metrics counters are read under their own
// synchronisation, so workers may keep running throughout.
func (c *Coordinator) snapshot(now time.Time) progressSnapshot {
	c.statusMu.RLock()
	var totalItems, totalBatches int64
	activeReaders, activeWriters := 0, 0
	for _, status := range c.workerStatus {
		// Work in hand is what makes a member active, and a stale stamp is what tells
		// a member that is stuck from one that is getting on with it.
		if status.Busy && now.Sub(status.LastActive) < workerIdleTimeout {
			if status.IsReader {
				activeReaders++
			} else {
				activeWriters++
			}
		}
		totalItems += status.ItemsWritten
		totalBatches += status.BatchesCount
	}
	c.statusMu.RUnlock()

	bytesRead := c.metrics.BytesRead()
	pace, paced := c.metrics.Pace()

	snap := progressSnapshot{
		ItemsWritten: totalItems,
		TotalBatches: totalBatches,
		FilesDone:    c.progress.completedCount(),
		FilesTotal:   c.totalFiles,
		Throttles:    c.metrics.Throttles(),
		Retries:      c.metrics.Retries(),
		LostItems:    c.metrics.LostItems(),
		Errors:       c.metrics.Errors(),
		ActiveReader: activeReaders,
		ActiveWriter: activeWriters,
		Pace:         pace,
		Paced:        paced,
	}

	if elapsed := now.Sub(c.lastReportTime).Seconds(); elapsed > 0 {
		itemsDelta := totalItems - c.lastReportItems
		bytesDelta := bytesRead - c.lastReportBytes
		snap.ItemsPerSec = float64(itemsDelta) / elapsed
		snap.MBPerSec = float64(bytesDelta) / bytesPerMB / elapsed
	}

	if c.totalExpectedItems > 0 {
		snap.Percent = float64(totalItems) / float64(c.totalExpectedItems) * 100
		// The manifest's item count is an estimate for incremental exports, so a
		// restore can legitimately write more items than it predicted.
		if snap.Percent > 100 {
			snap.Percent = 100
		}
	}

	c.lastReportTime = now
	c.lastReportItems = totalItems
	c.lastReportBytes = bytesRead

	return snap
}

// reportProgress prints the progress line once a second, overwriting the previous one.
func (c *Coordinator) reportProgress(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.console.update(c.snapshot(time.Now()).String())

		case <-ctx.Done():
			// Finish the line so the report that follows starts on its own.
			c.console.done()
			return
		}
	}
}

// checkpointInterval controls how often checkpoints are saved (every N batches).
// This balances durability (frequent saves) with performance (fewer S3 API calls).
const checkpointInterval = 100

// saveProgress persists a snapshot of what the restore has finished. Snapshotting and
// writing under one lock keeps the stored state monotonic. Nothing is skipped without
// it, since every snapshot is a subset of every later one and files are only ever added;
// an out-of-order write would cost a resume the work between the two snapshots, which
// the lock is cheap enough to avoid.
func (c *Coordinator) saveProgress(ctx context.Context) error {
	c.saveMu.Lock()
	defer c.saveMu.Unlock()
	return c.store.Save(ctx, c.progress.snapshot())
}

// dispatched is one line handed to a writer and not yet acknowledged. prev is the
// furthest line the reader had examined before this one, which is what the file's
// watermark falls back to while this line is still in flight.
type dispatched struct {
	offset int64
	prev   int64
	acked  bool
}

// fileLedger tracks, for one data file, which lines are in flight and how far the file
// can be committed. Writers take items from many files at once and finish them out of
// order, so the offset a resume restarts past cannot be the last line written: it has
// to be the low watermark, the highest offset with nothing unfinished below it.
//
// One reader owns the dispatching end of a ledger; any writer may acknowledge into it.
// Fields are ordered largest-to-smallest for memory alignment.
type fileLedger struct {
	pending  []dispatched  // Lines handed to writers, ascending by offset
	key      string        // The data file this ledger belongs to
	drained  chan struct{} // Closed once the file is read to the end and nothing is in flight
	examined int64         // Furthest line dispatched or skipped; where a retry resumes from
	mu       sync.Mutex
	eof      bool // The reader has read the file to the end
	closed   bool // drained has been closed
}

// newFileLedger opens a ledger for a file, starting from the offset a previous run
// recorded, or noOffset when none did.
func newFileLedger(key string, resumed int64) *fileLedger {
	return &fileLedger{
		key:      key,
		drained:  make(chan struct{}),
		examined: resumed,
	}
}

// reached reports the furthest line the reader has dispatched or skipped. A stream
// retry starts the file again from the beginning and passes over everything up to
// this, so no line is handed to a writer twice within a run.
func (l *fileLedger) reached() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.examined
}

// dispatch records a line about to be handed to a writer. It is called before the item
// is sent, never after: an acknowledgement arriving for a line the ledger has not
// dispatched would leave the watermark past a line still in flight.
//
// Offsets only ever increase, since a retry passes over everything already reached.
// A lower one would put pending out of order and silently corrupt the watermark, which
// costs a resume the records between the two, so it fails here instead.
func (l *fileLedger) dispatch(offset int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset <= l.examined {
		panic(fmt.Sprintf("coordinator: file %s dispatched offset %d after reaching %d",
			l.key, offset, l.examined))
	}
	l.pending = append(l.pending, dispatched{offset: offset, prev: l.examined})
	l.examined = offset
}

// skip notes a line that will never be written, which is one that could not be decoded.
// It returns the file's watermark, which steps over the line once everything below it
// has been acknowledged.
func (l *fileLedger) skip(offset int64) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset > l.examined {
		l.examined = offset
	}
	return l.watermarkLocked()
}

// ack marks a dispatched line written and returns the file's watermark: the highest
// offset with every line at or below it written or skipped. That is what a resume
// restarts past, so it never passes a line another writer still holds.
func (l *fileLedger) ack(offset int64) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	// pending is ascending, appended by the single reader that owns the file, so the
	// line is found by bisection rather than by scanning what is in flight.
	i := sort.Search(len(l.pending), func(i int) bool { return l.pending[i].offset >= offset })
	if i < len(l.pending) && l.pending[i].offset == offset {
		l.pending[i].acked = true
	}

	// The acknowledged head is what the watermark may now pass. Reslicing rather than
	// copying keeps this amortised: the abandoned prefix holds no pointers and goes
	// when the slice next grows.
	popped := 0
	for popped < len(l.pending) && l.pending[popped].acked {
		popped++
	}
	l.pending = l.pending[popped:]

	l.closeIfDrainedLocked()
	return l.watermarkLocked()
}

// finish records that the reader has read the file to the end. The file is done once
// the writers holding its last lines have acknowledged them.
func (l *fileLedger) finish() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.eof = true
	l.closeIfDrainedLocked()
}

// closeIfDrainedLocked releases anything waiting on the file once it has been read to
// the end and nothing is in flight.
func (l *fileLedger) closeIfDrainedLocked() {
	if l.eof && len(l.pending) == 0 && !l.closed {
		l.closed = true
		close(l.drained)
	}
}

// watermarkLocked is the offset the file can be committed to. Nothing in flight means
// everything examined is done; otherwise it stops short of the oldest line still out.
func (l *fileLedger) watermarkLocked() int64 {
	if len(l.pending) == 0 {
		return l.examined
	}
	return l.pending[0].prev
}

// item is one decoded operation on its way from the reader that decoded it to whichever
// writer picks it up, carrying what that writer needs to acknowledge it.
// Fields are ordered largest-to-smallest for memory alignment.
type item struct {
	op     itemimage.Operation
	ledger *fileLedger
	offset int64
}

// readFiles takes data files from the task channel and turns each into a stream of
// items for the writer pool, with the stream retried on failure.
//
// HOT PATH: Stream S3 -> Decode JSON -> hand to a writer.
// The dominant cost is JSON decoding in parser.Decode (~27% CPU, ~99% memory).
//
// How many of these run at once is what decides how widely the restore's writes are
// spread over the target table: each file holds one contiguous slice of the exported
// key space, so items from one file land on one or a very few target partitions.
// Concurrency is c.cfg.Readers.
func (c *Coordinator) readFiles(ctx context.Context, id int, tasks <-chan manifest.FileMeta, items chan<- item) error {
	const maxRetries = 3
	self := statusKey{id: id, reader: true}
	// However this reader leaves, it is no longer holding a file.
	defer c.markBusy(self, false)

	// Use the bucket from the config
	bucket := c.cfg.GetExportBucketName()

	for file := range tasks {
		// A file handed over as the run stops is surrendered rather than started, so
		// stopping never means finishing whatever was in flight.
		if err := ctx.Err(); err != nil {
			return err
		}
		// Files a previous run finished never reach here: dispatch drops them, and each
		// file goes to one reader, so nothing can complete a file between the two.
		resumed, _ := c.progress.resume(file.Key)
		ledger := newFileLedger(file.Key, resumed)
		c.markBusy(self, true)

		// Stream and process the file with retries
		var streamErr error
		linesSinceCheck := 0
		lastStamp := time.Now()
		for retry := 0; retry < maxRetries; retry++ {
			// The first attempt is not paced; every one after it waits longer than the
			// last, so a file failing against a struggling S3 does not hammer it. A wait
			// cut short means the restore is stopping, and the file is unfinished, so it
			// is surrendered rather than left to be recorded as complete below.
			if retry > 0 && !c.backoff.Wait(ctx, retry) {
				return fmt.Errorf("stopped retrying file %s: %w", file.Key, stopRetrying(ctx))
			}

			// Every attempt reads the file from its start and passes over everything the
			// ledger has already reached, so a retry continues from the furthest point
			// the failed attempt got to. Nothing is buffered in the reader, so a failed
			// attempt leaves nothing behind to be written twice.
			//
			// The file is read from the start because the streamer's offset is a
			// position in the stored object, while the offsets the callback reports and
			// the ledger records are positions in the decompressed stream. The two only
			// agree for an uncompressed file, and exports are gzipped.
			streamErr = c.streamer.Stream(ctx, bucket, file.Key, streamFromStart, func(line []byte, byteOffset int64) error {
				linesSinceCheck++
				if linesSinceCheck >= linesPerStampCheck {
					linesSinceCheck = 0
					if now := time.Now(); now.Sub(lastStamp) >= c.stamp {
						lastStamp = now
						c.markActive(self)
					}
				}

				// Lines already dispatched are in flight or written, and lines already
				// skipped have been counted. The check precedes decoding, which is where
				// the CPU and memory go.
				if byteOffset <= ledger.reached() {
					return nil
				}

				// Decode is the main CPU/memory bottleneck (~27% CPU, ~99% memory)
				op, err := c.parser.Decode(line)
				if errors.Is(err, itemimage.ErrCorrupt) {
					c.skipCorrupt(file.Key, byteOffset, err)
					if mark := ledger.skip(byteOffset); mark != noOffset {
						c.progress.record(file.Key, mark)
					}
					return nil
				}
				if err != nil {
					c.metrics.RecordError()
					return err
				}

				ledger.dispatch(byteOffset)
				select {
				case items <- item{op: op, ledger: ledger, offset: byteOffset}:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})

			if streamErr == nil {
				break
			}

			c.recordError(self, streamErr)
		}

		if streamErr != nil {
			return fmt.Errorf("failed to process file %s after %d retries: %w",
				file.Key, maxRetries, streamErr)
		}

		// The file is read, but its last lines are still spread across the writers. It
		// is only complete once they have all been written; recording it before that
		// would let a resume skip a file with records still unwritten.
		ledger.finish()
		select {
		case <-ledger.drained:
		case <-ctx.Done():
			return ctx.Err()
		}

		// Record the file as done. This is what a resume reads to skip it, so it is
		// saved unconditionally rather than at the batch interval.
		c.progress.complete(file.Key)
		if err := c.saveProgress(ctx); err != nil {
			c.recordError(self, err)
			return fmt.Errorf("failed to save completion checkpoint for file %s: %w", file.Key, err)
		}
		c.markBusy(self, false)
	}

	return nil
}

// defaultBatchLinger is how long a partly filled batch waits for more items before it
// is written anyway.
const defaultBatchLinger = 50 * time.Millisecond

// writeItems fills batches from whatever the readers produce and writes them.
//
// A batch is deliberately drawn from every file being read at once, which is the whole
// point of the split: consecutive lines of one export file share a partition, so a
// batch built from one file is a batch aimed at one partition. Ordering within a file
// is given up in exchange, which costs nothing here because an export holds one record
// per key: a full export by construction, an incremental one because it records each
// item's latest state once.
//
// HOT PATH: every item the restore writes passes through here. Concurrency is
// c.cfg.MaxWorkers, and the cost is dominated by the BatchWriteItem call itself.
func (c *Coordinator) writeItems(ctx context.Context, id int, items <-chan item) error {
	self := statusKey{id: id}
	// However this writer leaves, it is no longer holding a batch.
	defer c.markBusy(self, false)

	batch := make([]item, 0, c.cfg.BatchSize)
	ops := make([]itemimage.Operation, 0, c.cfg.BatchSize)
	batchesSinceCheckpoint := 0

	// A partly filled batch has to go out on its own eventually. Readers waiting for
	// their last lines to be acknowledged produce nothing more to top it up, so without
	// this the tail of a restore would wait on items that are never coming.
	linger := time.NewTimer(c.linger)
	stopTimer(linger)
	defer linger.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		ops = ops[:0]
		for i := range batch {
			ops = append(ops, batch[i].op)
		}

		start := time.Now()
		if err := c.writer.WriteBatch(ctx, ops); err != nil {
			c.recordError(self, err)
			return err
		}
		c.metrics.RecordProcessingTime(time.Since(start))
		c.metrics.RecordBatchWritten()
		c.metrics.RecordProcessed(int64(len(ops)))

		c.updateWorkerStatus(self, func(s *WorkerStatus) {
			s.ItemsWritten += int64(len(ops))
			s.BatchesCount++
			s.Busy = false
		})

		// Acknowledged only once the write has returned, so a checkpoint taken now
		// cannot describe a line as done that DynamoDB has not accepted. Each file's
		// watermark is recorded on every batch, so any checkpoint, whichever writer
		// takes it, carries the furthest safe point of every file in flight. Writing it
		// out is what costs an S3 call, so that still happens only at intervals.
		for i := range batch {
			if mark := batch[i].ledger.ack(batch[i].offset); mark != noOffset {
				c.progress.record(batch[i].ledger.key, mark)
			}
		}
		batch = batch[:0]

		batchesSinceCheckpoint++
		if batchesSinceCheckpoint >= checkpointInterval {
			batchesSinceCheckpoint = 0
			if err := c.saveProgress(ctx); err != nil {
				c.recordError(self, err)
				return err
			}
		}
		return nil
	}

	for {
		select {
		case next, ok := <-items:
			if !ok {
				// The readers are done, so nothing will top this batch up.
				return flush()
			}
			batch = append(batch, next)
			if len(batch) == 1 {
				c.markBusy(self, true)
				resetTimer(linger, c.linger)
			}
			if len(batch) >= c.cfg.BatchSize {
				stopTimer(linger)
				if err := flush(); err != nil {
					return err
				}
			}

		case <-linger.C:
			if err := flush(); err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// stopTimer stops a timer and clears any tick it had already delivered, so the next
// wait on it cannot see a stale one. Only the goroutine that owns the timer calls this.
func stopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

// resetTimer restarts a timer from now, whatever state it was in.
func resetTimer(t *time.Timer, d time.Duration) {
	stopTimer(t)
	t.Reset(d)
}

// recordError counts a failure, records it against the pool member that hit it, and
// prints it. A counter on the progress line says a restore is in trouble but never what
// the trouble is, and a restore that survives its failures would otherwise finish
// without ever having said what it survived.
//
// A context ending is left unprinted. That is the restore being stopped, which the
// caller asked for and every member of both pools reports at once, so printing it would
// bury whatever actually caused the stop under a line from each of them.
func (c *Coordinator) recordError(key statusKey, err error) {
	c.metrics.RecordError()
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		c.console.notice("%s %d: %v", key.role(), key.id, err)
	}
	c.updateWorkerStatus(key, func(s *WorkerStatus) {
		s.LastError = err
		s.LastErrorTime = time.Now()
	})
}

// statusKey identifies one member of one pool. Readers and writers number themselves
// from zero independently, so the id alone does not name a goroutine.
type statusKey struct {
	id     int
	reader bool
}

// role names the pool a key belongs to, for a message an operator reads.
func (k statusKey) role() string {
	if k.reader {
		return "reader"
	}
	return "writer"
}
