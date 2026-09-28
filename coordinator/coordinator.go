// Package coordinator drives a restore: it loads and verifies the export, reads its
// data files into batches for the writer, checkpoints their progress and reports the
// outcome.
//
// A pool of readers and one batcher are joined by a channel of decoded items. A data
// file holds one contiguous slice of the exported table's key space, so a batch built
// from one file is a batch aimed at one target partition; drawing every batch from all
// the files open at once is what lets a restore use the whole table rather than a few
// partitions of it. For the same reason a file stands in for a target partition when the
// table pushes back: items it hands back are sent again among other files' items, and
// the file they came from is slowed while the rest of the table is not.
package coordinator

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
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
// meaning there. See readFiles for why.
type Streamer interface {
	Stream(ctx context.Context, bucket, key string, offset int64, fn func(line []byte, offset int64) error) error
}

// streamFromStart is the only stored-object offset a reader asks for. Checkpoint
// offsets are decompressed positions; handing one to the streamer would range into the
// middle of a gzip member.
const streamFromStart int64 = 0

// noOffset is what progress reports for a file nothing has been written from yet.
// Zero cannot mean that, since the first line of every file sits at offset zero.
const noOffset int64 = -1

// readerStatus is what one reader has been doing, for the progress line.
// Fields are ordered largest-to-smallest for memory alignment.
type readerStatus struct {
	lastErrorTime time.Time // When the last error occurred
	lastActive    time.Time // When the reader last said it was working
	lastError     error     // Last error encountered
	// busy is whether the reader holds a file. A reader with nothing to do is not
	// counted among the active ones, because having nothing to do is the very thing the
	// count is there to show.
	busy bool
}

// Submitter writes batches of at most writer.MaxBatch operations in the background,
// calling done once per batch with what the table did not take, or with the error that
// made the batch fail. Submit blocks while as many batches are in flight as the table
// is rewarding, and returns the context's error, without calling done, if that ends
// first. done must not block on whoever called Submit.
type Submitter interface {
	Submit(ctx context.Context, ops []itemimage.Operation, done func(writer.Rejection, error)) error
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
// error. Returning the context's nil error there would let the reader fall through and
// record an unfinished file as complete, which a resume would then skip.
var errBackoffStopped = errors.New("coordinator: backoff stopped before the file was finished")

// ErrInterrupted is returned when the run stopped because the caller's context ended
// rather than because the export was finished. It wraps the reason the context was
// given, so a caller can tell a signal from a deadline, or from a reason of its own, and
// say what to do next.
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

// WithBatchLinger replaces how long a partly filled batch waits for more items before
// it is written anyway. The default is 50ms. Shorter trades larger requests for lower
// latency at the tail of a restore; it may not be zero, which would send every item as
// its own request.
// Example:
//
//	coord := coordinator.NewCoordinator(cfg, loader, streamer, parser, w, store, uploader, m,
//	    coordinator.WithBatchLinger(10*time.Millisecond))
func WithBatchLinger(d time.Duration) Option {
	return func(c *Coordinator) {
		if d <= 0 {
			panic(fmt.Sprintf("coordinator: batch linger %s is not positive", d))
		}
		c.linger = d
	}
}

// withBatchSize replaces how many items go in one batch, so a test can make batch
// boundaries fall where it needs them without writing twenty-five items to get one.
func withBatchSize(n int) Option {
	return func(c *Coordinator) {
		if n < 1 || n > writer.MaxBatch {
			panic(fmt.Sprintf("coordinator: batch size %d is outside 1..%d", n, writer.MaxBatch))
		}
		c.batchSize = n
	}
}

// withCheckpointEvery replaces how often progress is saved while the restore runs, so a
// test can see an interval save without waiting for one.
func withCheckpointEvery(d time.Duration) Option {
	return func(c *Coordinator) {
		c.checkpointEvery = d
	}
}

// Coordinator runs the restore: reading, batching, checkpointing and progress
// reporting.
type Coordinator struct {
	cfg             *config.Config
	manifest        manifest.Loader
	streamer        Streamer
	parser          itemimage.Decoder
	writer          Submitter
	store           checkpoint.Store
	metrics         *metrics.Metrics
	reportUploader  ReportUploader
	backoff         Backoffer
	console         *console      // Owns the progress line and anything printed beside it
	lane            *retryLane    // Items the table did not take, waiting to be sent again
	linger          time.Duration // How long a partly filled batch waits for more items
	stamp           time.Duration // How often a reader reports that it is still working
	checkpointEvery time.Duration // How often progress is saved while the restore runs
	batchSize       int           // Items in one batch
	inFlight        atomic.Int64  // Batches submitted and not yet answered

	// progress owns what the restore has finished; the store is only ever written
	// from a snapshot of it, taken under saveMu so the object in S3 advances in the
	// same order the snapshots were taken.
	progress *progress
	saveMu   sync.Mutex

	// What each reader is doing, for the progress line
	readerStatus map[int]*readerStatus
	statusMu     sync.RWMutex

	// Progress tracking for percentage and throughput calculation
	corruptNamed       atomic.Int64 // Skipped lines named on stderr so far
	totalExpectedItems int64        // Total items expected from manifest
	totalFiles         int          // Data files the export contains
	lastReportTime     time.Time    // Last progress report timestamp
	lastReportItems    int64        // Items count at last report
	lastReportBytes    int64        // Bytes count at last report
}

// progress is the single owner of how far the restore has got. Readers and batches report
// into it and it is the only source of what gets checkpointed, so two files finishing
// different files cannot overwrite each other's progress the way independent writes
// to a shared store would.
// Fields are ordered largest-to-smallest for memory alignment.
type progress struct {
	exportID  string              // Export this progress belongs to
	completed map[string]struct{} // Files processed to the end
	offsets   map[string]int64    // Offset of the last line written, per file still in progress
	skipped   int64               // Lines that could not be decoded, over every run so far
	changes   uint64              // Bumped on every change, so a save can tell whether there is anything new
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
// A file already complete takes no offset: the batch that wrote its last lines may
// record them after the reader has seen the file drain and marked it done, and an offset
// put back then would be carried by every later checkpoint for nothing.
func (p *progress) record(key string, offset int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, done := p.completed[key]; done {
		return
	}
	if recorded, ok := p.offsets[key]; ok && recorded >= offset {
		return
	}
	p.offsets[key] = offset
	p.changes++
}

// complete marks a file finished. Its offset is dropped, since a completed file is
// never resumed and keeping it would grow the checkpoint for the whole export.
func (p *progress) complete(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.offsets, key)
	p.completed[key] = struct{}{}
	p.changes++
}

// skip counts a line that could not be decoded. The count spans runs, because the line
// is in a file a resume will not read again: once the file is finished, this is the only
// record left that the line was ever there.
func (p *progress) skip() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.skipped++
	p.changes++
}

// version reports how many changes progress has taken, so a save that finds it where
// the last save left it can skip writing the same state again.
func (p *progress) version() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.changes
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
	w Submitter,
	store checkpoint.Store,
	reportUploader ReportUploader,
	m *metrics.Metrics,
	opts ...Option,
) *Coordinator {
	c := &Coordinator{
		cfg:             cfg,
		manifest:        manifest,
		streamer:        streamer,
		parser:          parser,
		writer:          w,
		store:           store,
		metrics:         m,
		reportUploader:  reportUploader,
		backoff:         writer.NewExponentialBackoff(time.Second, 30*time.Second),
		console:         newConsole(os.Stdout, os.Stderr),
		lane:            newRetryLane(),
		linger:          defaultBatchLinger,
		stamp:           defaultStampInterval,
		checkpointEvery: defaultCheckpointEvery,
		batchSize:       writer.MaxBatch,
		progress:        newProgress("", checkpoint.State{}),
		readerStatus:    make(map[int]*readerStatus),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Run restores the export: it loads the manifest and the checkpoint, verifies the data
// files still to do, and drives the readers and the batcher over them. The caller's context is
// what stops a run early; the caller owns any signal handling. However the run ends,
// the checkpoint is saved once the readers and the batcher have stopped, within the configured
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
	// checked is the one the readers read from, not the one the manifest names; they
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

	// The run has its own cancellation so one reader's or batch's failure stops the rest.
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
	// Buffered by a few batches, so a reader that is ahead has somewhere to put its items
	// and the batcher always has a batch's worth to choose from. What is held beyond
	// this is bounded by each file's window, not by the channel.
	items := make(chan item, itemBuffer)
	var readers, batcher, saver sync.WaitGroup

	// The reporter is stopped and waited for below rather than left to notice the run
	// ending, so its last line cannot land in the middle of the final report.
	reportCtx, stopReporting := context.WithCancel(ctx)
	defer stopReporting()
	reporterDone := make(chan struct{})
	go func() {
		defer close(reporterDone)
		c.reportProgress(reportCtx)
	}()

	// Readers and the batcher are started together and stopped in order: the readers
	// drain the file list, then the item channel is closed, then the batcher drains what
	// is left in it and waits for the batches it has in flight. Closing the item channel
	// any earlier would drop decoded items on the floor. A reader finishes a file only
	// once every item of it has been written, so when the channel closes, nothing is
	// waiting to be sent again either.
	// What progress the save before the pools recorded, read before anything can change
	// it: a baseline taken once the saver got round to it could already include a line
	// written since, which would then wait for the next change to be saved.
	savedVersion := c.progress.version()
	for i := 0; i < c.cfg.Readers; i++ {
		readers.Add(1)
		go func(readerID int) {
			defer readers.Done()
			c.initReader(readerID)
			if err := c.readFiles(runCtx, readerID, tasks, items); err != nil {
				fail(fmt.Errorf("reader %d failed: %w", readerID, err))
			}
		}(i)
	}
	batcher.Add(1)
	go func() {
		defer batcher.Done()
		c.writeItems(runCtx, items, fail)
	}()
	// Progress is saved on a timer rather than by whoever wrote the batch that crossed
	// an interval, so no write waits on S3. The saver stops with the readers and the
	// batcher; the save after them is the one that records where they got to.
	saverCtx, stopSaving := context.WithCancel(runCtx)
	defer stopSaving()
	saver.Add(1)
	go func() {
		defer saver.Done()
		if err := c.saveEvery(saverCtx, savedVersion); err != nil {
			fail(fmt.Errorf("failed to save progress: %w", err))
		}
	}()

	readersDone := make(chan struct{})
	go func() {
		readers.Wait()
		close(readersDone)
		close(items)
	}()

	// Send tasks. Handing one to readers that have already given up would block forever,
	// so readers that have exited or been stopped end dispatch. This is the only place a
	// file a previous run finished is dropped; the readers see only what is left to do.
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
	batcher.Wait()
	stopSaving()
	saver.Wait()
	stopReporting()
	<-reporterDone

	// Whatever the readers and batches got through is saved once they have all stopped, however
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
		// What an operator needs on being stopped is how far the restore got, which
		// "context canceled" does not convey. What to do next depends on why it was
		// stopped, which the caller knows and says; the reason is kept in the error.
		c.console.line("Stopped with %d of %d data files finished",
			c.progress.completedCount(), c.totalFiles)
		return shutdownError(fmt.Errorf("%w (%w)", ErrInterrupted, context.Cause(ctx)), saveErr)
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

// initReader registers a reader for the progress line.
func (c *Coordinator) initReader(id int) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.readerStatus[id] = &readerStatus{lastActive: time.Now()}
}

// updateReader applies fn to a reader's status and stamps it as active. An id with no
// status is a wiring mistake: every reader registers before it runs, and quietly
// dropping the update would lose an error report with it.
func (c *Coordinator) updateReader(id int, fn func(*readerStatus)) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	status, ok := c.readerStatus[id]
	if !ok {
		panic(fmt.Sprintf("coordinator: no status registered for reader %d", id))
	}
	fn(status)
	status.lastActive = time.Now()
}

// readerIdleTimeout is how long a reader may go without activity before the progress
// line stops counting it among the active ones.
const readerIdleTimeout = 10 * time.Second

// linesPerStampCheck is how many lines a reader gets through between consulting the
// clock about whether it is due to report that it is still working. Reading the clock
// per line is a cost the hot path does not need, and reading it per sixty-odd is free.
//
// A file holding fewer lines than this reports only when it is picked up, which covers
// it for readerIdleTimeout. That leaves a file of very large items, few enough in number
// and slow enough to read, showing its reader as stalled towards the end; the count is
// wrong there and nothing else is.
const linesPerStampCheck = 64

// defaultStampInterval is how often a reader that is making progress says so. It is
// well inside readerIdleTimeout, so a reader is never counted idle while it is working.
const defaultStampInterval = readerIdleTimeout / 4

// markActive stamps a reader as still working, which is all the progress line needs
// from a reader that has nothing else to report.
func (c *Coordinator) markActive(id int) {
	c.updateReader(id, func(*readerStatus) {})
}

// markBusy records whether a reader holds a file.
func (c *Coordinator) markBusy(id int, busy bool) {
	c.updateReader(id, func(s *readerStatus) { s.busy = busy })
}

// bytesPerMB converts the byte counters into the megabytes the progress line reports.
const bytesPerMB = 1024 * 1024

// progressSnapshot is the point-in-time view of the restore that the progress line renders.
// Fields are ordered largest-to-smallest for memory alignment.
type progressSnapshot struct {
	Operations   string  // Items applied per kind, as "put 12 delete 1"
	ItemsPerSec  float64 // Items written per second since the previous snapshot
	MBPerSec     float64 // Megabytes of export read per second since the previous snapshot
	Percent      float64 // Share of the manifest's item count written so far, capped at 100
	Pace         float64 // Write capacity units per second the table is currently allowing
	ItemsWritten int64   // Items this run has written to the table
	TotalBatches int64   // Batches written since the restore started
	InFlight     int64   // Batches submitted and not yet answered
	HeldBack     int64   // Items the table did not take, waiting to be sent again
	Throttles    int64
	Retries      int64
	LostItems    int64
	Errors       int64
	// FilesDone counts every data file the restore has finished, including ones an
	// earlier run finished, because that is what is left to do rather than what this
	// process has got through.
	FilesDone    int
	FilesTotal   int  // Data files the export contains
	ActiveReader int  // Readers holding a file that reported activity within readerIdleTimeout
	Concurrency  int  // Batches the writer lets be in flight at once
	Paced        bool // Whether anything is limiting the write rate yet
}

// String renders the line an operator watches while a restore runs. Every number is
// labelled where it appears, because the throttle, retry, lost and error counts drive
// different responses and reading one as another sends the operator the wrong way.
//
// What has gone right leads and what has gone wrong follows, because a restore is
// mostly the former: the items written, by kind, and the files finished are what say
// the restore is working, while a percentage alone says nothing an operator can act on
// and the failure counts on their own read as a restore in trouble.
//
// The readers and the writes in flight are shown apart because they say which end is
// the bottleneck: writes in flight well below what the writer allows mean the export is
// not being read fast enough, readers idle while files remain mean the table is not
// accepting writes fast enough. Items held back are what the table refused or handed
// back and is yet to take. The pace is what the table is currently allowing, and reads
// as "-" until something has limited the restore at all, since a restore nothing has
// throttled has no rate to report.
func (s progressSnapshot) String() string {
	pace := "-"
	if s.Paced {
		pace = fmt.Sprintf("%.0f WCU/s", s.Pace)
	}
	return fmt.Sprintf(
		"Progress: %.1f%% | %d items (%s) in %d batches | %d/%d files | %.0f/s, %.1f MB/s | "+
			"%d readers | %d/%d writes in flight | %d held back | pace %s | "+
			"%d throttles | %d retries | %d lost | %d errors",
		s.Percent, s.ItemsWritten, s.Operations, s.TotalBatches, s.FilesDone, s.FilesTotal,
		s.ItemsPerSec, s.MBPerSec, s.ActiveReader, s.InFlight, s.Concurrency, s.HeldBack,
		pace, s.Throttles, s.Retries, s.LostItems, s.Errors)
}

// snapshot folds reader status and metrics into the numbers the progress line shows,
// then advances the rolling window to now.
//
// Rates are measured over the interval since the previous snapshot rather than since
// the start, so a restore that slows down says so immediately instead of being hidden
// behind a long average. Taking now as an argument keeps the rate over a known interval.
//
// Advancing the window is what makes this a single-caller method: it belongs to
// reportProgress. Reader status and the metrics counters are read under their own
// synchronisation, so the restore may keep running throughout.
func (c *Coordinator) snapshot(now time.Time) progressSnapshot {
	c.statusMu.RLock()
	activeReaders := 0
	for _, status := range c.readerStatus {
		// A file in hand is what makes a reader active, and a stale stamp is what tells
		// a reader that is stuck from one that is getting on with it.
		if status.busy && now.Sub(status.lastActive) < readerIdleTimeout {
			activeReaders++
		}
	}
	c.statusMu.RUnlock()

	totalItems := c.metrics.Processed()
	bytesRead := c.metrics.BytesRead()
	pace, paced := c.metrics.Pace()

	snap := progressSnapshot{
		Operations:   c.metrics.Operations().String(),
		ItemsWritten: totalItems,
		TotalBatches: c.metrics.BatchesWritten(),
		InFlight:     c.inFlight.Load(),
		HeldBack:     c.lane.len(),
		FilesDone:    c.progress.completedCount(),
		FilesTotal:   c.totalFiles,
		Throttles:    c.metrics.Throttles(),
		Retries:      c.metrics.Retries(),
		LostItems:    c.metrics.LostItems(),
		Errors:       c.metrics.Errors(),
		ActiveReader: activeReaders,
		Concurrency:  c.metrics.Concurrency(),
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

// defaultCheckpointEvery is how often progress is saved while the restore runs. It
// bounds what a restore killed outright, with no chance to save, writes again when
// resumed; an interruption it is told about saves where it stopped regardless.
const defaultCheckpointEvery = 5 * time.Second

// saveEvery saves progress at every interval in which it moved from the version last
// saved, until ctx ends. A
// failed save ends it with the error, since a restore that cannot record its progress
// should find out now rather than when it is interrupted.
func (c *Coordinator) saveEvery(ctx context.Context, saved uint64) error {
	ticker := time.NewTicker(c.checkpointEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			version := c.progress.version()
			if version == saved {
				continue
			}
			if err := c.saveProgress(ctx); err != nil {
				// Stopping is not a failed save: the final save follows.
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			saved = version
		case <-ctx.Done():
			return nil
		}
	}
}

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

// dispatched is one line handed to the batcher and not yet acknowledged. prev is the
// furthest line the reader had examined before this one, which is what the file's
// watermark falls back to while this line is still in flight.
type dispatched struct {
	offset int64
	prev   int64
	acked  bool
}

// fileWindow is how many of a file's lines may be in flight before the table has
// pushed back on any of them: two batches' worth, enough that it never holds a file
// back that the table is taking.
const fileWindow = 2 * writer.MaxBatch

// fileLedger tracks, for one data file, which lines are in flight and how far the file
// can be committed. Batches take items from many files at once and finish them out of
// order, so the offset a resume restarts past cannot be the last line written: it has
// to be the low watermark, the highest offset with nothing unfinished below it.
//
// It also holds how many of the file's lines may be in flight. A file is one slice of
// the key space, so when the table hands its items back, it is that slice's partitions
// that are short; halving the file's window slows the one reader feeding them while
// every other file goes on at the pace the table takes it. The window grows back by one
// line per window's worth acknowledged, as a congestion window does.
//
// One reader owns the dispatching end of a ledger; any batch may acknowledge into it.
// Fields are ordered largest-to-smallest for memory alignment.
type fileLedger struct {
	pending  []dispatched  // Lines handed to the batcher, ascending by offset
	key      string        // The data file this ledger belongs to
	drained  chan struct{} // Closed once the file is read to the end and nothing is in flight
	room     chan struct{} // Signalled when a line in flight is acknowledged
	window   float64       // Lines of this file that may be in flight
	examined int64         // Furthest line dispatched or skipped; where a retry resumes from
	cutMark  int64         // examined when the window was last cut; rejections at or below it do not cut again
	inFlight int           // Lines dispatched and not yet acknowledged
	mu       sync.Mutex
	eof      bool // The reader has read the file to the end
	closed   bool // drained has been closed
	warned   bool // The file has been named as one the table keeps turning away
}

// newFileLedger opens a ledger for a file, starting from the offset a previous run
// recorded, or noOffset when none did.
func newFileLedger(key string, resumed int64) *fileLedger {
	return &fileLedger{
		key:      key,
		drained:  make(chan struct{}),
		room:     make(chan struct{}, 1),
		window:   fileWindow,
		examined: resumed,
		cutMark:  resumed,
	}
}

// reached reports the furthest line the reader has dispatched or skipped. A stream
// retry starts the file again from the beginning and passes over everything up to
// this, so no line is handed to the batcher twice within a run.
func (l *fileLedger) reached() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.examined
}

// waitRoom blocks while the file has as many lines in flight as its window allows,
// returning the context's error if that ends first.
func (l *fileLedger) waitRoom(ctx context.Context) error {
	for {
		l.mu.Lock()
		open := float64(l.inFlight) < l.window
		l.mu.Unlock()
		if open {
			return nil
		}
		select {
		case <-l.room:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// dispatch records a line about to be handed to the batcher. It is called before the
// item is sent, never after: an acknowledgement arriving for a line the ledger has not
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
	l.inFlight++
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

// reject notes that the table handed back a line of this file, which says the file's
// partitions are short, and halves the window. Lines dispatched before the last cut
// were already in flight when it was made, so their rejections are the same shortage
// and do not cut again. The line stays in flight: it will be sent again.
func (l *fileLedger) reject(offset int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset <= l.cutMark {
		return
	}
	l.window = max(1, l.window/2)
	l.cutMark = l.examined
}

// ack marks a dispatched line written and returns the file's watermark: the highest
// offset with every line at or below it written or skipped. That is what a resume
// restarts past, so it never passes a line a batch still holds.
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

	// The window grows only while it is what holds the file back; a file the table is
	// taking as fast as it is read would otherwise grow a window nothing has tested.
	if float64(l.inFlight) >= l.window {
		l.window += 1 / l.window
	}
	l.inFlight--
	select {
	case l.room <- struct{}{}:
	default:
	}

	l.closeIfDrainedLocked()
	return l.watermarkLocked()
}

// markWarned records that the file has been named as stuck, reporting whether it had not
// been before.
func (l *fileLedger) markWarned() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	first := !l.warned
	l.warned = true
	return first
}

// finish records that the reader has read the file to the end. The file is done once
// the batches holding its last lines have acknowledged them.
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

// item is one decoded operation on its way from the reader that decoded it to the
// table, carrying what is needed to acknowledge it and how often the table has turned
// it away.
// Fields are ordered largest-to-smallest for memory alignment.
type item struct {
	op       itemimage.Operation
	ledger   *fileLedger
	offset   int64
	attempts uint32 // Times the table refused or handed it back
}

// readFiles takes data files from the task channel and turns each into a stream of
// items for the batcher, with the stream retried on failure.
//
// HOT PATH: Stream S3 -> Decode JSON -> hand to the batcher.
// The dominant cost is JSON decoding in parser.Decode (~27% CPU, ~99% memory).
//
// How many of these run at once is what decides how widely the restore's writes are
// spread over the target table: each file holds one contiguous slice of the exported
// key space, so items from one file land on one or a very few target partitions.
// Concurrency is c.cfg.Readers.
func (c *Coordinator) readFiles(ctx context.Context, id int, tasks <-chan manifest.FileMeta, items chan<- item) error {
	const maxRetries = 3
	// However this reader leaves, it is no longer holding a file.
	defer c.markBusy(id, false)

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
		c.markBusy(id, true)

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
						c.markActive(id)
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
					// Counted where the stream's failure is recorded, once.
					return err
				}

				// A file whose partitions the table is pushing back on waits here for
				// its lines in flight to be taken, rather than adding to them.
				if err := ledger.waitRoom(ctx); err != nil {
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

			c.recordReaderError(id, streamErr)
		}

		if streamErr != nil {
			return fmt.Errorf("failed to process file %s after %d retries: %w",
				file.Key, maxRetries, streamErr)
		}

		// The file is read, but its last lines are still in batches. It is only complete
		// once they have all been written; recording it before that would let a resume
		// skip a file with records still unwritten.
		ledger.finish()
		select {
		case <-ledger.drained:
		case <-ctx.Done():
			return ctx.Err()
		}

		// Record the file as done. This is what a resume reads to skip it, so it is
		// saved at once rather than at the next interval.
		c.progress.complete(file.Key)
		if err := c.saveProgress(ctx); err != nil {
			c.recordReaderError(id, err)
			return fmt.Errorf("failed to save completion checkpoint for file %s: %w", file.Key, err)
		}
		c.markBusy(id, false)
	}

	return nil
}

// defaultBatchLinger is how long a partly filled batch waits for more items before it
// is written anyway.
const defaultBatchLinger = 50 * time.Millisecond

// itemBuffer is how many decoded items may wait between the readers and the batcher.
const itemBuffer = 4 * writer.MaxBatch

// writeItems fills batches from whatever the readers produce and submits them, until
// the readers are done and every batch submitted has been answered, or the run stops.
// A batch that fails ends the run through fail.
//
// A batch is deliberately drawn from every file being read at once, which is the whole
// point of the split: consecutive lines of one export file share a partition, so a
// batch built from one file is a batch aimed at one partition. Ordering within a file
// is given up in exchange, which costs nothing here because an export holds one record
// per key: a full export by construction, an incremental one because it records each
// item's latest state once.
//
// Items the table did not take come back through the retry lane and go out ahead of
// new ones, mixed into batches with other files' items, so a hot partition's items do
// not travel together and are not refused together.
//
// HOT PATH: every item the restore writes passes through here. Submit blocks while the
// writer has as many batches in flight as the table is rewarding, which is what holds
// the readers back when the table is the limit.
func (c *Coordinator) writeItems(ctx context.Context, items <-chan item, fail func(error)) {
	// Every batch submitted is answered before this returns, so nothing acknowledges
	// into progress after the final save has been taken.
	var submitted sync.WaitGroup
	defer submitted.Wait()

	batch := make([]item, 0, c.batchSize)

	// A partly filled batch has to go out on its own eventually. Readers waiting for
	// their last lines to be acknowledged produce nothing more to top it up, so without
	// this the tail of a restore would wait on items that are never coming.
	linger := time.NewTimer(c.linger)
	stopTimer(linger)
	defer linger.Stop()
	lingering := false

	// Wakes the batcher when the earliest item waiting in the lane comes due.
	due := time.NewTimer(time.Hour)
	stopTimer(due)
	defer due.Stop()
	var dueAt time.Time

	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		stopTimer(linger)
		lingering = false

		sent := batch
		batch = make([]item, 0, c.batchSize)
		ops := make([]itemimage.Operation, len(sent))
		for i := range sent {
			ops[i] = sent[i].op
		}

		submitted.Add(1)
		c.inFlight.Add(1)
		start := time.Now()
		err := c.writer.Submit(ctx, ops, func(r writer.Rejection, err error) {
			defer submitted.Done()
			defer c.inFlight.Add(-1)
			if err != nil {
				c.recordWriteError(err)
				fail(fmt.Errorf("writing a batch failed: %w", err))
				return
			}
			c.settle(sent, r)
			c.metrics.RecordProcessingTime(time.Since(start))
		})
		if err != nil {
			// The batch was never started. Either the run is stopping, or the writer
			// refused it for a reason of its own, which has to stop the run too: the
			// readers are waiting on items only this batcher can write.
			submitted.Done()
			c.inFlight.Add(-1)
			if ctx.Err() == nil {
				c.recordWriteError(err)
				fail(fmt.Errorf("writing a batch failed: %w", err))
			}
			return false
		}
		return true
	}

	for {
		// Items the table turned away go out first once their wait is over.
		if c.lane.len() > 0 {
			batch = c.lane.takeDue(time.Now(), batch, c.batchSize)
			if next, ok := c.lane.nextDue(); ok && !next.Equal(dueAt) {
				dueAt = next
				resetTimer(due, time.Until(next))
			}
		}
		if len(batch) >= c.batchSize {
			if !flush() {
				return
			}
			continue
		}
		if len(batch) > 0 && !lingering {
			resetTimer(linger, c.linger)
			lingering = true
		}

		select {
		case next, ok := <-items:
			if !ok {
				// The readers are done, and a reader finishes only once every item of its
				// file is written, so nothing is waiting to be sent again.
				flush()
				return
			}
			batch = append(batch, next)
		case <-linger.C:
			lingering = false
			if !flush() {
				return
			}
		case <-c.lane.wake:
		case <-due.C:
			dueAt = time.Time{}
		case <-ctx.Done():
			return
		}
	}
}

// retryBase and retryCap bound how long an item handed back waits before it is sent
// again: twice as long after every hand-back, from the one to at most the other, with
// full jitter so items handed back together do not return together.
const (
	retryBase = 50 * time.Millisecond
	retryCap  = 5 * time.Second
)

// settle applies what the table did with a batch. Items it took are acknowledged into
// their files' ledgers and counted; items it did not take wait a jittered backoff in
// the lane and go again. An item handed back from a call the table otherwise took says
// its file's partitions are short, so its file's window is cut. Items refused in a call
// the table turned away whole say the same only when they all came from one file; from
// several, the refusal is the table as a whole running short, which the writer's pacing
// answers, and cutting every file's window on top of it would answer it twice.
func (c *Coordinator) settle(batch []item, r writer.Rejection) {
	var turnedAway [writer.MaxBatch]bool
	now := time.Now()
	turnAway := func(i int) {
		turnedAway[i] = true
		batch[i].attempts++
		c.metrics.RecordRejected(batch[i].op.Type, 1)
		c.warnIfStuck(&batch[i])
		c.lane.push(batch[i], now.Add(retryDelay(batch[i].attempts)))
	}
	for _, i := range r.HandedBack {
		batch[i].ledger.reject(batch[i].offset)
		turnAway(i)
	}
	if oneFile(batch, r.Refused) {
		batch[r.Refused[0]].ledger.reject(batch[r.Refused[0]].offset)
	}
	for _, i := range r.Refused {
		turnAway(i)
	}

	var applied, bytes [itemimage.OperationKinds]int64
	// Each file's watermark is recorded once per batch, at the furthest it reached, so
	// progress takes one lock per file rather than one per item.
	var marks [writer.MaxBatch]struct {
		ledger *fileLedger
		mark   int64
	}
	files := 0
	for i := range batch {
		if turnedAway[i] {
			continue
		}
		it := &batch[i]
		applied[it.op.Type]++
		bytes[it.op.Type] += int64(it.op.Bytes)
		mark := it.ledger.ack(it.offset)
		if mark == noOffset {
			continue
		}
		j := 0
		for j < files && marks[j].ledger != it.ledger {
			j++
		}
		if j == files {
			marks[j].ledger = it.ledger
			files++
		}
		marks[j].mark = max(marks[j].mark, mark)
	}
	// Recorded only once the write has returned, so a checkpoint taken now cannot
	// describe a line as done that DynamoDB has not accepted.
	for j := range files {
		c.progress.record(marks[j].ledger.key, marks[j].mark)
	}
	for kind := range applied {
		if applied[kind] > 0 {
			c.metrics.RecordApplied(itemimage.OperationType(kind), applied[kind], bytes[kind])
		}
	}
	c.metrics.RecordBatchWritten()
}

// retryDelay is how long an item handed back for the given time waits before it is sent
// again, drawn with full jitter.
func retryDelay(attempts uint32) time.Duration {
	ceiling := retryCap
	if shift := attempts - 1; shift < 16 {
		ceiling = min(retryCap, retryBase<<shift)
	}
	return time.Duration(rand.Int64N(int64(ceiling) + 1))
}

// retryLane holds items the table did not take until each is due to be sent again,
// earliest first. It is filled by the writer's goroutines and drained by the batcher,
// and never blocks the former on the latter: a writer's goroutine waiting on a batcher
// that is waiting on the writer for a slot would stop the restore.
// Fields are ordered largest-to-smallest for memory alignment.
type retryLane struct {
	due   retryHeap
	wake  chan struct{} // Signalled when an item is added
	count atomic.Int64  // Items held, readable without the lock
	mu    sync.Mutex
}

// newRetryLane returns an empty lane.
func newRetryLane() *retryLane {
	return &retryLane{wake: make(chan struct{}, 1)}
}

// push adds an item to be sent again from at.
func (q *retryLane) push(it item, at time.Time) {
	q.mu.Lock()
	heap.Push(&q.due, retryEntry{item: it, at: at})
	q.mu.Unlock()
	q.count.Add(1)
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// takeDue moves items due by now into batch, earliest first, until it holds limit.
func (q *retryLane) takeDue(now time.Time, batch []item, limit int) []item {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(batch) < limit && len(q.due) > 0 && !q.due[0].at.After(now) {
		batch = append(batch, heap.Pop(&q.due).(retryEntry).item)
		q.count.Add(-1)
	}
	return batch
}

// nextDue reports when the earliest item held comes due, and whether there is one.
func (q *retryLane) nextDue() (time.Time, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.due) == 0 {
		return time.Time{}, false
	}
	return q.due[0].at, true
}

// len reports how many items are held.
func (q *retryLane) len() int64 {
	return q.count.Load()
}

// retryEntry is one item held in the lane and when it is due.
type retryEntry struct {
	at   time.Time
	item item
}

// retryHeap orders held items by when they are due, for container/heap.
type retryHeap []retryEntry

func (h retryHeap) Len() int           { return len(h) }
func (h retryHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h retryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *retryHeap) Push(x any)        { *h = append(*h, x.(retryEntry)) }
func (h *retryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = retryEntry{}
	*h = old[:n-1]
	return x
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

// recordReaderError counts a reader's failure, records it against the reader and prints
// it. A counter on the progress line says a restore is in trouble but never what the
// trouble is, and a restore that survives its failures would otherwise finish without
// ever having said what it survived.
func (c *Coordinator) recordReaderError(id int, err error) {
	if c.noteError(err) {
		c.console.notice("reader %d: %v", id, err)
	}
	c.updateReader(id, func(s *readerStatus) {
		s.lastError = err
		s.lastErrorTime = time.Now()
	})
}

// recordWriteError counts a failed batch and prints it.
func (c *Coordinator) recordWriteError(err error) {
	if c.noteError(err) {
		c.console.notice("writer: %v", err)
	}
}

// noteError counts an error and reports whether it is worth printing. A context ending
// is neither: that is the restore being stopped, which the caller asked for and every
// reader and batch reports at once, so counting or printing it would bury whatever
// actually caused the stop under one from each of them.
func (c *Coordinator) noteError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	c.metrics.RecordError()
	return true
}

// oneFile reports whether the items at the given places in batch all came from the same
// file, and there is at least one.
func oneFile(batch []item, at []int) bool {
	if len(at) == 0 {
		return false
	}
	for _, i := range at[1:] {
		if batch[i].ledger != batch[at[0]].ledger {
			return false
		}
	}
	return true
}

// stuckAfter is how many times the table may turn one item away before the restore says
// which file it is from. By then the item has waited out the full retry backoff several
// times over, which a passing shortage does not explain.
const stuckAfter = 10

// warnIfStuck names, once per file, a file whose items the table keeps turning away.
// They are sent again for as long as the restore runs, since capacity may come back, so
// without this a partition that never keeps up would show only as counts that grow.
func (c *Coordinator) warnIfStuck(it *item) {
	if it.attempts == stuckAfter && it.ledger.markWarned() {
		c.console.notice("the table has turned items from %s away %d times; the part of the table they belong to is not keeping up, and they are being sent again",
			it.ledger.key, stuckAfter)
	}
}
