// Package coordinator drives a restore: it loads and verifies the export, hands its
// data files to a pool of workers, checkpoints their progress and reports the outcome.
package coordinator

import (
	"context"
	"errors"
	"fmt"
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

// WorkerStatus is what one worker has done and where it is, for the progress line.
// Fields are ordered largest-to-smallest for optimal memory alignment.
type WorkerStatus struct {
	LastErrorTime time.Time // When the last error occurred (24 bytes)
	StartTime     time.Time // When the worker started (24 bytes)
	LastActive    time.Time // Last activity timestamp (24 bytes)
	LastError     error     // Last error encountered (16 bytes - interface)
	CurrentFile   string    // Currently processing file (16 bytes - string header)
	ItemsWritten  int64     // Number of items written (8 bytes)
	BatchesCount  int64     // Number of batches processed (8 bytes)
	ID            int       // Worker identifier (8 bytes on 64-bit)
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

	// progress owns what the restore has finished; the store is only ever written
	// from a snapshot of it, taken under saveMu so the object in S3 advances in the
	// same order the snapshots were taken.
	progress *progress
	saveMu   sync.Mutex

	// Per-worker status, for the progress line
	workerStatus map[int]*WorkerStatus
	statusMu     sync.RWMutex

	// Progress tracking for percentage and throughput calculation
	corruptNamed       atomic.Int64 // Skipped lines named on stderr so far
	totalExpectedItems int64        // Total items expected from manifest
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
	mu        sync.Mutex
}

// newProgress reopens the progress a previous run recorded for the given export.
func newProgress(exportID string, state checkpoint.State) *progress {
	p := &progress{
		completed: make(map[string]struct{}, len(state.Completed)),
		offsets:   make(map[string]int64, len(state.Offsets)),
		exportID:  exportID,
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

	return checkpoint.State{ExportID: p.exportID, Completed: completed, Offsets: offsets}
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
		progress:       newProgress("", checkpoint.State{}),
		workerStatus:   make(map[int]*WorkerStatus),
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
	fmt.Printf("Verifying %d data files against the manifest\n", len(remaining.DataFiles))
	verification, err := c.manifest.VerifyChecksums(ctx, c.cfg.GetExportBucketName(), remaining)
	if err != nil {
		return fmt.Errorf("export failed verification: %w", err)
	}
	fmt.Printf("Verified %d of %d data files against the manifest\n",
		verification.Verified, len(remaining.DataFiles))
	if len(verification.Unverified) > 0 {
		fmt.Printf("%d data files carry no checksum that can be compared\n", len(verification.Unverified))
	}

	// Store total expected items for progress percentage calculation
	c.totalExpectedItems = summary.ItemCount
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
	var wg sync.WaitGroup

	// The reporter is stopped and waited for below rather than left to notice the run
	// ending, so its last line cannot land in the middle of the final report.
	reportCtx, stopReporting := context.WithCancel(ctx)
	defer stopReporting()
	reporterDone := make(chan struct{})
	go func() {
		defer close(reporterDone)
		c.reportProgress(reportCtx)
	}()

	// Start workers
	for i := 0; i < c.cfg.MaxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.initWorker(workerID)
			if err := c.worker(runCtx, workerID, tasks); err != nil {
				fail(fmt.Errorf("worker %d failed: %w", workerID, err))
			}
		}(i)
	}

	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	// Send tasks. Handing one to a pool that has already given up would block forever,
	// so a pool that has exited or been stopped ends dispatch.
dispatch:
	for _, file := range summary.DataFiles {
		if _, done := c.progress.resume(file.Key); done {
			continue
		}

		select {
		case tasks <- file:
		case <-workersDone:
			break dispatch
		case <-runCtx.Done():
			break dispatch
		}
	}
	close(tasks)

	<-workersDone
	stopReporting()
	<-reporterDone

	// Whatever the workers got through is saved once they have all stopped, however
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
		return shutdownError(err, saveErr)
	}
	if firstFailure != nil {
		return shutdownError(firstFailure, saveErr)
	}
	if saveErr != nil {
		return fmt.Errorf("failed to save final checkpoint: %w", saveErr)
	}

	// Generate and print report
	report := c.metrics.GenerateReport()
	fmt.Println(report)

	// Upload report to S3 if configured
	if c.cfg.ReportS3URI != "" && c.reportUploader != nil {
		if err := c.reportUploader.UploadReport(ctx, c.cfg.ReportS3URI, report); err != nil {
			return fmt.Errorf("failed to upload report: %w", err)
		}
		fmt.Printf("Report uploaded to %s\n", c.cfg.ReportS3URI)
	}

	// Judged after the report is out, so the record of what was skipped survives the
	// failure it causes.
	if skipped := c.metrics.CorruptCount(); skipped > 0 {
		return fmt.Errorf("%w: %d lines could not be decoded; running again will not change that",
			ErrRecordsSkipped, skipped)
	}

	return nil
}

// corruptLinesNamed caps how many skipped lines are named on stderr. The count in the
// report covers the rest; naming every one of a badly damaged file would drown the log.
const corruptLinesNamed = 20

// skipCorrupt records a line that could not be decoded and names the first few, with
// the file and offset an operator needs to find them.
func (c *Coordinator) skipCorrupt(key string, offset int64, err error) {
	c.metrics.RecordCorrupt()
	if c.corruptNamed.Add(1) <= corruptLinesNamed {
		fmt.Fprintf(os.Stderr, "skipping %s at offset %d: %v\n", key, offset, err)
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

// initWorker initializes a worker's status tracking
func (c *Coordinator) initWorker(id int) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.workerStatus[id] = &WorkerStatus{
		ID:        id,
		StartTime: time.Now(),
	}
}

// updateWorkerStatus applies fn to a worker's status and stamps it as active
func (c *Coordinator) updateWorkerStatus(id int, fn func(*WorkerStatus)) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	if status, ok := c.workerStatus[id]; ok {
		fn(status)
		status.LastActive = time.Now()
	}
}

// workerIdleTimeout is how long a worker may go without activity before the progress
// line stops counting it among the active workers.
const workerIdleTimeout = 10 * time.Second

// bytesPerMB converts the byte counters into the megabytes the progress line reports.
const bytesPerMB = 1024 * 1024

// progressSnapshot is the point-in-time view of the restore that the progress line renders.
// Fields are ordered largest-to-smallest for memory alignment.
type progressSnapshot struct {
	ItemsPerSec   float64 // Items written per second since the previous snapshot
	MBPerSec      float64 // Megabytes of export read per second since the previous snapshot
	Percent       float64 // Share of the manifest's item count written so far, capped at 100
	TotalBatches  int64   // Batches written since the restore started
	Throttles     int64
	Retries       int64
	LostItems     int64
	Errors        int64
	ActiveWorkers int // Workers that reported activity within workerIdleTimeout
}

// String renders the line an operator watches while a restore runs. Every number is
// labelled where it appears, because the throttle, retry, lost and error counts drive
// different responses and reading one as another sends the operator the wrong way.
func (s progressSnapshot) String() string {
	return fmt.Sprintf(
		"Progress: %.1f%% (%.0f/s, %.1f MB/s) | %d batches | %d workers | %d throttles | %d retries | %d lost | %d errors",
		s.Percent, s.ItemsPerSec, s.MBPerSec, s.TotalBatches, s.ActiveWorkers,
		s.Throttles, s.Retries, s.LostItems, s.Errors)
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
	activeWorkers := 0
	for _, status := range c.workerStatus {
		if now.Sub(status.LastActive) < workerIdleTimeout {
			activeWorkers++
		}
		totalItems += status.ItemsWritten
		totalBatches += status.BatchesCount
	}
	c.statusMu.RUnlock()

	bytesRead := c.metrics.BytesRead()

	snap := progressSnapshot{
		TotalBatches:  totalBatches,
		Throttles:     c.metrics.Throttles(),
		Retries:       c.metrics.Retries(),
		LostItems:     c.metrics.LostItems(),
		Errors:        c.metrics.Errors(),
		ActiveWorkers: activeWorkers,
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

// reportProgress prints the progress line once a second, overwriting the previous
// one. A line shorter than its predecessor is padded so nothing of the old line shows.
func (c *Coordinator) reportProgress(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	width := 0
	for {
		select {
		case <-ticker.C:
			line := c.snapshot(time.Now()).String()
			fmt.Printf("\r%-*s", width, line)
			width = len(line)

		case <-ctx.Done():
			// Print newline before exit so final output appears on new line
			fmt.Println()
			return
		}
	}
}

// checkpointInterval controls how often checkpoints are saved (every N batches).
// This balances durability (frequent saves) with performance (fewer S3 API calls).
const checkpointInterval = 100

// saveProgress persists a snapshot of what the restore has finished. Snapshotting and
// writing under one lock keeps the stored state monotonic: without it a worker that
// snapshotted earlier could win the race to S3 and undo a later worker's progress.
func (c *Coordinator) saveProgress(ctx context.Context) error {
	c.saveMu.Lock()
	defer c.saveMu.Unlock()
	return c.store.Save(ctx, c.progress.snapshot())
}

// worker takes files from the task channel and restores each: streaming, decoding,
// batching, writing and checkpointing, with the stream retried on failure.
//
// HOT PATH: Core processing loop that orchestrates the data pipeline.
// Each worker runs: Stream S3 -> Decode JSON -> Batch -> Write DynamoDB
//
// The main performance bottlenecks in order are:
//  1. JSON decoding in parser.Decode (~27% CPU, ~99% memory)
//  2. Network I/O to S3 and DynamoDB
//  3. Checkpoint saves (mitigated by batching every checkpointInterval batches)
//
// Concurrency is controlled by c.cfg.MaxWorkers.
func (c *Coordinator) worker(ctx context.Context, id int, tasks <-chan manifest.FileMeta) error {
	batch := make([]itemimage.Operation, 0, c.cfg.BatchSize)
	const maxRetries = 3

	// Use the bucket from the config
	bucket := c.cfg.GetExportBucketName()

	for file := range tasks {
		// A file handed over as the run stops is surrendered rather than started, so
		// stopping never means finishing whatever was in flight.
		if err := ctx.Err(); err != nil {
			return err
		}
		c.updateWorkerStatus(id, func(s *WorkerStatus) {
			s.CurrentFile = file.Key
		})

		if _, done := c.progress.resume(file.Key); done {
			continue
		}

		// Offset of the last line handed to the writer, and batches since the last save.
		var currentOffset int64
		var batchesSinceCheckpoint int

		// Furthest line examined in this file across attempts. A corrupt line sits
		// above the last written one, so a retry re-reads it; without this it would
		// be counted and named again on every attempt.
		examined, _ := c.progress.resume(file.Key)

		// Stream and process the file with retries
		var streamErr error
		for retry := 0; retry < maxRetries; retry++ {
			// The first attempt is not paced; every one after it waits longer than the
			// last, so a file failing against a struggling S3 does not hammer it. A wait
			// cut short means the restore is stopping, and the file is unfinished, so it
			// is surrendered rather than left to be recorded as complete below.
			if retry > 0 && !c.backoff.Wait(ctx, retry) {
				return fmt.Errorf("stopped retrying file %s: %w", file.Key, stopRetrying(ctx))
			}

			// Every attempt reads the file from its start and skips what is already
			// written, taken fresh from progress so a retry continues from the furthest
			// point the failed attempt reached. Whatever the failed attempt left
			// buffered goes with it: those lines are re-read and re-skipped or
			// re-written by the next attempt, never carried over and written twice.
			//
			// The file is read from the start because the streamer's offset is a
			// position in the stored object, while the offsets the callback reports and
			// progress records are positions in the decompressed stream. The two only
			// agree for an uncompressed file, and exports are gzipped.
			startOffset, _ := c.progress.resume(file.Key)
			batch = batch[:0]
			batchesSinceCheckpoint = 0
			currentOffset = startOffset

			// HOT PATH: Inner loop - callback invoked for every JSON line from S3
			streamErr = c.streamer.Stream(ctx, bucket, file.Key, streamFromStart, func(line []byte, byteOffset int64) error {
				// Lines up to and including the recorded one are already written. The
				// check precedes decoding, which is where the CPU and memory go.
				if byteOffset <= startOffset {
					return nil
				}
				seen := byteOffset <= examined
				if !seen {
					examined = byteOffset
				}

				// Decode is the main CPU/memory bottleneck (~27% CPU, ~99% memory)
				op, err := c.parser.Decode(line)
				if errors.Is(err, itemimage.ErrCorrupt) {
					if !seen {
						c.skipCorrupt(file.Key, byteOffset, err)
					}
					return nil
				}
				if err != nil {
					c.metrics.RecordError()
					return err
				}

				currentOffset = byteOffset
				batch = append(batch, op)

				if len(batch) >= c.cfg.BatchSize {
					batchesSinceCheckpoint++
					shouldCheckpoint := batchesSinceCheckpoint >= checkpointInterval
					if err := c.writeBatch(ctx, id, batch, file, currentOffset, shouldCheckpoint); err != nil {
						return err
					}
					if shouldCheckpoint {
						batchesSinceCheckpoint = 0
					}
					batch = batch[:0]
				}

				return nil
			})

			if streamErr == nil {
				break
			}

			c.recordError(id, streamErr)
		}

		if streamErr != nil {
			return fmt.Errorf("failed to process file %s after %d retries: %w",
				file.Key, maxRetries, streamErr)
		}

		// Write any remaining items with checkpoint
		if len(batch) > 0 {
			if err := c.writeBatch(ctx, id, batch, file, currentOffset, true); err != nil {
				return err
			}
			batch = batch[:0]
		}

		// Record the file as done. This is what a resume reads to skip it, so it is
		// saved unconditionally rather than at the batch interval.
		c.progress.complete(file.Key)
		if err := c.saveProgress(ctx); err != nil {
			c.recordError(id, err)
			return fmt.Errorf("failed to save completion checkpoint for file %s: %w", file.Key, err)
		}
	}

	return nil
}

// writeBatch writes a batch of operations with metrics.
// If shouldCheckpoint is true, saves progress to checkpoint store.
func (c *Coordinator) writeBatch(ctx context.Context, id int, batch []itemimage.Operation,
	file manifest.FileMeta, offset int64, shouldCheckpoint bool) error {
	start := time.Now()
	if err := c.writer.WriteBatch(ctx, batch); err != nil {
		c.recordError(id, err)
		return err
	}
	c.metrics.RecordProcessingTime(time.Since(start))
	c.metrics.RecordBatchWritten()
	c.metrics.RecordProcessed(int64(len(batch)))

	c.updateWorkerStatus(id, func(s *WorkerStatus) {
		s.ItemsWritten += int64(len(batch))
		s.BatchesCount++
	})

	// The offset of the batch's last line is recorded on every batch so any checkpoint,
	// whichever worker takes it, carries the furthest point every file has reached.
	// Writing it out is what costs an S3 call, so that still happens only at intervals.
	c.progress.record(file.Key, offset)
	if shouldCheckpoint {
		if err := c.saveProgress(ctx); err != nil {
			c.recordError(id, err)
			return err
		}
	}

	return nil
}

// recordError records a worker error
func (c *Coordinator) recordError(id int, err error) {
	c.metrics.RecordError()
	c.updateWorkerStatus(id, func(s *WorkerStatus) {
		s.LastError = err
		s.LastErrorTime = time.Now()
	})
}
