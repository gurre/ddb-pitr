// Package coordinator implements the worker pool pattern as specified in section 5
// of the design specification. It orchestrates the restore operation using a pool
// of workers to process files in parallel.
package coordinator

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/gurre/ddb-pitr/checkpoint"
	"github.com/gurre/ddb-pitr/config"
	"github.com/gurre/ddb-pitr/itemimage"
	"github.com/gurre/ddb-pitr/manifest"
	"github.com/gurre/ddb-pitr/metrics"
	"github.com/gurre/ddb-pitr/writer"
	"github.com/gurre/s3streamer"
)

// WorkerStatus represents the status of a worker as required by section 5.
// It tracks progress and errors for monitoring and reporting.
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

// Coordinator implements the worker pool pattern from section 5.
// It manages the restore process, including worker coordination,
// checkpoint management, and progress reporting.
type Coordinator struct {
	cfg            *config.Config
	manifest       manifest.Loader
	streamer       s3streamer.Streamer
	parser         itemimage.Decoder
	writer         writer.Writer
	store          checkpoint.Store
	metrics        *metrics.Metrics
	reportUploader ReportUploader

	// Worker management as specified in section 5
	workerStatus map[int]*WorkerStatus
	statusMu     sync.RWMutex
}

// NewCoordinator creates a new Coordinator instance with all required dependencies
func NewCoordinator(
	cfg *config.Config,
	manifest manifest.Loader,
	streamer s3streamer.Streamer,
	parser itemimage.Decoder,
	writer writer.Writer,
	store checkpoint.Store,
	reportUploader ReportUploader,
) *Coordinator {
	return &Coordinator{
		cfg:            cfg,
		manifest:       manifest,
		streamer:       streamer,
		parser:         parser,
		writer:         writer,
		store:          store,
		metrics:        metrics.NewMetrics(),
		reportUploader: reportUploader,
		workerStatus:   make(map[int]*WorkerStatus),
	}
}

// Run implements the main restore process as specified in section 5.
// It sets up signal handling, loads manifests and checkpoints,
// starts the worker pool, and coordinates the restore operation.
func (c *Coordinator) Run(ctx context.Context) error {
	// Set up signal handling
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, os.Kill)
	defer cancel()

	// Parse S3 URI to validate it
	u, err := url.Parse(c.cfg.ExportS3URI)
	if err != nil {
		return fmt.Errorf("invalid S3 URI: %w", err)
	}
	if u.Scheme != "s3" {
		return fmt.Errorf("invalid S3 URI scheme: %s", u.Scheme)
	}

	// Load manifest
	summary, err := c.manifest.Load(ctx, c.cfg.ExportS3URI)
	if err != nil {
		return fmt.Errorf("failed to load manifest: %w", err)
	}

	// Load checkpoint
	state, err := c.store.Load(ctx)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	// Set up worker pool
	tasks := make(chan manifest.FileMeta)
	results := make(chan error, c.cfg.MaxWorkers)
	var wg sync.WaitGroup

	// Start progress reporter
	if !c.cfg.DryRun {
		go c.reportProgress(ctx)
	}

	// Start workers
	for i := 0; i < c.cfg.MaxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.initWorker(workerID)
			if err := c.worker(ctx, workerID, tasks); err != nil {
				results <- fmt.Errorf("worker %d failed: %w", workerID, err)
			}
		}(i)
	}

	// Send tasks
	remainingFiles := 0
	for _, file := range summary.DataFiles {
		// Skip files we've already processed
		if file.Key < state.LastFile {
			continue
		}
		remainingFiles++

		select {
		case tasks <- file:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	close(tasks)

	// Wait for workers to finish and collect errors
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Collect worker errors until all workers complete
	var errs []error
	for {
		select {
		case err := <-results:
			if err != nil {
				errs = append(errs, err)
			}
		case <-done:
			// Workers are done, drain any remaining results
			for {
				select {
				case err := <-results:
					if err != nil {
						errs = append(errs, err)
					}
				default:
					goto finish
				}
			}
		case <-ctx.Done():
			// Wait for workers to acknowledge cancellation
			<-done
			return ctx.Err()
		}
	}

finish:
	if len(errs) > 0 {
		return fmt.Errorf("some workers failed: %v", errs)
	}

	// Flush any remaining items
	if err := c.writer.Flush(ctx); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
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

	return nil
}

// initWorker initializes a worker's status tracking as required by section 5
func (c *Coordinator) initWorker(id int) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.workerStatus[id] = &WorkerStatus{
		ID:        id,
		StartTime: time.Now(),
	}
}

// updateWorkerStatus updates a worker's status for monitoring as specified in section 5
func (c *Coordinator) updateWorkerStatus(id int, fn func(*WorkerStatus)) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	if status, ok := c.workerStatus[id]; ok {
		fn(status)
		status.LastActive = time.Now()
	}
}

// reportProgress implements the progress reporting requirements from section 5.
// It periodically reports progress to stdout.
func (c *Coordinator) reportProgress(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.statusMu.RLock()
			var totalItems, totalBatches int64
			activeWorkers := 0
			for _, status := range c.workerStatus {
				if time.Since(status.LastActive) < 10*time.Second {
					activeWorkers++
				}
				totalItems += status.ItemsWritten
				totalBatches += status.BatchesCount
			}
			c.statusMu.RUnlock()

			fmt.Printf("Progress: %d items written in %d batches (%d active workers)\n",
				totalItems, totalBatches, activeWorkers)

		case <-ctx.Done():
			return
		}
	}
}

// checkpointInterval controls how often checkpoints are saved (every N batches).
// This balances durability (frequent saves) with performance (fewer S3 API calls).
const checkpointInterval = 100

// completedFileOffset is a sentinel value indicating a file has been fully processed.
// Using -1 distinguishes "completed" from "start at offset 0".
const completedFileOffset = int64(-1)

// worker implements the worker pool pattern from section 5.
// It processes files from the task channel, handling batching,
// checkpointing, and error reporting.
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
		c.updateWorkerStatus(id, func(s *WorkerStatus) {
			s.CurrentFile = file.Key
		})

		// Load checkpoint for this file - fail fast on persistent errors
		state, err := c.store.Load(ctx)
		if err != nil {
			c.recordError(id, err)
			return fmt.Errorf("failed to load checkpoint for file %s: %w", file.Key, err)
		}

		// Determine starting offset
		offset := int64(0)
		if file.Key == state.LastFile {
			// If file was completed (sentinel value), skip it entirely
			if state.LastByteOffset == completedFileOffset {
				continue
			}
			offset = state.LastByteOffset
		}

		// Track current byte offset and batch count for checkpointing
		var currentOffset int64
		var batchesSinceCheckpoint int

		// Stream and process the file with retries
		var streamErr error
		for retry := 0; retry < maxRetries; retry++ {
			if retry > 0 {
				select {
				case <-time.After(time.Duration(1<<uint(retry)) * time.Second):
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			// HOT PATH: Inner loop - callback invoked for every JSON line from S3
			streamErr = c.streamer.Stream(ctx, bucket, file.Key, offset, func(line []byte, byteOffset int64) error {
				// Track the current position for checkpoint saves
				currentOffset = byteOffset

				// Decode is the main CPU/memory bottleneck (~27% CPU, ~99% memory)
				op, err := c.parser.Decode(line)
				if err == itemimage.ErrCorrupt {
					c.metrics.RecordCorrupt()
					return nil
				}
				if err != nil {
					c.metrics.RecordError()
					return err
				}

				batch = append(batch, op)
				c.metrics.RecordProcessed()

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

		// Save final checkpoint marking file as complete using sentinel value
		if err := c.store.Save(ctx, checkpoint.State{
			ExportID:       file.Key,
			LastFile:       file.Key,
			LastByteOffset: completedFileOffset,
		}); err != nil {
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

	c.updateWorkerStatus(id, func(s *WorkerStatus) {
		s.ItemsWritten += int64(len(batch))
		s.BatchesCount++
	})

	// Only save checkpoint at intervals to reduce S3 API calls
	if shouldCheckpoint {
		if err := c.store.Save(ctx, checkpoint.State{
			ExportID:       file.Key,
			LastFile:       file.Key,
			LastByteOffset: offset,
		}); err != nil {
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
