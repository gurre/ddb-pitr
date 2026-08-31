package coordinator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gurre/ddb-pitr/checkpoint"
	"github.com/gurre/ddb-pitr/config"
	"github.com/gurre/ddb-pitr/itemimage"
	"github.com/gurre/ddb-pitr/manifest"
	"github.com/gurre/ddb-pitr/metrics"
)

// testReportURI is where tests ask for the final report to be uploaded.
const testReportURI = "s3://reports/restore-001.json"

// TestCoordinatorHappyPath verifies a single-file export is streamed, decoded and
// written in one batch, which is the baseline every other behaviour builds on.
func TestCoordinatorHappyPath(t *testing.T) {
	writer := &mockWriter{}
	coord, _ := newTestCoordinator(t, testDeps{
		files:  []manifest.FileMeta{{Key: "file1", ItemCount: 2}},
		lines:  [][]byte{[]byte(`{"id":"123"}`), []byte(`{"id":"124"}`)},
		writer: writer,
		configure: func(cfg *config.Config) {
			cfg.BatchSize = 10
		},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	if len(writer.batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(writer.batches))
	}
	if len(writer.batches[0]) != 2 {
		t.Errorf("expected 2 operations in batch, got %d", len(writer.batches[0]))
	}
}

// TestCoordinatorFlushesFullBatches verifies operations are handed to the writer in
// batches of the configured size rather than accumulating until end of file, which is
// what keeps memory flat across a multi-terabyte export.
func TestCoordinatorFlushesFullBatches(t *testing.T) {
	writer := &mockWriter{}
	lines := make([][]byte, 5)
	for i := range lines {
		lines[i] = []byte(`{"id":"1"}`)
	}
	coord, _ := newTestCoordinator(t, testDeps{
		files:  []manifest.FileMeta{{Key: "file1", ItemCount: 5}},
		lines:  lines,
		writer: writer,
		configure: func(cfg *config.Config) {
			cfg.BatchSize = 2
		},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	var sizes []int
	for _, batch := range writer.batches {
		sizes = append(sizes, len(batch))
	}
	if len(sizes) != 3 || sizes[0] != 2 || sizes[1] != 2 || sizes[2] != 1 {
		t.Errorf("expected batches of 2, 2 and 1, got %v", sizes)
	}
}

// TestCoordinatorSkipsCompletedFile verifies a file the checkpoint marks as complete is
// not streamed again while the files after it still are. Re-reading a completed file is
// the difference between resuming a restore and restarting it; stopping at it would
// abandon the rest of the export.
func TestCoordinatorSkipsCompletedFile(t *testing.T) {
	streamer := &mockStreamer{lines: [][]byte{[]byte(`{"id":"1"}`)}}
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{
			{Key: "file1", ItemCount: 1},
			{Key: "file2", ItemCount: 1},
		},
		streamer: streamer,
		store:    &mockStore{state: checkpoint.State{LastFile: "file1", LastByteOffset: completedFileOffset}},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	var keys []string
	for _, req := range streamer.requests {
		keys = append(keys, req.key)
	}
	if len(keys) != 1 || keys[0] != "file2" {
		t.Errorf("expected only file2 to be streamed, got %v", keys)
	}
}

// TestCoordinatorCheckpointsAtInterval verifies progress is recorded every
// checkpointInterval batches at the offset reached, then again for the trailing partial
// batch and finally with the completion sentinel. Checkpointing too rarely loses work on
// an interrupted restore; too often turns S3 into the bottleneck.
func TestCoordinatorCheckpointsAtInterval(t *testing.T) {
	// Two lines per batch, so batch N completes at line offset 2N-1. With 401 lines the
	// 100th and 200th batches land on offsets 199 and 399, and line 400 trails behind.
	lines := make([][]byte, 401)
	for i := range lines {
		lines[i] = []byte(`{"id":"1"}`)
	}
	store := &mockStore{}
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{{Key: "file1", ItemCount: int64(len(lines))}},
		lines: lines,
		store: store,
		configure: func(cfg *config.Config) {
			cfg.BatchSize = 2
		},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	want := []int64{199, 399, 400, completedFileOffset}
	got := store.savedOffsets()
	if len(got) != len(want) {
		t.Fatalf("checkpoint offsets = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("checkpoint offsets = %v, want %v", got, want)
		}
	}
}

// TestCoordinatorSkipsEmptyTrailingBatch verifies a file whose item count divides evenly
// into batches ends without an extra empty write and without an extra checkpoint.
func TestCoordinatorSkipsEmptyTrailingBatch(t *testing.T) {
	lines := make([][]byte, 4)
	for i := range lines {
		lines[i] = []byte(`{"id":"1"}`)
	}
	store := &mockStore{}
	writer := &mockWriter{}
	coord, _ := newTestCoordinator(t, testDeps{
		files:  []manifest.FileMeta{{Key: "file1", ItemCount: 4}},
		lines:  lines,
		store:  store,
		writer: writer,
		configure: func(cfg *config.Config) {
			cfg.BatchSize = 2
		},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	if len(writer.batches) != 2 {
		t.Errorf("expected 2 batches, got %d", len(writer.batches))
	}
	if got := store.savedOffsets(); len(got) != 1 || got[0] != completedFileOffset {
		t.Errorf("expected only the completion checkpoint, got offsets %v", got)
	}
}

// TestCoordinatorDoesNotCarryItemsBetweenFiles verifies the batch buffer is emptied after
// a file's trailing partial batch. A retained item would be written a second time with the
// next file, duplicating it in the restored table.
func TestCoordinatorDoesNotCarryItemsBetweenFiles(t *testing.T) {
	writer := &mockWriter{}
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{
			{Key: "file1", ItemCount: 1},
			{Key: "file2", ItemCount: 1},
		},
		lines:  [][]byte{[]byte(`{"id":"1"}`)},
		writer: writer,
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	if len(writer.batches) != 2 {
		t.Fatalf("expected 1 batch per file, got %d", len(writer.batches))
	}
	for i, batch := range writer.batches {
		if len(batch) != 1 {
			t.Errorf("batch %d carried %d operations, want 1", i, len(batch))
		}
	}
}

// TestCoordinatorTracksWorkerTotals verifies every worker in the pool is registered and
// that written items and batches accumulate into the progress the operator sees.
func TestCoordinatorTracksWorkerTotals(t *testing.T) {
	lines := make([][]byte, 6)
	for i := range lines {
		lines[i] = []byte(`{"id":"1"}`)
	}
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{{Key: "file1", ItemCount: 6}},
		lines: lines,
		configure: func(cfg *config.Config) {
			cfg.BatchSize = 2
			cfg.MaxWorkers = 3
		},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	coord.statusMu.RLock()
	pool := len(coord.workerStatus)
	coord.statusMu.RUnlock()
	if pool != 3 {
		t.Errorf("expected a pool of 3 workers, got %d", pool)
	}

	snap := coord.snapshot(time.Now())
	if snap.TotalBatches != 3 {
		t.Errorf("expected 3 batches counted, got %d", snap.TotalBatches)
	}
	// Six items against the six the manifest promised.
	if snap.Percent != 100 {
		t.Errorf("expected 100%% of the expected items written, got %f", snap.Percent)
	}
}

// TestCoordinatorResumesFromRecordedOffset verifies an interrupted file is resumed at
// the byte offset the checkpoint recorded, so items already written are not written twice.
func TestCoordinatorResumesFromRecordedOffset(t *testing.T) {
	streamer := &mockStreamer{lines: [][]byte{[]byte(`{"id":"1"}`)}}
	coord, _ := newTestCoordinator(t, testDeps{
		files:    []manifest.FileMeta{{Key: "file1", ItemCount: 1}},
		streamer: streamer,
		store:    &mockStore{state: checkpoint.State{LastFile: "file1", LastByteOffset: 4096}},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	if len(streamer.requests) != 1 {
		t.Fatalf("expected 1 stream request, got %d", len(streamer.requests))
	}
	if streamer.requests[0].offset != 4096 {
		t.Errorf("expected the stream to resume at offset 4096, got %d", streamer.requests[0].offset)
	}
}

// TestCoordinatorStartsUnrelatedFileAtZero verifies the recorded offset is only applied
// to the file it belongs to. Applying it to another file would silently skip its head.
func TestCoordinatorStartsUnrelatedFileAtZero(t *testing.T) {
	streamer := &mockStreamer{lines: [][]byte{[]byte(`{"id":"1"}`)}}
	coord, _ := newTestCoordinator(t, testDeps{
		files:    []manifest.FileMeta{{Key: "file2", ItemCount: 1}},
		streamer: streamer,
		store:    &mockStore{state: checkpoint.State{LastFile: "file1", LastByteOffset: 4096}},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	if len(streamer.requests) != 1 {
		t.Fatalf("expected 1 stream request, got %d", len(streamer.requests))
	}
	if streamer.requests[0].offset != 0 {
		t.Errorf("expected an unrelated file to start at offset 0, got %d", streamer.requests[0].offset)
	}
}

// TestCoordinatorSkipsFilesBeforeCheckpoint verifies files ordered before the checkpoint
// are not dispatched at all, which is what makes a resume cheaper than a restart.
func TestCoordinatorSkipsFilesBeforeCheckpoint(t *testing.T) {
	streamer := &mockStreamer{lines: [][]byte{[]byte(`{"id":"1"}`)}}
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{
			{Key: "file1", ItemCount: 1},
			{Key: "file2", ItemCount: 1},
			{Key: "file3", ItemCount: 1},
		},
		streamer: streamer,
		store:    &mockStore{state: checkpoint.State{LastFile: "file2", LastByteOffset: 0}},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	var keys []string
	for _, req := range streamer.requests {
		keys = append(keys, req.key)
	}
	if len(keys) != 2 || keys[0] != "file2" || keys[1] != "file3" {
		t.Errorf("expected file2 and file3 to be streamed, got %v", keys)
	}
}

// TestCoordinatorMarksFileComplete verifies the sentinel offset is written once a file
// has been fully processed, so a later run can tell "finished" from "start at zero".
func TestCoordinatorMarksFileComplete(t *testing.T) {
	store := &mockStore{}
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{{Key: "file1", ItemCount: 1}},
		lines: [][]byte{[]byte(`{"id":"1"}`)},
		store: store,
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	final := store.lastSaved()
	if final.LastFile != "file1" {
		t.Errorf("expected the completed file recorded as file1, got %q", final.LastFile)
	}
	if final.LastByteOffset != completedFileOffset {
		t.Errorf("expected the completion sentinel %d, got %d", completedFileOffset, final.LastByteOffset)
	}
}

// TestCoordinatorRetriesFailedStream verifies a file whose stream fails is retried
// rather than abandoned, since S3 reads fail transiently on long restores.
func TestCoordinatorRetriesFailedStream(t *testing.T) {
	streamer := &mockStreamer{
		lines: [][]byte{[]byte(`{"id":"1"}`)},
		errs:  []error{errors.New("connection reset"), nil},
	}
	coord, m := newTestCoordinator(t, testDeps{
		files:    []manifest.FileMeta{{Key: "file1", ItemCount: 1}},
		streamer: streamer,
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	if len(streamer.requests) != 2 {
		t.Errorf("expected the stream to be retried once, got %d requests", len(streamer.requests))
	}
	if m.Errors() != 1 {
		t.Errorf("expected the failed attempt recorded as 1 error, got %d", m.Errors())
	}
}

// TestCoordinatorFailsWhenStreamKeepsFailing verifies a file that never streams ends the
// restore with an error instead of being reported as a success with missing data.
func TestCoordinatorFailsWhenStreamKeepsFailing(t *testing.T) {
	streamer := &mockStreamer{
		lines: [][]byte{[]byte(`{"id":"1"}`)},
		errs:  []error{errors.New("access denied")},
	}
	coord, _ := newTestCoordinator(t, testDeps{
		files:    []manifest.FileMeta{{Key: "file1", ItemCount: 1}},
		streamer: streamer,
	})

	err := runCoordinator(t, coord)
	if err == nil {
		t.Fatal("expected an error when the file never streams")
	}
	if !strings.Contains(err.Error(), "access denied") {
		t.Errorf("expected the stream failure surfaced, got %v", err)
	}
	if len(streamer.requests) != 3 {
		t.Errorf("expected 3 stream attempts, got %d", len(streamer.requests))
	}
}

// TestCoordinatorFailsOnWriteError verifies a write failure stops the restore. Continuing
// past it would report a complete restore that is missing items.
func TestCoordinatorFailsOnWriteError(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{
		files:  []manifest.FileMeta{{Key: "file1", ItemCount: 1}},
		lines:  [][]byte{[]byte(`{"id":"1"}`)},
		writer: &mockWriter{err: errors.New("table not found")},
	})

	if err := runCoordinator(t, coord); err == nil {
		t.Fatal("expected an error when the writer fails")
	}
}

// TestCoordinatorFailsOnManifestError verifies an unreadable manifest fails before any
// worker starts, rather than restoring an empty export.
func TestCoordinatorFailsOnManifestError(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{
		loader: &mockLoader{err: errors.New("no such key")},
	})

	if err := runCoordinator(t, coord); err == nil {
		t.Fatal("expected an error when the manifest cannot be loaded")
	}
}

// TestCoordinatorFailsOnCheckpointLoadError verifies an unreadable checkpoint stops the
// restore. Treating it as "no progress" would silently reprocess the whole export.
func TestCoordinatorFailsOnCheckpointLoadError(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{{Key: "file1", ItemCount: 1}},
		store: &mockStore{loadErr: errors.New("access denied")},
	})

	if err := runCoordinator(t, coord); err == nil {
		t.Fatal("expected an error when the checkpoint cannot be loaded")
	}
}

// TestCoordinatorFailsOnCheckpointSaveError verifies a checkpoint that cannot be saved
// fails the restore, since silently continuing produces progress that cannot be resumed.
func TestCoordinatorFailsOnCheckpointSaveError(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{{Key: "file1", ItemCount: 1}},
		lines: [][]byte{[]byte(`{"id":"1"}`)},
		store: &mockStore{saveErr: errors.New("access denied")},
	})

	if err := runCoordinator(t, coord); err == nil {
		t.Fatal("expected an error when the checkpoint cannot be saved")
	}
}

// TestCoordinatorRejectsNonS3Export verifies the export location is rejected before any
// AWS call when it is not an S3 URI.
func TestCoordinatorRejectsNonS3Export(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{{Key: "file1", ItemCount: 1}},
	})
	coord.cfg.ExportS3URI = "https://example.com/export"

	if err := runCoordinator(t, coord); err == nil {
		t.Fatal("expected an error for a non-S3 export URI")
	}
}

// TestCoordinatorCountsCorruptLines verifies a line the decoder rejects as corrupt is
// counted and skipped rather than failing the restore, since one bad line in a
// multi-terabyte export should not cost the whole run.
func TestCoordinatorCountsCorruptLines(t *testing.T) {
	writer := &mockWriter{}
	coord, m := newTestCoordinator(t, testDeps{
		files:   []manifest.FileMeta{{Key: "file1", ItemCount: 2}},
		lines:   [][]byte{[]byte(`corrupt`), []byte(`{"id":"1"}`)},
		decoder: &mockDecoder{corruptLines: map[string]bool{"corrupt": true}},
		writer:  writer,
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	if len(writer.batches) != 1 || len(writer.batches[0]) != 1 {
		t.Errorf("expected only the decodable line written, got %v", writer.batches)
	}
	if got := m.GenerateReport().CorruptCount; got != 1 {
		t.Errorf("expected 1 corrupt line recorded, got %d", got)
	}
}

// TestCoordinatorUploadsReport verifies the final report reaches the configured S3 URI,
// which is the only record of the restore once the process exits.
func TestCoordinatorUploadsReport(t *testing.T) {
	uploader := &mockUploader{}
	coord, _ := newTestCoordinator(t, testDeps{
		files:    []manifest.FileMeta{{Key: "file1", ItemCount: 1}},
		lines:    [][]byte{[]byte(`{"id":"1"}`)},
		uploader: uploader,
		configure: func(cfg *config.Config) {
			cfg.ReportS3URI = testReportURI
		},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	if len(uploader.uris) != 1 || uploader.uris[0] != testReportURI {
		t.Errorf("expected the report uploaded to the configured URI, got %v", uploader.uris)
	}
}

// TestCoordinatorSkipsReportUploadWhenUnconfigured verifies no report is uploaded when
// no report URI was given, even though an uploader is wired in.
func TestCoordinatorSkipsReportUploadWhenUnconfigured(t *testing.T) {
	uploader := &mockUploader{}
	coord, _ := newTestCoordinator(t, testDeps{
		files:    []manifest.FileMeta{{Key: "file1", ItemCount: 1}},
		lines:    [][]byte{[]byte(`{"id":"1"}`)},
		uploader: uploader,
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	if len(uploader.uris) != 0 {
		t.Errorf("expected no report upload, got %v", uploader.uris)
	}
}

// TestCoordinatorFailsWhenReportUploadFails verifies a report that cannot be uploaded
// fails the run, so an operator is never left believing a record exists when it does not.
func TestCoordinatorFailsWhenReportUploadFails(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{
		files:    []manifest.FileMeta{{Key: "file1", ItemCount: 1}},
		lines:    [][]byte{[]byte(`{"id":"1"}`)},
		uploader: &mockUploader{err: errors.New("access denied")},
		configure: func(cfg *config.Config) {
			cfg.ReportS3URI = testReportURI
		},
	})

	if err := runCoordinator(t, coord); err == nil {
		t.Fatal("expected an error when the report upload fails")
	}
}

// TestSnapshotMeasuresRatesOverTheInterval verifies the progress line reports the rate
// since the previous snapshot, and converts bytes to megabytes, so an operator watching
// it sees current speed rather than a lifetime average.
func TestSnapshotMeasuresRatesOverTheInterval(t *testing.T) {
	coord, m := newTestCoordinator(t, testDeps{})
	start := time.Now()
	coord.lastReportTime = start

	coord.initWorker(0)
	coord.updateWorkerStatus(0, func(s *WorkerStatus) { s.ItemsWritten = 400 })
	m.RecordBytes(4 * 1024 * 1024)

	snap := coord.snapshot(start.Add(2 * time.Second))

	if snap.ItemsPerSec != 200 {
		t.Errorf("expected 200 items/sec, got %f", snap.ItemsPerSec)
	}
	if snap.MBPerSec != 2 {
		t.Errorf("expected 2 MB/sec, got %f", snap.MBPerSec)
	}
}

// TestSnapshotReportsZeroRatesWithoutElapsedTime verifies two snapshots taken at the same
// instant report zero rather than dividing by a zero interval, which would put NaN on the
// progress line.
func TestSnapshotReportsZeroRatesWithoutElapsedTime(t *testing.T) {
	coord, m := newTestCoordinator(t, testDeps{})
	now := time.Now()
	coord.lastReportTime = now

	coord.initWorker(0)
	coord.updateWorkerStatus(0, func(s *WorkerStatus) { s.ItemsWritten = 400 })
	m.RecordBytes(4 * 1024 * 1024)

	snap := coord.snapshot(now)

	if snap.ItemsPerSec != 0 {
		t.Errorf("expected 0 items/sec over a zero interval, got %f", snap.ItemsPerSec)
	}
	if snap.MBPerSec != 0 {
		t.Errorf("expected 0 MB/sec over a zero interval, got %f", snap.MBPerSec)
	}
}

// TestSnapshotReportsRatesRelativeToPreviousSnapshot verifies the rolling window advances:
// a second snapshot reports only the work done since the first.
func TestSnapshotReportsRatesRelativeToPreviousSnapshot(t *testing.T) {
	coord, m := newTestCoordinator(t, testDeps{})
	start := time.Now()
	coord.lastReportTime = start

	coord.initWorker(0)
	coord.updateWorkerStatus(0, func(s *WorkerStatus) { s.ItemsWritten = 100 })
	m.RecordBytes(1024 * 1024)
	coord.snapshot(start.Add(time.Second))

	coord.updateWorkerStatus(0, func(s *WorkerStatus) { s.ItemsWritten = 150 })
	m.RecordBytes(3 * 1024 * 1024)
	snap := coord.snapshot(start.Add(2 * time.Second))

	if snap.ItemsPerSec != 50 {
		t.Errorf("expected 50 items/sec over the second interval, got %f", snap.ItemsPerSec)
	}
	if snap.MBPerSec != 3 {
		t.Errorf("expected 3 MB/sec over the second interval, got %f", snap.MBPerSec)
	}
}

// TestSnapshotReportsPercentOfExpectedItems verifies progress is expressed against the
// manifest's item count and never exceeds 100%, since the count is an estimate for
// incremental exports.
func TestSnapshotReportsPercentOfExpectedItems(t *testing.T) {
	tests := []struct {
		name     string
		expected int64
		written  int64
		want     float64
	}{
		{name: "half way", expected: 200, written: 100, want: 50},
		{name: "a single expected item counts", expected: 1, written: 1, want: 100},
		{name: "capped just past the end", expected: 1000, written: 1005, want: 100},
		{name: "capped when far more items arrive than expected", expected: 100, written: 250, want: 100},
		{name: "unknown total reports zero", expected: 0, written: 100, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coord, _ := newTestCoordinator(t, testDeps{})
			coord.totalExpectedItems = tt.expected
			coord.lastReportTime = time.Now()
			coord.initWorker(0)
			coord.updateWorkerStatus(0, func(s *WorkerStatus) { s.ItemsWritten = tt.written })

			if got := coord.snapshot(time.Now()).Percent; got != tt.want {
				t.Errorf("percent = %f, want %f", got, tt.want)
			}
		})
	}
}

// TestSnapshotCountsOnlyRecentlyActiveWorkers verifies a worker that has been quiet for
// the idle timeout or longer drops out of the active count, which is how a stalled
// restore becomes visible on the progress line.
func TestSnapshotCountsOnlyRecentlyActiveWorkers(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{})
	now := time.Now()
	coord.lastReportTime = now

	// One worker just acted, one has been quiet for exactly the timeout, one for twice it.
	idle := []time.Duration{time.Second, 10 * time.Second, 20 * time.Second}
	for id, quiet := range idle {
		coord.initWorker(id)
		coord.workerStatus[id].LastActive = now.Add(-quiet)
	}

	if got := coord.snapshot(now).ActiveWorkers; got != 1 {
		t.Errorf("expected 1 active worker, got %d", got)
	}
}

// TestSnapshotSumsWorkerBatches verifies batch counts are summed across all workers, so
// the reported total reflects the whole pool rather than one worker.
func TestSnapshotSumsWorkerBatches(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{})
	coord.lastReportTime = time.Now()

	coord.initWorker(0)
	coord.initWorker(1)
	coord.updateWorkerStatus(0, func(s *WorkerStatus) { s.BatchesCount = 3 })
	coord.updateWorkerStatus(1, func(s *WorkerStatus) { s.BatchesCount = 4 })

	if got := coord.snapshot(time.Now()).TotalBatches; got != 7 {
		t.Errorf("expected 7 batches, got %d", got)
	}
}

// testDeps describes the dependencies a test wants; anything left unset gets a
// permissive default so each test only states what it is about.
type testDeps struct {
	loader    *mockLoader
	streamer  *mockStreamer
	decoder   *mockDecoder
	writer    *mockWriter
	store     *mockStore
	uploader  *mockUploader
	configure func(*config.Config)
	files     []manifest.FileMeta
	lines     [][]byte
}

// newTestCoordinator wires a coordinator over the given dependencies and returns it
// together with the metrics it reports to.
func newTestCoordinator(t *testing.T, deps testDeps) (*Coordinator, *metrics.Metrics) {
	t.Helper()

	loader := deps.loader
	if loader == nil {
		var itemCount int64
		for _, f := range deps.files {
			itemCount += f.ItemCount
		}
		loader = &mockLoader{summary: manifest.Summary{
			S3Bucket:  "test-bucket",
			ItemCount: itemCount,
			DataFiles: deps.files,
		}}
	}
	streamer := deps.streamer
	if streamer == nil {
		streamer = &mockStreamer{lines: deps.lines}
	}
	decoder := deps.decoder
	if decoder == nil {
		decoder = &mockDecoder{}
	}
	w := deps.writer
	if w == nil {
		w = &mockWriter{}
	}
	store := deps.store
	if store == nil {
		store = &mockStore{}
	}

	cfg := &config.Config{
		TableName:       "test-table",
		ExportS3URI:     "s3://test-bucket/test-prefix",
		ExportType:      "FULL",
		ViewType:        "NEW",
		Region:          "us-west-2",
		MaxWorkers:      1,
		BatchSize:       25,
		ShutdownTimeout: time.Second,
	}
	if deps.configure != nil {
		deps.configure(cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("failed to validate config: %v", err)
	}

	m := metrics.NewMetrics()
	// A nil *mockUploader must not reach the coordinator as a non-nil interface.
	var uploader ReportUploader
	if deps.uploader != nil {
		uploader = deps.uploader
	}

	return NewCoordinator(cfg, loader, streamer, decoder, w, store, uploader, m), m
}

// runCoordinator runs the coordinator under a deadline so a stuck run fails the test
// instead of hanging the package.
func runCoordinator(t *testing.T, coord *Coordinator) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return coord.Run(ctx)
}

type mockLoader struct {
	err     error
	summary manifest.Summary
}

func (m *mockLoader) Load(ctx context.Context, manifestS3URI string) (manifest.Summary, error) {
	if m.err != nil {
		return manifest.Summary{}, m.err
	}
	return m.summary, nil
}

func (m *mockLoader) VerifyChecksums(ctx context.Context, summary manifest.Summary) error {
	return nil
}

// streamRequest records what a worker asked the streamer for.
type streamRequest struct {
	key    string
	offset int64
}

// mockStreamer replays fixed lines and can fail the first attempts, so retry behaviour
// can be driven. Once errs is exhausted its last entry repeats.
type mockStreamer struct {
	lines    [][]byte
	errs     []error
	requests []streamRequest
	mu       sync.Mutex
}

func (m *mockStreamer) Stream(ctx context.Context, bucket, key string, offset int64, fn func([]byte, int64) error) error {
	m.mu.Lock()
	attempt := len(m.requests)
	m.requests = append(m.requests, streamRequest{key: key, offset: offset})
	m.mu.Unlock()

	if len(m.errs) > 0 {
		err := m.errs[len(m.errs)-1]
		if attempt < len(m.errs) {
			err = m.errs[attempt]
		}
		if err != nil {
			return err
		}
	}

	for i, line := range m.lines {
		if err := fn(line, int64(i)); err != nil {
			return err
		}
	}
	return nil
}

// mockDecoder turns every line into the same put operation, except lines it is told to
// treat as corrupt.
type mockDecoder struct {
	corruptLines map[string]bool
}

func (m *mockDecoder) Decode(line []byte) (itemimage.Operation, error) {
	if m.corruptLines[string(line)] {
		return itemimage.Operation{}, itemimage.ErrCorrupt
	}
	return itemimage.Operation{
		Type: itemimage.OpPut,
		Keys: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: "123"},
		},
		NewImage: map[string]types.AttributeValue{
			"id":   &types.AttributeValueMemberS{Value: "123"},
			"name": &types.AttributeValueMemberS{Value: "test"},
		},
	}, nil
}

type mockWriter struct {
	err     error
	batches [][]itemimage.Operation
	mu      sync.Mutex
}

func (m *mockWriter) WriteBatch(ctx context.Context, ops []itemimage.Operation) error {
	if m.err != nil {
		return m.err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// The coordinator reuses its batch slice, so keep a copy.
	m.batches = append(m.batches, append([]itemimage.Operation(nil), ops...))
	return nil
}

func (m *mockWriter) Flush(ctx context.Context) error {
	return nil
}

type mockStore struct {
	loadErr error
	saveErr error
	state   checkpoint.State
	saved   []checkpoint.State
	mu      sync.Mutex
}

func (m *mockStore) Load(ctx context.Context) (checkpoint.State, error) {
	if m.loadErr != nil {
		return checkpoint.State{}, m.loadErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state, nil
}

func (m *mockStore) Save(ctx context.Context, s checkpoint.State) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = s
	m.saved = append(m.saved, s)
	return nil
}

func (m *mockStore) lastSaved() checkpoint.State {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.saved) == 0 {
		return checkpoint.State{}
	}
	return m.saved[len(m.saved)-1]
}

func (m *mockStore) savedOffsets() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	offsets := make([]int64, 0, len(m.saved))
	for _, s := range m.saved {
		offsets = append(offsets, s.LastByteOffset)
	}
	return offsets
}

type mockUploader struct {
	err  error
	uris []string
}

func (m *mockUploader) UploadReport(ctx context.Context, uri string, report metrics.Report) error {
	m.uris = append(m.uris, uri)
	return m.err
}
