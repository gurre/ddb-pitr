package coordinator

import (
	"context"
	"errors"
	"fmt"
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

// testReportURI is where tests ask for the final report to be uploaded, and
// testExportARN identifies the export their manifests describe.
const (
	testReportURI = "s3://reports/restore-001.json"
	testExportARN = "arn:aws:dynamodb:eu-north-1:123456789012:table/orders/export/01768385930622-efd1a093"
	testFileKey   = "file1"
	testFileKey2  = "file2"
)

// TestCoordinatorHappyPath verifies a single-file export is streamed, decoded and
// written in one batch, which is the baseline every other behaviour builds on.
func TestCoordinatorHappyPath(t *testing.T) {
	writer := &mockWriter{}
	coord, _ := newTestCoordinator(t, testDeps{
		files:  []manifest.FileMeta{{Key: testFileKey, ItemCount: 2}},
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
		files:  []manifest.FileMeta{{Key: testFileKey, ItemCount: 5}},
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
			{Key: testFileKey, ItemCount: 1},
			{Key: testFileKey2, ItemCount: 1},
		},
		streamer: streamer,
		store:    &mockStore{state: checkpoint.State{Completed: []string{testFileKey}}},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	var keys []string
	for _, req := range streamer.requests {
		keys = append(keys, req.key)
	}
	if len(keys) != 1 || keys[0] != testFileKey2 {
		t.Errorf("expected only file2 to be streamed, got %v", keys)
	}
}

// TestCoordinatorCheckpointsAtInterval verifies progress is written every
// checkpointInterval batches at the offset reached, then again for the trailing partial
// batch, and finally as a completion. Checkpointing too rarely loses work on an
// interrupted restore; too often turns S3 into the bottleneck.
func TestCoordinatorCheckpointsAtInterval(t *testing.T) {
	// Two lines per batch, so batch N completes at line offset 2N-1. With 401 lines the
	// 100th and 200th batches land on offsets 199 and 399, and line 400 trails behind.
	lines := make([][]byte, 401)
	for i := range lines {
		lines[i] = []byte(`{"id":"1"}`)
	}
	store := &mockStore{}
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{{Key: testFileKey, ItemCount: int64(len(lines))}},
		lines: lines,
		store: store,
		configure: func(cfg *config.Config) {
			cfg.BatchSize = 2
		},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	want := []int64{199, 399, 400}
	got := store.savedOffsets(testFileKey)
	if len(got) != len(want) {
		t.Fatalf("checkpoint offsets = %v, want %v then a completion", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("checkpoint offsets = %v, want %v then a completion", got, want)
		}
	}
	if final := store.lastSaved(); len(final.Completed) != 1 || final.Completed[0] != testFileKey {
		t.Errorf("expected a final completion checkpoint for file1, got %+v", final)
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
		files:  []manifest.FileMeta{{Key: testFileKey, ItemCount: 4}},
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
	if got := store.savedOffsets(testFileKey); len(got) != 0 {
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
			{Key: testFileKey, ItemCount: 1},
			{Key: testFileKey2, ItemCount: 1},
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
		files: []manifest.FileMeta{{Key: testFileKey, ItemCount: 6}},
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
		files:    []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
		streamer: streamer,
		store:    &mockStore{state: checkpoint.State{Offsets: map[string]int64{testFileKey: 4096}}},
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
		files:    []manifest.FileMeta{{Key: testFileKey2, ItemCount: 1}},
		streamer: streamer,
		store:    &mockStore{state: checkpoint.State{Offsets: map[string]int64{testFileKey: 4096}}},
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

// TestCoordinatorSkipsOnlyTheFilesRecordedComplete verifies a resume re-streams every
// file the checkpoint does not list as finished, whatever their order.
//
// Workers process different files at once, so the file that finished most recently says
// nothing about the ones ordered before it. A resume that inferred "everything earlier
// is done" would silently abandon whatever the other workers had not got through, and
// still report the restore as complete.
func TestCoordinatorSkipsOnlyTheFilesRecordedComplete(t *testing.T) {
	streamer := &mockStreamer{lines: [][]byte{[]byte(`{"id":"1"}`)}}
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{
			{Key: testFileKey, ItemCount: 1},
			{Key: testFileKey2, ItemCount: 1},
			{Key: "file3", ItemCount: 1},
			{Key: "file4", ItemCount: 1},
		},
		streamer: streamer,
		// file3 finished while file1 and file2 were still in flight.
		store: &mockStore{state: checkpoint.State{Completed: []string{"file3"}}},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	var keys []string
	for _, req := range streamer.requests {
		keys = append(keys, req.key)
	}
	if len(keys) != 3 || keys[0] != testFileKey || keys[1] != testFileKey2 || keys[2] != "file4" {
		t.Errorf("expected file1, file2 and file4 to be streamed, got %v", keys)
	}
}

// TestCoordinatorMarksFileComplete verifies a fully processed file is recorded as
// finished and stops carrying an offset, so a later run can tell "finished" from
// "start at zero" without a sentinel value to interpret.
func TestCoordinatorMarksFileComplete(t *testing.T) {
	store := &mockStore{}
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
		lines: [][]byte{[]byte(`{"id":"1"}`)},
		store: store,
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	final := store.lastSaved()
	if len(final.Completed) != 1 || final.Completed[0] != testFileKey {
		t.Errorf("expected file1 recorded as complete, got %v", final.Completed)
	}
	if _, ok := final.Offsets[testFileKey]; ok {
		t.Errorf("expected a completed file to carry no offset, got %v", final.Offsets)
	}
}

// TestCoordinatorRecordsEveryWorkersProgress verifies a checkpoint carries what the
// whole pool has done, not just the worker that happened to take it. One worker's save
// overwriting another's is how a resume comes to skip files nobody finished.
func TestCoordinatorRecordsEveryWorkersProgress(t *testing.T) {
	files := make([]manifest.FileMeta, 6)
	for i := range files {
		files[i] = manifest.FileMeta{Key: fmt.Sprintf("file%d", i), ItemCount: 1}
	}
	store := &mockStore{}
	coord, _ := newTestCoordinator(t, testDeps{
		files: files,
		lines: [][]byte{[]byte(`{"id":"1"}`)},
		store: store,
		configure: func(cfg *config.Config) {
			cfg.MaxWorkers = 3
		},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	final := store.lastSaved()
	if len(final.Completed) != len(files) {
		t.Fatalf("checkpoint records %d completed files, want %d: %v",
			len(final.Completed), len(files), final.Completed)
	}
}

// TestCoordinatorRejectsCheckpointFromAnotherExport verifies progress recorded against a
// different export is refused. File keys repeat across exports, so resuming from the
// wrong one would skip files by name collision and still report success.
func TestCoordinatorRejectsCheckpointFromAnotherExport(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
		lines: [][]byte{[]byte(`{"id":"1"}`)},
		store: &mockStore{state: checkpoint.State{
			ExportID:  "arn:aws:dynamodb:eu-north-1:123456789012:table/orders/export/other",
			Completed: []string{testFileKey},
		}},
	})

	if err := runCoordinator(t, coord); err == nil {
		t.Fatal("expected a checkpoint from another export to be refused")
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
		files:    []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
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

// TestCoordinatorBacksOffFurtherOnEachStreamRetry verifies the first attempt at a file
// is not paced and every attempt after it waits longer than the last. Retrying a
// struggling S3 immediately, or at a fixed interval, keeps it struggling.
func TestCoordinatorBacksOffFurtherOnEachStreamRetry(t *testing.T) {
	backoff := &instantBackoff{}
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
		streamer: &mockStreamer{
			lines: [][]byte{[]byte(`{"id":"1"}`)},
			errs:  []error{errors.New("connection reset"), errors.New("connection reset"), nil},
		},
		backoff: backoff,
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	got := backoff.recorded()
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("expected waits for attempts 1 and 2 and none before the first, got %v", got)
	}
}

// TestCoordinatorSurrendersFileWhenBackoffStops verifies a file whose retry wait is cut
// short fails the restore rather than falling through.
//
// Falling through would record the file as complete, and a resume would then skip a file
// that was never finished. The wait is only cut short when the restore is shutting down,
// which is exactly when a checkpoint must not overstate what was done.
func TestCoordinatorSurrendersFileWhenBackoffStops(t *testing.T) {
	store := &mockStore{}
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
		streamer: &mockStreamer{
			lines: [][]byte{[]byte(`{"id":"1"}`)},
			errs:  []error{errors.New("connection reset")},
		},
		store:   store,
		backoff: &stoppedBackoff{},
	})

	if err := runCoordinator(t, coord); err == nil {
		t.Fatal("expected a run whose retry wait was cut short to fail")
	}
	if final := store.lastSaved(); len(final.Completed) != 0 {
		t.Errorf("expected no file recorded as complete, got %v", final.Completed)
	}
}

// TestCoordinatorReportsCancellationWhenInterruptedMidRetry verifies a restore stopped
// while it is retrying a file reports the cancellation itself, rather than a generic
// failure. An operator who pressed Ctrl-C needs to see that the run stopped because they
// asked, not go looking for a fault in the export.
func TestCoordinatorReportsCancellationWhenInterruptedMidRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(callerContext())
	defer cancel()

	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
		streamer: &mockStreamer{
			lines: [][]byte{[]byte(`{"id":"1"}`)},
			errs:  []error{errors.New("connection reset")},
			// The interrupt lands while the file is between attempts.
			onAttempt: cancel,
		},
	})

	if err := coord.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("expected the run to report cancellation, got %v", err)
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
		files:    []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
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
		files:  []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
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
		files: []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
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
		files: []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
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
		files: []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
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
		files:   []manifest.FileMeta{{Key: testFileKey, ItemCount: 2}},
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
		files:    []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
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
		files:    []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
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
		files:    []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
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

// TestCoordinatorReportsWriteFailureRatherThanHanging verifies a restore whose workers
// have all given up ends with their error, even with files still waiting to be handed
// out. Handing a file to a pool that has exited blocks on a channel nobody is reading,
// which would leave the process alive and silent instead of failing.
func TestCoordinatorReportsWriteFailureRatherThanHanging(t *testing.T) {
	files := make([]manifest.FileMeta, 20)
	for i := range files {
		files[i] = manifest.FileMeta{Key: fmt.Sprintf("file%02d", i), ItemCount: 1}
	}
	coord, _ := newTestCoordinator(t, testDeps{
		files:  files,
		lines:  [][]byte{[]byte(`{"id":"1"}`)},
		writer: &mockWriter{err: errors.New("table not found")},
		configure: func(cfg *config.Config) {
			cfg.MaxWorkers = 1
		},
	})

	done := make(chan error, 1)
	go func() { done <- runCoordinator(t, coord) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected the write failure reported")
		}
		if !strings.Contains(err.Error(), "table not found") {
			t.Errorf("expected the write failure surfaced, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("dispatching to an exited worker pool never returned")
	}
}

// TestCoordinatorDiscardsBufferedItemsOnStreamRetry verifies a file whose stream fails
// part-way is retried from the checkpointed offset with nothing carried over from the
// failed attempt. Items already buffered would otherwise be re-read on the retry and
// written twice, inflating the item count and duplicating work.
func TestCoordinatorDiscardsBufferedItemsOnStreamRetry(t *testing.T) {
	// Four lines, and the first attempt dies after the third. The batch size is larger
	// than the file, so everything sits in the buffer when the failure lands.
	streamer := &mockStreamer{
		lines:     [][]byte{[]byte(`{"id":"1"}`), []byte(`{"id":"2"}`), []byte(`{"id":"3"}`), []byte(`{"id":"4"}`)},
		failAfter: 3,
	}
	writer := &mockWriter{}
	coord, _ := newTestCoordinator(t, testDeps{
		files:    []manifest.FileMeta{{Key: testFileKey, ItemCount: 4}},
		streamer: streamer,
		writer:   writer,
		configure: func(cfg *config.Config) {
			cfg.BatchSize = 10
		},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	if len(streamer.requests) != 2 {
		t.Fatalf("expected the stream to be retried once, got %d requests", len(streamer.requests))
	}
	var written int
	for _, batch := range writer.batches {
		written += len(batch)
	}
	if written != 4 {
		t.Errorf("expected the file's 4 items written once, got %d", written)
	}
}

// TestCoordinatorFailsWhenExportDoesNotVerify verifies a manifest whose data files no
// longer match stops the restore before anything is written. Catching it afterwards
// would mean the table already holds data the export never contained.
func TestCoordinatorFailsWhenExportDoesNotVerify(t *testing.T) {
	writer := &mockWriter{}
	coord, _ := newTestCoordinator(t, testDeps{
		loader: &mockLoader{
			summary:   manifest.Summary{ExportARN: testExportARN, DataFiles: []manifest.FileMeta{{Key: testFileKey}}},
			verifyErr: errors.New("checksum mismatch for data file file1"),
		},
		writer: writer,
	})

	err := runCoordinator(t, coord)
	if err == nil {
		t.Fatal("expected an export that fails verification to stop the restore")
	}
	if len(writer.batches) != 0 {
		t.Errorf("expected nothing written before verification, got %d batches", len(writer.batches))
	}
}

// TestProgressLineLabelsEveryCount verifies the progress line puts each number against
// its own label. The counts drive different responses from an operator, so reading
// throttles as retries or lost items as errors sends them the wrong way.
func TestProgressLineLabelsEveryCount(t *testing.T) {
	// Every value is distinct so a transposed pair cannot look correct.
	snap := progressSnapshot{
		Percent:       12.5,
		ItemsPerSec:   345,
		MBPerSec:      6.75,
		TotalBatches:  81,
		ActiveWorkers: 9,
		Throttles:     11,
		Retries:       22,
		LostItems:     33,
		Errors:        44,
	}

	const want = "Progress: 12.5% (345/s, 6.8 MB/s) | 81 batches | 9 workers | " +
		"11 throttles | 22 retries | 33 lost | 44 errors"
	if got := snap.String(); got != want {
		t.Errorf("progress line = %q, want %q", got, want)
	}
}

// TestWorkerStatusIsSafeUnderConcurrentUpdates verifies the pool's shared status map is
// read and written under a lock. Every worker writes to it while the single progress
// reporter reads it once a second, so unsynchronised access here is a live data race in
// every restore, which this test exposes under -race.
func TestWorkerStatusIsSafeUnderConcurrentUpdates(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{})
	coord.lastReportTime = time.Now()

	var wg sync.WaitGroup

	// One reader, as reportProgress is the only caller of snapshot.
	reading := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-reading:
				return
			default:
				coord.snapshot(time.Now())
			}
		}
	}()

	for id := 0; id < 4; id++ {
		wg.Add(2)
		go func(id int) { defer wg.Done(); coord.initWorker(id) }(id)
		go func(id int) {
			defer wg.Done()
			coord.updateWorkerStatus(id, func(s *WorkerStatus) { s.ItemsWritten++ })
		}(id)
	}

	close(reading)
	wg.Wait()
}

// TestProgressRoundTripsThroughTheCheckpoint verifies the progress a run recorded is
// what a later run reads back, export identity included. The identity is what stops one
// export resuming from another's progress, so a snapshot that dropped it would let that
// through unnoticed.
func TestProgressRoundTripsThroughTheCheckpoint(t *testing.T) {
	p := newProgress(testExportARN, checkpoint.State{
		Completed: []string{"file1"},
		Offsets:   map[string]int64{testFileKey2: 4096},
	})
	p.record("file3", 512)
	p.complete(testFileKey2)

	got := p.snapshot()

	if got.ExportID != testExportARN {
		t.Errorf("ExportID = %q, want %q", got.ExportID, testExportARN)
	}
	// Sorted, so two checkpoints of the same progress are byte-identical.
	if len(got.Completed) != 2 || got.Completed[0] != "file1" || got.Completed[1] != testFileKey2 {
		t.Errorf("Completed = %v, want file1 and file2 in order", got.Completed)
	}
	if len(got.Offsets) != 1 || got.Offsets["file3"] != 512 {
		t.Errorf("Offsets = %v, want only file3 at 512", got.Offsets)
	}
}

// TestProgressResumeReportsWhereToRestart verifies a file's recorded offset is returned
// for that file only, and that a finished file is reported finished rather than as
// starting at zero. Confusing the two either re-reads a whole file or skips one.
func TestProgressResumeReportsWhereToRestart(t *testing.T) {
	p := newProgress(testExportARN, checkpoint.State{
		Completed: []string{"file1"},
		Offsets:   map[string]int64{testFileKey2: 4096},
	})

	tests := []struct {
		key        string
		wantOffset int64
		wantDone   bool
	}{
		{key: "file1", wantOffset: 0, wantDone: true},
		{key: testFileKey2, wantOffset: 4096, wantDone: false},
		{key: "file3", wantOffset: 0, wantDone: false},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			offset, done := p.resume(tt.key)
			if offset != tt.wantOffset || done != tt.wantDone {
				t.Errorf("resume(%q) = (%d, %v), want (%d, %v)",
					tt.key, offset, done, tt.wantOffset, tt.wantDone)
			}
		})
	}
}

// TestProgressIsSafeUnderConcurrentWorkers verifies the shared progress record is guarded.
// Every worker reports into it while checkpoints snapshot it, so unsynchronised access is
// a live data race in every multi-worker restore, which -race exposes here.
func TestProgressIsSafeUnderConcurrentWorkers(t *testing.T) {
	p := newProgress(testExportARN, checkpoint.State{})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(4)
		key := fmt.Sprintf("file%d", i)
		go func() { defer wg.Done(); p.record(key, 100) }()
		go func() { defer wg.Done(); p.resume(key) }()
		go func() { defer wg.Done(); p.complete(key) }()
		go func() { defer wg.Done(); p.snapshot() }()
	}
	wg.Wait()
}

// TestCheckpointMismatchNamesBothExports verifies the refusal says which export the
// checkpoint belongs to and which one was asked for. An operator who reused a resume key
// can only fix it if the message distinguishes the two.
func TestCheckpointMismatchNamesBothExports(t *testing.T) {
	const otherARN = "arn:aws:dynamodb:eu-north-1:123456789012:table/orders/export/other"
	coord, _ := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
		lines: [][]byte{[]byte(`{"id":"1"}`)},
		store: &mockStore{state: checkpoint.State{ExportID: otherARN}},
	})

	err := runCoordinator(t, coord)
	if err == nil {
		t.Fatal("expected a checkpoint from another export to be refused")
	}
	// The checkpoint's export comes first, then the one being restored; transposing
	// them tells the operator to fix the wrong side.
	want := fmt.Sprintf("progress for export %s, not %s", otherARN, testExportARN)
	if !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not contain %q", err, want)
	}
}

// TestSnapshotCarriesEveryFailureCount verifies the throttle, retry, lost and error
// counters all reach the progress line. Each one calls for a different response, so a
// counter stuck at zero tells the operator a restore is healthier than it is.
func TestSnapshotCarriesEveryFailureCount(t *testing.T) {
	coord, m := newTestCoordinator(t, testDeps{})
	coord.lastReportTime = time.Now()

	// Distinct counts, so a field taking another's value cannot look correct.
	for i := 0; i < 2; i++ {
		m.RecordThrottle()
	}
	for i := 0; i < 3; i++ {
		m.RecordRetry()
	}
	for i := 0; i < 4; i++ {
		m.RecordError()
	}
	m.RecordLost(5)

	snap := coord.snapshot(time.Now())

	for _, tt := range []struct {
		name string
		got  int64
		want int64
	}{
		{"Throttles", snap.Throttles, 2},
		{"Retries", snap.Retries, 3},
		{"Errors", snap.Errors, 4},
		{"LostItems", snap.LostItems, 5},
	} {
		if tt.got != tt.want {
			t.Errorf("snapshot.%s = %d, want %d", tt.name, tt.got, tt.want)
		}
	}
}

// TestCoordinatorCountsWhatItWrote verifies the restore's own counters reach the report.
// The report is the durable record of the run, so an item or batch that goes uncounted
// tells an operator less was restored than actually was.
func TestCoordinatorCountsWhatItWrote(t *testing.T) {
	lines := make([][]byte, 6)
	for i := range lines {
		lines[i] = []byte(`{"id":"1"}`)
	}
	coord, m := newTestCoordinator(t, testDeps{
		files: []manifest.FileMeta{{Key: testFileKey, ItemCount: 6}},
		lines: lines,
		configure: func(cfg *config.Config) {
			cfg.BatchSize = 2
		},
	})

	if err := runCoordinator(t, coord); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	report := m.GenerateReport()
	if report.TotalItems != 6 {
		t.Errorf("TotalItems = %d, want 6", report.TotalItems)
	}
	if report.BatchesWritten != 3 {
		t.Errorf("BatchesWritten = %d, want 3", report.BatchesWritten)
	}
	if report.ProcessingTime <= 0 {
		t.Errorf("ProcessingTime = %v, want the time spent writing", report.ProcessingTime)
	}
}

// TestCoordinatorAttributesErrorsToTheWorker verifies a failure is both counted and
// recorded against the worker that hit it. The worker's last error is what an operator
// reads to tell one struggling file from a restore failing everywhere.
func TestCoordinatorAttributesErrorsToTheWorker(t *testing.T) {
	coord, m := newTestCoordinator(t, testDeps{
		files:  []manifest.FileMeta{{Key: testFileKey, ItemCount: 1}},
		lines:  [][]byte{[]byte(`{"id":"1"}`)},
		writer: &mockWriter{err: errors.New("table not found")},
	})

	if err := runCoordinator(t, coord); err == nil {
		t.Fatal("expected the write failure reported")
	}

	if m.Errors() == 0 {
		t.Error("expected the write failure counted as an error")
	}

	coord.statusMu.RLock()
	defer coord.statusMu.RUnlock()
	var recorded int
	for _, status := range coord.workerStatus {
		if status.LastError != nil {
			recorded++
			if status.LastErrorTime.IsZero() {
				t.Error("a recorded error carries no time")
			}
		}
	}
	if recorded != 1 {
		t.Errorf("expected the error recorded against 1 worker, got %d", recorded)
	}
}

// TestInitWorkerRecordsTheWorkersOwnID verifies each worker's status is filed under, and
// carries, its own identifier. The progress line and error reports name workers by it,
// so a status that lost its ID would attribute a stall to the wrong worker.
func TestInitWorkerRecordsTheWorkersOwnID(t *testing.T) {
	coord, _ := newTestCoordinator(t, testDeps{})

	coord.initWorker(0)
	coord.initWorker(7)

	coord.statusMu.RLock()
	defer coord.statusMu.RUnlock()
	for id, status := range coord.workerStatus {
		if status.ID != id {
			t.Errorf("worker filed under %d reports ID %d", id, status.ID)
		}
		if status.StartTime.IsZero() {
			t.Errorf("worker %d has no start time", id)
		}
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
	backoff   Backoffer
	configure func(*config.Config)
	files     []manifest.FileMeta
	lines     [][]byte
}

// instantBackoff removes the real waiting from retry tests while still honouring
// cancellation, which is the only property of the wait the retry loop depends on. It
// records the attempt numbers it was given so tests can assert the loop keeps counting
// up, and therefore keeps backing off further.
type instantBackoff struct {
	attempts []int
	mu       sync.Mutex
}

func (b *instantBackoff) Wait(ctx context.Context, attempt int) bool {
	b.mu.Lock()
	b.attempts = append(b.attempts, attempt)
	b.mu.Unlock()
	return ctx.Err() == nil
}

func (b *instantBackoff) recorded() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int(nil), b.attempts...)
}

// stoppedBackoff stands for a wait cut short before the delay elapsed.
type stoppedBackoff struct{}

func (b *stoppedBackoff) Wait(ctx context.Context, attempt int) bool { return false }

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
			ExportARN: testExportARN,
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
	backoff := deps.backoff
	if backoff == nil {
		backoff = &instantBackoff{}
	}

	return NewCoordinator(cfg, loader, streamer, decoder, w, store, uploader, m,
		WithStreamBackoff(backoff)), m
}

// runCoordinator runs the coordinator under a deadline so a stuck run fails the test
// instead of hanging the package. The context carries a marker every dependency checks
// for, so a call the coordinator made under some other context fails rather than passing
// unnoticed.
func runCoordinator(t *testing.T, coord *Coordinator) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(callerContext(), 30*time.Second)
	defer cancel()

	return coord.Run(ctx)
}

// callerContext is a background context carrying the marker the mocks look for.
func callerContext() context.Context {
	return context.WithValue(context.Background(), callerContextKey{}, true)
}

type mockLoader struct {
	err       error
	verifyErr error
	summary   manifest.Summary
}

func (m *mockLoader) Load(ctx context.Context, manifestS3URI string) (manifest.Summary, error) {
	if m.err != nil {
		return manifest.Summary{}, m.err
	}
	if err := requireCallerContext(ctx); err != nil {
		return manifest.Summary{}, err
	}
	return m.summary, nil
}

func (m *mockLoader) VerifyChecksums(ctx context.Context, summary manifest.Summary) (manifest.Verification, error) {
	if m.verifyErr != nil {
		return manifest.Verification{}, m.verifyErr
	}
	if err := requireCallerContext(ctx); err != nil {
		return manifest.Verification{}, err
	}
	return manifest.Verification{Verified: len(summary.DataFiles)}, nil
}

// callerContextKey marks the context a test passed into Run, so a dependency can tell
// the caller's context from one the coordinator substituted for it.
type callerContextKey struct{}

// requireCallerContext fails a dependency call that did not arrive under the context
// the test handed to Run. Losing it would leave the call with no deadline and no
// cancellation, so a shutting-down restore would keep talking to AWS.
func requireCallerContext(ctx context.Context) error {
	if ctx.Value(callerContextKey{}) == nil {
		return errors.New("call made outside the caller's context")
	}
	return nil
}

// streamRequest records what a worker asked the streamer for.
type streamRequest struct {
	key    string
	offset int64
}

// mockStreamer replays fixed lines and can fail the first attempts, so retry behaviour
// can be driven. Once errs is exhausted its last entry repeats. failAfter instead fails
// the first attempt part-way through the file, which is what leaves a partial batch
// buffered behind a retry.
type mockStreamer struct {
	lines     [][]byte
	errs      []error
	requests  []streamRequest
	onAttempt func() // Runs at the start of every attempt; lets a test interrupt one
	failAfter int    // Lines the first attempt delivers before failing; 0 disables
	mu        sync.Mutex
}

func (m *mockStreamer) Stream(ctx context.Context, bucket, key string, offset int64, fn func([]byte, int64) error) error {
	if err := requireCallerContext(ctx); err != nil {
		return err
	}

	m.mu.Lock()
	attempt := len(m.requests)
	m.requests = append(m.requests, streamRequest{key: key, offset: offset})
	onAttempt := m.onAttempt
	m.mu.Unlock()

	if onAttempt != nil {
		onAttempt()
	}

	if len(m.errs) > 0 {
		err := m.errs[len(m.errs)-1]
		if attempt < len(m.errs) {
			err = m.errs[attempt]
		}
		if err != nil {
			return err
		}
	}

	limit := len(m.lines)
	if m.failAfter > 0 && attempt == 0 {
		limit = m.failAfter
	}
	for i := 0; i < limit; i++ {
		if err := fn(m.lines[i], int64(i)); err != nil {
			return err
		}
	}
	if limit < len(m.lines) {
		return errors.New("connection reset")
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
	if err := requireCallerContext(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// The coordinator reuses its batch slice, so keep a copy.
	m.batches = append(m.batches, append([]itemimage.Operation(nil), ops...))
	return nil
}

func (m *mockWriter) Flush(ctx context.Context) error {
	return requireCallerContext(ctx)
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
	if err := requireCallerContext(ctx); err != nil {
		return checkpoint.State{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state, nil
}

func (m *mockStore) Save(ctx context.Context, s checkpoint.State) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	if err := requireCallerContext(ctx); err != nil {
		return err
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

// savedOffsets returns the offsets recorded for one file, in the order they were saved.
// A checkpoint that no longer carries the file has finished it and contributes nothing.
func (m *mockStore) savedOffsets(key string) []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	offsets := make([]int64, 0, len(m.saved))
	for _, s := range m.saved {
		if offset, ok := s.Offsets[key]; ok {
			offsets = append(offsets, offset)
		}
	}
	return offsets
}

type mockUploader struct {
	err  error
	uris []string
	mu   sync.Mutex
}

func (m *mockUploader) UploadReport(ctx context.Context, uri string, report metrics.Report) error {
	if err := requireCallerContext(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uris = append(m.uris, uri)
	return m.err
}
