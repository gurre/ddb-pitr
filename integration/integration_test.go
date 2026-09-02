package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gurre/ddb-pitr/checkpoint"
	"github.com/gurre/ddb-pitr/config"
	"github.com/gurre/ddb-pitr/coordinator"
	"github.com/gurre/ddb-pitr/integration/mock"
	"github.com/gurre/ddb-pitr/itemimage"
	"github.com/gurre/ddb-pitr/manifest"
	"github.com/gurre/ddb-pitr/metrics"
	"github.com/gurre/ddb-pitr/writer"
	"github.com/gurre/s3streamer"
)

func TestFullIntegrationFlow(t *testing.T) {
	testDataDir, err := filepath.Abs("../s3exportdata")
	if err != nil {
		t.Fatalf("Failed to get absolute path to s3exportdata: %v", err)
	}

	mockS3 := mock.NewS3Client(testDataDir)
	if err := mockS3.LoadTestFiles(); err != nil {
		t.Fatalf("Failed to load test files: %v", err)
	}

	cfg := &config.Config{
		TableName:       "test-table",
		ExportS3URI:     "s3://test-bucket/AWSDynamoDB/01768385930622-efd1a093/manifest-summary.json",
		ExportType:      "FULL",
		ViewType:        "NEW",
		Region:          "us-west-2",
		MaxWorkers:      1,
		BatchSize:       25,
		ShutdownTimeout: 5 * time.Second,
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Invalid config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	manifestLoader := manifest.NewS3Loader(mockS3)
	manifestSummary, err := manifestLoader.Load(ctx, cfg.ExportS3URI)
	if err != nil {
		t.Fatalf("Failed to load manifest: %v", err)
	}

	t.Logf("Loaded manifest with %d data files", len(manifestSummary.DataFiles))
	if len(manifestSummary.DataFiles) == 0 {
		t.Fatalf("No data files found in manifest")
	}

	// Use real JSONDecoder to validate testdata content
	decoder := itemimage.NewJSONDecoder()
	streamer := s3streamer.NewS3Streamer(mockS3)

	totalItems := 0
	for i, file := range manifestSummary.DataFiles {
		t.Logf("Processing file %d: %s", i+1, file.Key)

		itemCount := 0
		err = streamer.Stream(ctx, mock.ExportBucket, file.Key, 0, func(line []byte, byteOffset int64) error {
			op, err := decoder.Decode(line)
			if err != nil {
				t.Errorf("Failed to decode line: %v", err)
				return nil
			}
			if op.Type != itemimage.OpPut {
				t.Errorf("Expected OpPut for FULL export, got %v", op.Type)
			}
			if op.NewImage == nil {
				t.Error("NewImage should not be nil for FULL export")
			}
			itemCount++
			return nil
		})

		if err != nil {
			t.Errorf("Error streaming data from file %s: %v", file.Key, err)
			continue
		}
		t.Logf("File %s: %d items", file.Key, itemCount)
		totalItems += itemCount
	}

	t.Logf("Total items processed: %d", totalItems)
	if totalItems != 3 {
		t.Errorf("Expected 3 items in FULL export, got %d", totalItems)
	}
}

// TestEndToEndWithCoordinator tests a full end-to-end flow using the Coordinator.
func TestEndToEndWithCoordinator(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running test in short mode")
	}

	testDataDir, err := filepath.Abs("../s3exportdata")
	if err != nil {
		t.Fatalf("Failed to get absolute path to s3exportdata: %v", err)
	}

	mockS3 := mock.NewS3Client(testDataDir)
	if err := mockS3.LoadTestFiles(); err != nil {
		t.Fatalf("Failed to load test files: %v", err)
	}

	mockDynamoDB := mock.NewDynamoDBClient()

	cfg := &config.Config{
		TableName:       "test-table",
		ExportS3URI:     "s3://test-bucket/AWSDynamoDB/01768385930622-efd1a093/manifest-summary.json",
		ExportType:      "FULL",
		ViewType:        "NEW",
		Region:          "us-west-2",
		MaxWorkers:      1,
		BatchSize:       25,
		ShutdownTimeout: 1 * time.Second,
		DryRun:          true,
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Invalid config: %v", err)
	}

	manifestLoader := manifest.NewS3Loader(mockS3)
	streamer := s3streamer.NewS3Streamer(mockS3)
	jsonDecoder := itemimage.NewJSONDecoder() // Use real decoder
	ddbWriter := writer.NewDynamoDBWriter(mockDynamoDB, cfg.TableName, cfg.BatchSize, writer.Callbacks{})
	checkpointStore := checkpoint.NewMemoryStore()

	coord := coordinator.NewCoordinator(
		cfg,
		manifestLoader,
		streamer,
		jsonDecoder,
		ddbWriter,
		checkpointStore,
		nil, // no report uploader in tests
		metrics.NewMetrics(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	doneCh := make(chan struct{})
	errCh := make(chan error, 1)

	go func() {
		err := coord.Run(ctx)
		if err != nil {
			errCh <- err
		}
		close(doneCh)
	}()

	select {
	case err := <-errCh:
		t.Fatalf("Coordinator run failed: %v", err)
	case <-doneCh:
		t.Log("Coordinator completed successfully")
	case <-time.After(8 * time.Second):
		t.Fatal("Test timed out waiting for coordinator to complete")
	}

	tableContents := mockDynamoDB.GetTableContents(cfg.TableName)
	t.Logf("Total items written to DynamoDB: %d", len(tableContents))

	batchWrites := mockDynamoDB.GetBatchWrites()
	t.Logf("Total batch writes: %d", len(batchWrites))

	// Verify 3 items were processed
	if len(tableContents) != 3 {
		t.Errorf("Expected 3 items written, got %d", len(tableContents))
	}
}

// TestIncrementalExportWithCoordinator tests incremental export with NEW_AND_OLD_IMAGES.
func TestIncrementalExportWithCoordinator(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running test in short mode")
	}

	testDataDir, err := filepath.Abs("../s3exportdata")
	if err != nil {
		t.Fatalf("Failed to get absolute path to s3exportdata: %v", err)
	}

	mockS3 := mock.NewS3Client(testDataDir)
	if err := mockS3.LoadTestFiles(); err != nil {
		t.Fatalf("Failed to load test files: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test the incremental export with 6 items
	manifestLoader := manifest.NewS3Loader(mockS3)
	exportURI := "s3://test-bucket/AWSDynamoDB/01768386924000-d339e52d/manifest-summary.json"

	manifestSummary, err := manifestLoader.Load(ctx, exportURI)
	if err != nil {
		t.Fatalf("Failed to load incremental manifest: %v", err)
	}

	// Verify it's an incremental export
	if manifestSummary.ExportType != "INCREMENTAL_EXPORT" {
		t.Errorf("Expected INCREMENTAL_EXPORT, got %s", manifestSummary.ExportType)
	}
	if manifestSummary.OutputView != "NEW_AND_OLD_IMAGES" {
		t.Errorf("Expected NEW_AND_OLD_IMAGES, got %s", manifestSummary.OutputView)
	}

	t.Logf("Loaded incremental manifest with %d data files", len(manifestSummary.DataFiles))

	// Use real decoder to parse incremental export data
	decoder := itemimage.NewJSONDecoder()
	streamer := s3streamer.NewS3Streamer(mockS3)

	var putCount, updateCount, deleteCount int

	for _, file := range manifestSummary.DataFiles {
		err = streamer.Stream(ctx, mock.ExportBucket, file.Key, 0, func(line []byte, byteOffset int64) error {
			op, err := decoder.Decode(line)
			if err != nil {
				// Some files may not exist in test data
				return nil
			}

			switch op.Type {
			case itemimage.OpPut:
				putCount++
			case itemimage.OpUpdate:
				updateCount++
				// Verify update has both old and new images
				if op.OldImage == nil || op.NewImage == nil {
					t.Error("Update operation should have both OldImage and NewImage")
				}
			case itemimage.OpDelete:
				deleteCount++
				if op.OldImage == nil {
					t.Error("Delete operation should have OldImage")
				}
			}
			return nil
		})
		if err != nil {
			t.Logf("Warning: Error streaming file %s: %v", file.Key, err)
		}
	}

	totalItems := putCount + updateCount + deleteCount
	t.Logf("Incremental export: %d puts, %d updates, %d deletes (total: %d)",
		putCount, updateCount, deleteCount, totalItems)

	// Verify we got update operations (incremental should have mixed types)
	if updateCount == 0 {
		t.Log("Note: No update operations found - all items may be new")
	}
}

// TestAllExportsLoadable verifies all three exports can be loaded from mock S3.
func TestAllExportsLoadable(t *testing.T) {
	testDataDir, err := filepath.Abs("../s3exportdata")
	if err != nil {
		t.Fatalf("Failed to get absolute path to s3exportdata: %v", err)
	}

	mockS3 := mock.NewS3Client(testDataDir)
	if err := mockS3.LoadTestFiles(); err != nil {
		t.Fatalf("Failed to load test files: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	manifestLoader := manifest.NewS3Loader(mockS3)

	exports := []struct {
		name       string
		uri        string
		exportType string
		itemCount  int64
	}{
		{
			name:       "FULL export",
			uri:        "s3://test-bucket/AWSDynamoDB/01768385930622-efd1a093/manifest-summary.json",
			exportType: "FULL_EXPORT",
			itemCount:  3,
		},
		{
			name:       "INCREMENTAL export #1",
			uri:        "s3://test-bucket/AWSDynamoDB/01768386924000-d339e52d/manifest-summary.json",
			exportType: "INCREMENTAL_EXPORT",
			itemCount:  6,
		},
		{
			name:       "INCREMENTAL export #2",
			uri:        "s3://test-bucket/AWSDynamoDB/01768388186000-4a2fc3ff/manifest-summary.json",
			exportType: "INCREMENTAL_EXPORT",
			itemCount:  5,
		},
	}

	for _, exp := range exports {
		t.Run(exp.name, func(t *testing.T) {
			summary, err := manifestLoader.Load(ctx, exp.uri)
			if err != nil {
				t.Fatalf("Failed to load manifest: %v", err)
			}

			if summary.ItemCount != exp.itemCount {
				t.Errorf("Expected %d items, got %d", exp.itemCount, summary.ItemCount)
			}

			t.Logf("Loaded %s: %d items, %d data files",
				exp.name, summary.ItemCount, len(summary.DataFiles))
		})
	}
}

// TestDataCorrectnessAfterOperations tests that data is correct after applying
// FULL export, then INCREMENTAL exports with PUTs, UPDATEs, and DELETEs.
// This test verifies the complete restore chain and validates final state.
func TestDataCorrectnessAfterOperations(t *testing.T) {
	testDataDir, err := filepath.Abs("../s3exportdata")
	if err != nil {
		t.Fatalf("Failed to get absolute path: %v", err)
	}

	mockS3 := mock.NewS3Client(testDataDir)
	if err := mockS3.LoadTestFiles(); err != nil {
		t.Fatalf("Failed to load test files: %v", err)
	}

	mockDynamoDB := mock.NewDynamoDBClient()
	tableName := "test-table"

	manifestLoader := manifest.NewS3Loader(mockS3)
	streamer := s3streamer.NewS3Streamer(mockS3)
	decoder := itemimage.NewJSONDecoder()
	ddbWriter := writer.NewDynamoDBWriter(mockDynamoDB, tableName, 25, writer.Callbacks{})

	ctx := context.Background()

	// Helper to create key map for lookups
	makeKey := func(pk, sk string) map[string]types.AttributeValue {
		return map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
			"sk": &types.AttributeValueMemberS{Value: sk},
		}
	}

	// Helper to process an export
	processExport := func(t *testing.T, exportURI string) {
		summary, err := manifestLoader.Load(ctx, exportURI)
		if err != nil {
			t.Fatalf("Failed to load manifest: %v", err)
		}

		for _, file := range summary.DataFiles {
			var ops []itemimage.Operation
			err := streamer.Stream(ctx, mock.ExportBucket, file.Key, 0, func(line []byte, _ int64) error {
				op, err := decoder.Decode(line)
				if err != nil {
					return nil
				}
				ops = append(ops, op)
				return nil
			})
			if err != nil {
				t.Logf("Warning: streaming %s: %v", file.Key, err)
				continue
			}
			if len(ops) > 0 {
				if err := ddbWriter.WriteBatch(ctx, ops); err != nil {
					t.Fatalf("Failed to write batch: %v", err)
				}
			}
		}
	}

	// Phase 1: Apply FULL export (3 items: pk=1/sk=1, pk=1/sk=2, pk=1/sk=3)
	t.Run("Phase1_FullExport", func(t *testing.T) {
		processExport(t, "s3://test-bucket/AWSDynamoDB/01768385930622-efd1a093/manifest-summary.json")

		contents := mockDynamoDB.GetTableContents(tableName)
		if len(contents) != 3 {
			t.Errorf("Expected 3 items after FULL export, got %d", len(contents))
		}

		// Verify specific items exist
		if !mockDynamoDB.ItemExists(tableName, makeKey("1", "1")) {
			t.Error("Expected item pk=1,sk=1 to exist")
		}
		if !mockDynamoDB.ItemExists(tableName, makeKey("1", "2")) {
			t.Error("Expected item pk=1,sk=2 to exist")
		}
		if !mockDynamoDB.ItemExists(tableName, makeKey("1", "3")) {
			t.Error("Expected item pk=1,sk=3 to exist")
		}
		t.Logf("FULL export: %d items in table", len(contents))
	})

	// Phase 2: Apply INCREMENTAL #1 (6 PUTs: pk=2/sk=1,2,3 and pk=3/sk=1,2,3)
	t.Run("Phase2_Incremental1", func(t *testing.T) {
		processExport(t, "s3://test-bucket/AWSDynamoDB/01768386924000-d339e52d/manifest-summary.json")

		contents := mockDynamoDB.GetTableContents(tableName)
		if len(contents) != 9 {
			t.Errorf("Expected 9 items after INCREMENTAL #1, got %d", len(contents))
		}

		// Verify new items from incremental
		if !mockDynamoDB.ItemExists(tableName, makeKey("2", "1")) {
			t.Error("Expected item pk=2,sk=1 to exist")
		}
		if !mockDynamoDB.ItemExists(tableName, makeKey("2", "2")) {
			t.Error("Expected item pk=2,sk=2 to exist")
		}
		if !mockDynamoDB.ItemExists(tableName, makeKey("2", "3")) {
			t.Error("Expected item pk=2,sk=3 to exist")
		}
		if !mockDynamoDB.ItemExists(tableName, makeKey("3", "1")) {
			t.Error("Expected item pk=3,sk=1 to exist")
		}
		t.Logf("INCREMENTAL #1: %d items in table", len(contents))
	})

	// Phase 3: Apply INCREMENTAL #2 (2 DELETEs, 2 UPDATEs, 1 PUT)
	// DELETEs: pk=2,sk=3 and pk=1,sk=1
	// UPDATEs: pk=1,sk=3 (adds bin_update) and pk=3,sk=2 (removes number)
	// PUT: pk=4,sk=2
	t.Run("Phase3_Incremental2", func(t *testing.T) {
		processExport(t, "s3://test-bucket/AWSDynamoDB/01768388186000-4a2fc3ff/manifest-summary.json")

		contents := mockDynamoDB.GetTableContents(tableName)
		// 9 - 2 deletes + 1 put = 8 items
		if len(contents) != 8 {
			t.Errorf("Expected 8 items after INCREMENTAL #2, got %d", len(contents))
		}

		// Verify deletes removed items
		if mockDynamoDB.ItemExists(tableName, makeKey("2", "3")) {
			t.Error("Item pk=2,sk=3 should have been deleted")
		}
		if mockDynamoDB.ItemExists(tableName, makeKey("1", "1")) {
			t.Error("Item pk=1,sk=1 should have been deleted")
		}

		// Verify new item exists
		if !mockDynamoDB.ItemExists(tableName, makeKey("4", "2")) {
			t.Error("Expected new item pk=4,sk=2 to exist")
		}

		// Verify updated item pk=1,sk=3 has bin_update attribute
		item13 := mockDynamoDB.GetItem(tableName, makeKey("1", "3"))
		if item13 == nil {
			t.Fatal("Expected item pk=1,sk=3 to exist")
		}
		if _, hasBinUpdate := item13["bin_update"]; !hasBinUpdate {
			t.Error("Item pk=1,sk=3 should have bin_update attribute after update")
		}

		// Verify updated item pk=3,sk=2 no longer has number attribute
		item32 := mockDynamoDB.GetItem(tableName, makeKey("3", "2"))
		if item32 == nil {
			t.Fatal("Expected item pk=3,sk=2 to exist")
		}
		if _, hasNumber := item32["number"]; hasNumber {
			t.Error("Item pk=3,sk=2 should NOT have number attribute after update")
		}

		t.Logf("INCREMENTAL #2: %d items in table (2 deleted, 1 added, 2 updated)", len(contents))
	})

	// Final state verification
	t.Run("FinalState", func(t *testing.T) {
		contents := mockDynamoDB.GetTableContents(tableName)
		t.Logf("Final table state: %d items", len(contents))

		expectedItems := []struct{ pk, sk string }{
			{"1", "2"}, // Original from FULL
			{"1", "3"}, // Original from FULL, updated in INCREMENTAL #2
			{"2", "1"}, // From INCREMENTAL #1
			{"2", "2"}, // From INCREMENTAL #1
			{"3", "1"}, // From INCREMENTAL #1
			{"3", "2"}, // From INCREMENTAL #1, updated in INCREMENTAL #2
			{"3", "3"}, // From INCREMENTAL #1
			{"4", "2"}, // From INCREMENTAL #2
		}

		for _, exp := range expectedItems {
			if !mockDynamoDB.ItemExists(tableName, makeKey(exp.pk, exp.sk)) {
				t.Errorf("Expected item pk=%s,sk=%s to exist in final state", exp.pk, exp.sk)
			}
		}

		deletedItems := []struct{ pk, sk string }{
			{"1", "1"}, // Deleted in INCREMENTAL #2
			{"2", "3"}, // Deleted in INCREMENTAL #2
		}

		for _, del := range deletedItems {
			if mockDynamoDB.ItemExists(tableName, makeKey(del.pk, del.sk)) {
				t.Errorf("Item pk=%s,sk=%s should be deleted", del.pk, del.sk)
			}
		}
	})
}

// fullExportDataFile is the one data file of the FULL export fixture that holds items:
// three lines, gzipped, which is enough for a resume to land inside it.
const fullExportDataFile = "AWSDynamoDB/01768385930622-efd1a093/data/5mrfg3b44e3vhnfickkozoym6a.json.gz"

// fullExportURI is the manifest of the FULL export fixture.
const fullExportURI = "s3://test-bucket/AWSDynamoDB/01768385930622-efd1a093/manifest-summary.json"

// loadFixtures returns a mock S3 holding every export under s3exportdata.
func loadFixtures(t *testing.T) *mock.S3Client {
	t.Helper()
	testDataDir, err := filepath.Abs("../s3exportdata")
	if err != nil {
		t.Fatalf("Failed to get absolute path to s3exportdata: %v", err)
	}
	mockS3 := mock.NewS3Client(testDataDir)
	if err := mockS3.LoadTestFiles(); err != nil {
		t.Fatalf("Failed to load test files: %v", err)
	}
	return mockS3
}

// restoreConfig is a validated configuration for restoring the given export with one
// worker, so the order items reach the table is the order of the file.
func restoreConfig(t *testing.T, exportURI string) *config.Config {
	t.Helper()
	cfg := &config.Config{
		TableName:       "test-table",
		ExportS3URI:     exportURI,
		ExportType:      "FULL",
		ViewType:        "NEW",
		Region:          "us-west-2",
		MaxWorkers:      1,
		BatchSize:       1,
		ShutdownTimeout: time.Second,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Invalid config: %v", err)
	}
	return cfg
}

// TestResumeSkipsLinesAlreadyWrittenAgainstTheRealStreamer verifies a checkpoint
// holding an offset the real streamer reported for a gzipped file resumes exactly after
// that line. The streamer's own offset argument is a position in the compressed object,
// so a checkpoint offset handed straight back to it would range into the middle of the
// gzip stream; only a test against the real streamer and a real gzipped file can tell
// a resume that works from one that merely looks right against a line-counting double.
func TestResumeSkipsLinesAlreadyWrittenAgainstTheRealStreamer(t *testing.T) {
	mockS3 := loadFixtures(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The offsets a checkpoint can hold are the ones the streamer reports.
	streamer := s3streamer.NewS3Streamer(mockS3)
	var offsets []int64
	err := streamer.Stream(ctx, mock.ExportBucket, fullExportDataFile, 0, func(_ []byte, offset int64) error {
		offsets = append(offsets, offset)
		return nil
	})
	if err != nil || len(offsets) != 3 {
		t.Fatalf("expected the fixture to stream 3 lines, got %d lines, err %v", len(offsets), err)
	}

	cfg := restoreConfig(t, fullExportURI)
	summary, err := manifest.NewS3Loader(mockS3).Load(ctx, cfg.ExportS3URI)
	if err != nil {
		t.Fatalf("Failed to load manifest: %v", err)
	}
	store := checkpoint.NewMemoryStore()
	if err := store.Save(ctx, checkpoint.State{
		ExportID: summary.ExportARN,
		Offsets:  map[string]int64{fullExportDataFile: offsets[1]},
	}); err != nil {
		t.Fatalf("Failed to seed checkpoint: %v", err)
	}
	mockDynamoDB := mock.NewDynamoDBClient()
	coord := coordinator.NewCoordinator(cfg, manifest.NewS3Loader(mockS3), streamer,
		itemimage.NewJSONDecoder(),
		writer.NewDynamoDBWriter(mockDynamoDB, cfg.TableName, cfg.BatchSize, writer.Callbacks{}),
		store, nil, metrics.NewMetrics())

	if err := coord.Run(ctx); err != nil {
		t.Fatalf("Coordinator run failed: %v", err)
	}

	// The fixture holds pk=1 with sk=1, sk=2, sk=3 in that order; only the third
	// follows the second line.
	contents := mockDynamoDB.GetTableContents(cfg.TableName)
	if len(contents) != 1 {
		t.Fatalf("expected only the line after the recorded one written, got %d items", len(contents))
	}
	third := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "1"},
		"sk": &types.AttributeValueMemberS{Value: "3"},
	}
	if !mockDynamoDB.ItemExists(cfg.TableName, third) {
		t.Errorf("expected pk=1,sk=3 to be the item written, got %v", contents)
	}
}

// TestStreamerReadsEachByteOfTheObjectOnce verifies the ranged reads the streamer
// issues, after its compression probe, tile the object exactly once. The mock only
// started honouring Range so that a resume could be tested; this is what keeps that
// honest, since a mock returning the whole object for every range would make any
// chunked read silently duplicate data and any resume test pass for the wrong reason.
func TestStreamerReadsEachByteOfTheObjectOnce(t *testing.T) {
	mockS3 := loadFixtures(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := s3streamer.NewS3Streamer(mockS3).Stream(ctx, mock.ExportBucket, fullExportDataFile, 0,
		func([]byte, int64) error { return nil })
	if err != nil {
		t.Fatalf("stream failed: %v", err)
	}

	size := len(mockS3.Files[mock.ExportBucket+"/"+fullExportDataFile])
	ranges := mockS3.Ranges()
	if len(ranges) < 2 {
		t.Fatalf("expected a probe followed by at least one chunk, got %v", ranges)
	}
	next := 0
	for _, r := range ranges[1:] {
		var start, end int
		if _, err := fmt.Sscanf(r, "bytes=%d-%d", &start, &end); err != nil {
			t.Fatalf("unexpected range %q: %v", r, err)
		}
		if start != next {
			t.Fatalf("ranges %v do not tile the object from byte 0 without gaps or overlap", ranges[1:])
		}
		next = end + 1
	}
	if next < size {
		t.Errorf("ranges %v stop at byte %d of %d", ranges[1:], next, size)
	}
}

// failingWriter passes batches through to another writer until failFrom, counting from
// one, and refuses every batch from then on. It stands in for a restore whose table
// stopped accepting writes part-way through.
type failingWriter struct {
	inner    writer.Writer
	failFrom int
	calls    int
}

func (f *failingWriter) WriteBatch(ctx context.Context, ops []itemimage.Operation) error {
	f.calls++
	if f.calls >= f.failFrom {
		return fmt.Errorf("table unavailable")
	}
	return f.inner.WriteBatch(ctx, ops)
}

// TestInterruptedRestoreResumesWithoutRewritingItems verifies a restore that fails
// part-way through a gzipped file, then runs again against the same checkpoint,
// writes every item exactly once across the two runs. No offset appears in this test:
// it proves the checkpoint the first run saved at shutdown is one the second run can
// resume from, through the real streamer and the real decoder.
func TestInterruptedRestoreResumesWithoutRewritingItems(t *testing.T) {
	mockS3 := loadFixtures(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := restoreConfig(t, fullExportURI)
	mockDynamoDB := mock.NewDynamoDBClient()
	store := checkpoint.NewMemoryStore()
	realWriter := writer.NewDynamoDBWriter(mockDynamoDB, cfg.TableName, cfg.BatchSize, writer.Callbacks{},
		writer.WithBackoff(writer.NewExponentialBackoff(time.Millisecond, time.Millisecond)))
	run := func(w writer.Writer) error {
		return coordinator.NewCoordinator(cfg, manifest.NewS3Loader(mockS3), s3streamer.NewS3Streamer(mockS3),
			itemimage.NewJSONDecoder(), w, store, nil, metrics.NewMetrics(),
			coordinator.WithStreamBackoff(writer.NewExponentialBackoff(time.Millisecond, time.Millisecond)),
		).Run(ctx)
	}

	// The first run writes one item and then loses its table.
	if err := run(&failingWriter{inner: realWriter, failFrom: 2}); err == nil {
		t.Fatal("expected the first run to fail")
	}
	if got := len(mockDynamoDB.GetBatchWrites()); got != 1 {
		t.Fatalf("expected the first run to have written 1 batch, got %d", got)
	}

	if err := run(realWriter); err != nil {
		t.Fatalf("expected the second run to finish, got %v", err)
	}

	if got := len(mockDynamoDB.GetTableContents(cfg.TableName)); got != 3 {
		t.Errorf("expected all 3 items in the table, got %d", got)
	}
	if got := len(mockDynamoDB.GetBatchWrites()); got != 3 {
		t.Errorf("expected 3 batch writes across both runs, one per item, got %d", got)
	}
}
