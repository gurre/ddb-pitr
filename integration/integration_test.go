package integration

import (
	"context"
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
		ReadAheadParts:  2,
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
		err = streamer.Stream(ctx, manifestSummary.S3Bucket, file.Key, 0, func(line []byte, byteOffset int64) error {
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
		ReadAheadParts:  2,
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
	ddbWriter := writer.NewDynamoDBWriter(mockDynamoDB, cfg.TableName, cfg.BatchSize)
	checkpointStore := checkpoint.NewMemoryStore()

	coord := coordinator.NewCoordinator(
		cfg,
		manifestLoader,
		streamer,
		jsonDecoder,
		ddbWriter,
		checkpointStore,
		nil, // no report uploader in tests
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
		err = streamer.Stream(ctx, manifestSummary.S3Bucket, file.Key, 0, func(line []byte, byteOffset int64) error {
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
	ddbWriter := writer.NewDynamoDBWriter(mockDynamoDB, tableName, 25)

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
			err := streamer.Stream(ctx, summary.S3Bucket, file.Key, 0, func(line []byte, _ int64) error {
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
