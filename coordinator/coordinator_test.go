package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gurre/ddb-pitr/checkpoint"
	"github.com/gurre/ddb-pitr/config"
	"github.com/gurre/ddb-pitr/itemimage"
	"github.com/gurre/ddb-pitr/manifest"
)

type mockLoader struct {
	summary manifest.Summary
}

func (m *mockLoader) Load(ctx context.Context, manifestS3URI string) (manifest.Summary, error) {
	return m.summary, nil
}

func (m *mockLoader) VerifyChecksums(ctx context.Context, summary manifest.Summary) error {
	return nil
}

type mockStreamer struct {
	data [][]byte
}

func (m *mockStreamer) Stream(ctx context.Context, bucket, key string, offset int64, fn func([]byte, int64) error) error {
	for i, line := range m.data {
		if err := fn(line, int64(i)); err != nil {
			return err
		}
	}
	return nil
}

type mockDecoder struct{}

func (m *mockDecoder) Decode(line []byte) (itemimage.Operation, error) {
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
	batches [][]itemimage.Operation
}

func (m *mockWriter) WriteBatch(ctx context.Context, ops []itemimage.Operation) error {
	m.batches = append(m.batches, ops)
	return nil
}

func (m *mockWriter) Flush(ctx context.Context) error {
	return nil
}

type mockStore struct {
	state checkpoint.State
}

func (m *mockStore) Load(ctx context.Context) (checkpoint.State, error) {
	return m.state, nil
}

func (m *mockStore) Save(ctx context.Context, s checkpoint.State) error {
	m.state = s
	return nil
}

func TestCoordinatorHappyPath(t *testing.T) {
	// Set up test data
	testData := [][]byte{
		[]byte(`{"id":"123","name":"test"}`),
		[]byte(`{"id":"124","name":"test2"}`),
	}

	// Create mocks
	loader := &mockLoader{
		summary: manifest.Summary{
			S3Bucket:  "test-bucket",
			ItemCount: 2,
			DataFiles: []manifest.FileMeta{
				{Key: "file1", ItemCount: 2},
			},
		},
	}
	streamer := &mockStreamer{data: testData}
	decoder := &mockDecoder{}
	writer := &mockWriter{}
	store := &mockStore{}

	// Create coordinator
	cfg := &config.Config{
		TableName:       "test-table",
		ExportS3URI:     "s3://test-bucket/test-prefix",
		ExportType:      "FULL",
		ViewType:        "NEW",
		Region:          "us-west-2",
		MaxWorkers:      1,
		ReadAheadParts:  1,
		BatchSize:       10,
		ShutdownTimeout: time.Second,
	}

	// Validate config to parse the S3 URI and populate exportBucketName
	if err := cfg.Validate(); err != nil {
		t.Fatalf("failed to validate config: %v", err)
	}

	coord := NewCoordinator(cfg, loader, streamer, decoder, writer, store, nil)

	// Run coordinator
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := coord.Run(ctx); err != nil {
		t.Fatalf("coordinator failed: %v", err)
	}

	// Verify results
	if len(writer.batches) != 1 {
		t.Errorf("expected 1 batch, got %d", len(writer.batches))
	}
	if len(writer.batches[0]) != 2 {
		t.Errorf("expected 2 operations in batch, got %d", len(writer.batches[0]))
	}
}
