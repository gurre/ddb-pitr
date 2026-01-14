package manifest

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// mockS3Client implements the aws.S3Client interface for testing
type mockS3Client struct {
	data  map[string][]byte
	etags map[string]string // Custom ETags for specific keys
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if params.Key == nil {
		return nil, fmt.Errorf("key is nil")
	}

	data, ok := m.data[*params.Key]
	if !ok {
		return nil, &types.NoSuchKey{}
	}

	return &s3.GetObjectOutput{
		Body: &mockReadCloser{data: data},
	}, nil
}

func (m *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if params.Key == nil {
		return nil, fmt.Errorf("key is nil")
	}

	// Check if we have a custom ETag for this key
	if m.etags != nil {
		if etag, ok := m.etags[*params.Key]; ok {
			return &s3.HeadObjectOutput{
				ETag: aws.String(etag),
			}, nil
		}
	}

	// Fall back to default behavior
	data, ok := m.data[*params.Key]
	if !ok {
		return nil, &types.NoSuchKey{}
	}

	return &s3.HeadObjectOutput{
		ETag: aws.String(fmt.Sprintf("%x", data)), // Mock ETag as a hex string of the data
	}, nil
}

// mockReadCloser implements io.ReadCloser for testing
type mockReadCloser struct {
	data   []byte
	offset int
}

func (m *mockReadCloser) Read(p []byte) (n int, err error) {
	if m.offset >= len(m.data) {
		return 0, io.EOF
	}

	n = copy(p, m.data[m.offset:])
	m.offset += n

	if m.offset >= len(m.data) {
		err = io.EOF
	}

	return n, err
}

func (m *mockReadCloser) Close() error {
	return nil
}

// loadTestFile loads test data from the testdata directory
func loadTestFile(t *testing.T, path string) []byte {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read test file %s: %v", path, err)
	}

	return data
}

// TestManifestLoaderErrorCases tests error handling in the loader
func TestManifestLoaderErrorCases(t *testing.T) {
	// Test missing files
	mockClient := &mockS3Client{
		data: map[string][]byte{},
	}
	loader := NewS3Loader(mockClient)

	_, err := loader.Load(context.Background(), "s3://test-bucket/test-key")
	if err == nil {
		t.Error("expected error for missing files, got nil")
	}
}

// TestFullExport tests loading a full export manifest
func TestFullExport(t *testing.T) {
	// Set up paths
	summaryPath := "../s3exportdata/AWSDynamoDB/01768385930622-efd1a093/manifest-summary.json"
	filesPath := "../s3exportdata/AWSDynamoDB/01768385930622-efd1a093/manifest-files.json"

	summaryKey := "AWSDynamoDB/01768385930622-efd1a093/manifest-summary.json"
	filesKey := "AWSDynamoDB/01768385930622-efd1a093/manifest-files.json"

	// Set up mock client with real data
	mockClient := &mockS3Client{
		data: map[string][]byte{
			summaryKey: loadTestFile(t, summaryPath),
			filesKey:   loadTestFile(t, filesPath),
		},
	}

	// Create loader
	loader := NewS3Loader(mockClient)

	// Load manifest
	summary, err := loader.Load(context.Background(), "s3://test-bucket/"+summaryKey)
	if err != nil {
		t.Fatalf("failed to load manifest: %v", err)
	}

	if summary.ItemCount != 3 {
		t.Errorf("expected 3 items, got %d", summary.ItemCount)
	}

	if len(summary.DataFiles) != 4 {
		t.Errorf("expected 4 data files, got %d", len(summary.DataFiles))
	}
}

// TestIncrementalExport tests loading an incremental export manifest
func TestIncrementalExport(t *testing.T) {
	// Set up paths
	summaryPath := "../s3exportdata/AWSDynamoDB/01768386924000-d339e52d/manifest-summary.json"
	filesPath := "../s3exportdata/AWSDynamoDB/01768386924000-d339e52d/manifest-files.json"

	summaryKey := "AWSDynamoDB/01768386924000-d339e52d/manifest-summary.json"
	filesKey := "AWSDynamoDB/01768386924000-d339e52d/manifest-files.json"

	// Set up mock client with real data
	mockClient := &mockS3Client{
		data: map[string][]byte{
			summaryKey: loadTestFile(t, summaryPath),
			filesKey:   loadTestFile(t, filesPath),
		},
	}

	// Create loader
	loader := NewS3Loader(mockClient)

	// Load manifest
	summary, err := loader.Load(context.Background(), "s3://test-bucket/"+summaryKey)
	if err != nil {
		t.Fatalf("failed to load manifest: %v", err)
	}

	// Verify incremental export properties
	if summary.ExportType != "INCREMENTAL_EXPORT" {
		t.Errorf("expected INCREMENTAL_EXPORT export type, got %s", summary.ExportType)
	}

	if summary.OutputView != "NEW_AND_OLD_IMAGES" {
		t.Errorf("expected NEW_AND_OLD_IMAGES view type, got %s", summary.OutputView)
	}

	if len(summary.DataFiles) != 4 {
		t.Errorf("expected 4 data files, got %d", len(summary.DataFiles))
	}

	// Verify export time range
	if summary.ExportFromTime == "" || summary.ExportToTime == "" {
		t.Errorf("expected export time range, got from=%s, to=%s",
			summary.ExportFromTime, summary.ExportToTime)
	}
}

// TestInvalidS3URI tests handling of invalid S3 URIs
func TestInvalidS3URI(t *testing.T) {
	loader := NewS3Loader(&mockS3Client{})

	invalidURIs := []string{
		"not-an-s3-uri",
		"s3://",
		"s3://bucket",
		"file:///path/to/file",
	}

	for _, uri := range invalidURIs {
		_, err := loader.Load(context.Background(), uri)
		if err == nil {
			t.Errorf("expected error for invalid URI %s, got nil", uri)
		}
	}
}
