package manifest

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// testMD5Base64 and testMD5Hex are the same digest in the two encodings the manifest and
// S3 respectively use for it.
const (
	testMD5Base64 = "XrY7u+Ae7tCTyyK7j1rNww=="
	testMD5Hex    = "5eb63bbbe01eeed093cb22bb8f5acdc3"
)

// mockS3Client implements the aws.S3Client interface for testing
type mockS3Client struct {
	data    map[string][]byte
	etags   map[string]string // Custom ETags for specific keys
	buckets []string          // Buckets the loader asked for, in order
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if params.Key == nil {
		return nil, fmt.Errorf("key is nil")
	}

	m.buckets = append(m.buckets, *params.Bucket)

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

	// Check if we have a custom ETag for this key. An empty one stands for the S3
	// implementations that omit the header entirely.
	if m.etags != nil {
		if etag, ok := m.etags[*params.Key]; ok {
			if etag == "" {
				return &s3.HeadObjectOutput{}, nil
			}
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

// TestVerifyChecksumsAcceptsMatchingETag verifies a data file whose S3 ETag matches the
// MD5 the manifest recorded passes verification. The manifest carries the checksum in
// Base64 while S3 reports it as hex, so the comparison has to convert.
func TestVerifyChecksumsAcceptsMatchingETag(t *testing.T) {
	summary := summaryWithChecksum("data-001.json.gz", testMD5Base64)
	// S3 reports the same digest in hex.
	loader := NewS3Loader(&mockS3Client{
		etags: map[string]string{"data-001.json.gz": testMD5Hex},
	})

	if err := loader.VerifyChecksums(context.Background(), summary); err != nil {
		t.Errorf("expected a matching checksum to verify, got %v", err)
	}
}

// TestVerifyChecksumsAcceptsQuotedETag verifies the quoting some S3 implementations put
// around an ETag does not fail an otherwise matching file.
func TestVerifyChecksumsAcceptsQuotedETag(t *testing.T) {
	summary := summaryWithChecksum("data-001.json.gz", testMD5Base64)
	loader := NewS3Loader(&mockS3Client{
		etags: map[string]string{"data-001.json.gz": `"` + testMD5Hex + `"`},
	})

	if err := loader.VerifyChecksums(context.Background(), summary); err != nil {
		t.Errorf("expected a quoted matching checksum to verify, got %v", err)
	}
}

// TestVerifyChecksumsRejectsMismatch verifies a data file whose content differs from what
// the manifest recorded is reported. Restoring a corrupted export silently is the failure
// this check exists to prevent.
func TestVerifyChecksumsRejectsMismatch(t *testing.T) {
	summary := summaryWithChecksum("data-001.json.gz", testMD5Base64)
	loader := NewS3Loader(&mockS3Client{
		etags: map[string]string{"data-001.json.gz": "00000000000000000000000000000000"},
	})

	err := loader.VerifyChecksums(context.Background(), summary)
	if err == nil {
		t.Fatal("expected a checksum mismatch to be reported")
	}
	if !strings.Contains(err.Error(), "data-001.json.gz") {
		t.Errorf("expected the offending file named, got %v", err)
	}
}

// TestVerifyChecksumsRejectsUnusableInputs verifies verification refuses to pass when it
// cannot actually compare: no bucket to read from, no ETag reported, or a checksum that
// is not valid Base64.
func TestVerifyChecksumsRejectsUnusableInputs(t *testing.T) {
	tests := []struct {
		name    string
		summary Summary
		client  *mockS3Client
	}{
		{
			name:    "no bucket in the summary",
			summary: Summary{DataFiles: []FileMeta{{Key: "data-001.json.gz"}}},
			client:  &mockS3Client{},
		},
		{
			name:    "S3 reports no ETag",
			summary: summaryWithChecksum("data-001.json.gz", testMD5Base64),
			client:  &mockS3Client{etags: map[string]string{"data-001.json.gz": ""}},
		},
		{
			name:    "the data file cannot be read",
			summary: summaryWithChecksum("data-001.json.gz", testMD5Base64),
			client:  &mockS3Client{data: map[string][]byte{}},
		},
		{
			name:    "checksum is not Base64",
			summary: summaryWithChecksum("data-001.json.gz", "not base64!"),
			client:  &mockS3Client{etags: map[string]string{"data-001.json.gz": "abc"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := NewS3Loader(tt.client).VerifyChecksums(context.Background(), tt.summary); err == nil {
				t.Error("expected verification to fail")
			}
		})
	}
}

// TestExtractsBucketAndKeyFromURI verifies the export URI is split into the bucket and the
// key rather than being swapped, which would send every request to the wrong place.
func TestExtractsBucketAndKeyFromURI(t *testing.T) {
	const summaryKey = "AWSDynamoDB/01768385930622-efd1a093/manifest-summary.json"
	mockClient := &mockS3Client{data: map[string][]byte{}}

	// The loader reports the key it could not find, and the mock only serves the key it
	// was given, so a swapped bucket and key shows up here.
	_, err := NewS3Loader(mockClient).Load(context.Background(), "s3://test-bucket/"+summaryKey)
	if err == nil {
		t.Fatal("expected an error for a missing manifest")
	}

	mockClient.data[summaryKey] = []byte(`{"itemCount":1,"manifestFilesS3Key":"files.json"}`)
	mockClient.data["files.json"] = []byte(`{"dataFileS3Key":"data-001.json.gz"}`)

	summary, err := NewS3Loader(mockClient).Load(context.Background(), "s3://test-bucket/"+summaryKey)
	if err != nil {
		t.Fatalf("failed to load manifest: %v", err)
	}
	if len(summary.DataFiles) != 1 || summary.DataFiles[0].Key != "data-001.json.gz" {
		t.Errorf("expected the data file parsed from the manifest, got %v", summary.DataFiles)
	}
	for _, bucket := range mockClient.buckets {
		if bucket != "test-bucket" {
			t.Errorf("expected every request against test-bucket, got %q", bucket)
		}
	}
}

// summaryWithChecksum builds a one-file summary carrying the given Base64 MD5.
func summaryWithChecksum(key, md5Base64 string) Summary {
	return Summary{
		S3Bucket:  "test-bucket",
		DataFiles: []FileMeta{{Key: key, MD5Base64: md5Base64}},
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
