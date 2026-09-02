package manifest

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

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

// testBucket is the bucket the tests ask verification to check.
const testBucket = "test-bucket"

// mockS3Client implements the aws.S3Client interface for testing. It counts calls that
// arrived without the marker a test put on its context, which is how a call made under a
// substituted context is told from one made under the caller's.
type mockS3Client struct {
	data     map[string][]byte
	etags    map[string]string // Custom ETags for specific keys
	buckets  []string          // Buckets the loader asked for, in order
	detached int
	inFlight int32 // Calls currently in progress
	peak     int32 // Most calls ever in progress at once
	mu       sync.Mutex
}

// enter notes a call starting and reports the marker for leave; together they measure
// how many calls overlap, which is how a test sees verification running in parallel.
func (m *mockS3Client) enter() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inFlight++
	if m.inFlight > m.peak {
		m.peak = m.inFlight
	}
}

func (m *mockS3Client) leave() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inFlight--
}

// callerContextKey marks the context a test passed in.
type callerContextKey struct{}

func (m *mockS3Client) noteContext(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx.Value(callerContextKey{}) == nil {
		m.detached++
	}
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.noteContext(ctx)
	if params.Key == nil {
		return nil, fmt.Errorf("key is nil")
	}
	if params.Bucket == nil || *params.Bucket == "" {
		return nil, fmt.Errorf("no bucket in request")
	}

	m.mu.Lock()
	m.buckets = append(m.buckets, *params.Bucket)
	data, ok := m.data[*params.Key]
	m.mu.Unlock()
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
	m.enter()
	defer m.leave()
	// Overlapping calls have to be given the chance to overlap.
	time.Sleep(time.Millisecond)
	m.noteContext(ctx)
	if params.Key == nil {
		return nil, fmt.Errorf("key is nil")
	}
	if params.Bucket == nil || *params.Bucket == "" {
		return nil, fmt.Errorf("no bucket in request")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.buckets = append(m.buckets, *params.Bucket)

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

// mockReadCloser implements io.ReadCloser for testing. Reading a closed body fails, the
// way a real S3 response body does, so a loader that closed one before finishing with it
// is caught here rather than in production.
type mockReadCloser struct {
	data   []byte
	offset int
	closed bool
}

func (m *mockReadCloser) Read(p []byte) (n int, err error) {
	if m.closed {
		return 0, fmt.Errorf("read after the response body was closed")
	}
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
	m.closed = true
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

	result, err := loader.VerifyChecksums(context.Background(), testBucket, summary)
	if err != nil {
		t.Fatalf("expected a matching checksum to verify, got %v", err)
	}
	if result.Verified != 1 {
		t.Errorf("expected 1 file counted as verified, got %d", result.Verified)
	}
}

// TestVerifyChecksumsAcceptsQuotedETag verifies the quoting some S3 implementations put
// around an ETag does not fail an otherwise matching file.
func TestVerifyChecksumsAcceptsQuotedETag(t *testing.T) {
	summary := summaryWithChecksum("data-001.json.gz", testMD5Base64)
	loader := NewS3Loader(&mockS3Client{
		etags: map[string]string{"data-001.json.gz": `"` + testMD5Hex + `"`},
	})

	result, err := loader.VerifyChecksums(context.Background(), testBucket, summary)
	if err != nil {
		t.Fatalf("expected a quoted matching checksum to verify, got %v", err)
	}
	if result.Verified != 1 {
		t.Errorf("expected 1 file counted as verified, got %d", result.Verified)
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

	_, err := loader.VerifyChecksums(context.Background(), testBucket, summary)
	if err == nil {
		t.Fatal("expected a checksum mismatch to be reported")
	}
	if !strings.Contains(err.Error(), "data-001.json.gz") {
		t.Errorf("expected the offending file named, got %v", err)
	}
}

// TestVerifyChecksumsMatchesRecordedETag verifies a data file is accepted when the ETag
// S3 reports is the one the manifest recorded at export time. Real exports store large
// files in parts, whose ETag is a digest of the parts' digests and never equals the
// object's MD5, so this is the only comparison that can pass for them.
func TestVerifyChecksumsMatchesRecordedETag(t *testing.T) {
	const multipartETag = "7deb4078f238dd87d6af0538152c04e9-1"
	summary := Summary{
		S3Bucket: "test-bucket",
		// The MD5 is present and deliberately does not match the ETag, exactly as it
		// does not in a real manifest for a file uploaded in parts.
		DataFiles: []FileMeta{{Key: "data-001.json.gz", ETag: multipartETag, MD5Base64: testMD5Base64}},
	}
	loader := NewS3Loader(&mockS3Client{etags: map[string]string{"data-001.json.gz": multipartETag}})

	result, err := loader.VerifyChecksums(context.Background(), testBucket, summary)
	if err != nil {
		t.Fatalf("expected a file matching its recorded ETag to verify, got %v", err)
	}
	if result.Verified != 1 {
		t.Errorf("expected 1 file counted as verified, got %d", result.Verified)
	}
}

// TestVerifyChecksumsRejectsChangedETag verifies a data file that has been replaced
// since the export was written is reported. This is the failure verification exists to
// catch, and it must not be softened into "unverifiable" just because the ETag is
// multipart.
func TestVerifyChecksumsRejectsChangedETag(t *testing.T) {
	summary := Summary{
		S3Bucket:  "test-bucket",
		DataFiles: []FileMeta{{Key: "data-001.json.gz", ETag: "7deb4078f238dd87d6af0538152c04e9-1"}},
	}
	loader := NewS3Loader(&mockS3Client{
		etags: map[string]string{"data-001.json.gz": "b5443265c5e545b6c8d8275e0b6f8c15-1"},
	})

	if _, err := loader.VerifyChecksums(context.Background(), testBucket, summary); err == nil {
		t.Error("expected a data file that no longer matches the manifest to be reported")
	}
}

// TestVerifyChecksumsReportsWhatItCannotCheck verifies a file with nothing comparable is
// listed as unverified rather than counted as good. Reporting "verified" for a file
// nothing was checked against would be the more dangerous of the two answers.
func TestVerifyChecksumsReportsWhatItCannotCheck(t *testing.T) {
	summary := Summary{DataFiles: []FileMeta{{Key: "data-001.json.gz"}}}
	loader := NewS3Loader(&mockS3Client{etags: map[string]string{"data-001.json.gz": testMD5Hex}})

	result, err := loader.VerifyChecksums(context.Background(), testBucket, summary)
	if err != nil {
		t.Fatalf("expected an unverifiable file to be reported, not to fail: %v", err)
	}
	if result.Verified != 0 {
		t.Errorf("expected nothing counted as verified, got %d", result.Verified)
	}
	if len(result.Unverified) != 1 || result.Unverified[0] != "data-001.json.gz" {
		t.Errorf("expected the file listed as unverified, got %v", result.Unverified)
	}
}

// TestVerifyChecksumsAcceptsACopiedObjectByItsMD5 verifies a data file whose ETag no
// longer matches the multipart one the export recorded, but whose single-part ETag is
// the MD5 the manifest carries, is verified. Copying an export re-uploads its objects
// and the ETag changes; the MD5 is what still identifies the content, and without it
// no copied export could ever pass.
func TestVerifyChecksumsAcceptsACopiedObjectByItsMD5(t *testing.T) {
	summary := Summary{DataFiles: []FileMeta{
		{Key: "data-001.json.gz", ETag: "7deb4078f238dd87d6af0538152c04e9-1", MD5Base64: testMD5Base64},
	}}
	loader := NewS3Loader(&mockS3Client{etags: map[string]string{"data-001.json.gz": testMD5Hex}})

	result, err := loader.VerifyChecksums(context.Background(), testBucket, summary)
	if err != nil {
		t.Fatalf("expected a copied object with a matching MD5 to verify, got %v", err)
	}
	if result.Verified != 1 {
		t.Errorf("expected 1 file counted as verified, got %d", result.Verified)
	}
}

// TestVerifyChecksumsReadsACopiedMultipartObject verifies a data file whose ETag is a
// multipart digest that matches nothing the manifest recorded is read and its content
// compared to the recorded MD5, passing when the content matches and failing when it
// does not. A large copied file lands here; refusing to decide would leave the operator
// choosing between trusting it blind and not restoring at all.
func TestVerifyChecksumsReadsACopiedMultipartObject(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr bool
	}{
		{name: "content matches the recorded MD5", content: "hello world"},
		{name: "content differs from the recorded MD5", content: "hello there", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			summary := Summary{DataFiles: []FileMeta{
				{Key: "data-001.json.gz", ETag: "7deb4078f238dd87d6af0538152c04e9-1", MD5Base64: testMD5Base64},
			}}
			loader := NewS3Loader(&mockS3Client{
				etags: map[string]string{"data-001.json.gz": "b5443265c5e545b6c8d8275e0b6f8c15-2"},
				data:  map[string][]byte{"data-001.json.gz": []byte(tt.content)},
			})

			result, err := loader.VerifyChecksums(context.Background(), testBucket, summary)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected content that differs from the manifest to fail")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected the object verified by its content, got %v", err)
			}
			if result.Verified != 1 {
				t.Errorf("expected 1 file counted as verified, got %d", result.Verified)
			}
		})
	}
}

// TestVerifyChecksumsChecksFilesInParallel verifies the data files are examined
// several at a time. An export lists a file per partition, tens of thousands for a
// large table, and one round trip each in sequence is a preflight measured in tens of
// minutes with nothing to show for it.
func TestVerifyChecksumsChecksFilesInParallel(t *testing.T) {
	const files = 64
	summary := Summary{}
	client := &mockS3Client{etags: map[string]string{}}
	for i := 0; i < files; i++ {
		key := fmt.Sprintf("data-%03d.json.gz", i)
		summary.DataFiles = append(summary.DataFiles, FileMeta{Key: key, ETag: "abc-1"})
		client.etags[key] = "abc-1"
	}

	result, err := NewS3Loader(client).VerifyChecksums(context.Background(), testBucket, summary)
	if err != nil {
		t.Fatalf("expected every file to verify, got %v", err)
	}
	if result.Verified != files {
		t.Errorf("expected %d files verified, got %d", files, result.Verified)
	}
	if client.peak < 2 {
		t.Errorf("expected files checked concurrently, but at most %d call was in flight", client.peak)
	}
}

// TestVerifyChecksumsAcceptsRealExportManifest verifies the checked-in export manifest,
// whose files all carry the multipart ETags a real DynamoDB export produces, passes
// verification. The restore runs this before writing anything, so a real export failing
// here would block every restore.
func TestVerifyChecksumsAcceptsRealExportManifest(t *testing.T) {
	const prefix = "AWSDynamoDB/01768385930622-efd1a093/"
	summaryKey := prefix + "manifest-summary.json"
	client := &mockS3Client{
		data: map[string][]byte{
			summaryKey:                     loadTestFile(t, "../s3exportdata/"+summaryKey),
			prefix + "manifest-files.json": loadTestFile(t, "../s3exportdata/"+prefix+"manifest-files.json"),
		},
		etags: map[string]string{},
	}

	summary, err := NewS3Loader(client).Load(context.Background(), "s3://test-bucket/"+summaryKey)
	if err != nil {
		t.Fatalf("failed to load manifest: %v", err)
	}
	// S3 reports exactly what the manifest recorded, which is the healthy case.
	for _, file := range summary.DataFiles {
		client.etags[file.Key] = file.ETag
	}

	result, err := NewS3Loader(client).VerifyChecksums(context.Background(), testBucket, summary)
	if err != nil {
		t.Fatalf("expected a real export manifest to verify, got %v", err)
	}
	if result.Verified != len(summary.DataFiles) {
		t.Errorf("verified %d of %d data files, want all of them", result.Verified, len(summary.DataFiles))
	}
}

// TestVerifyChecksumsHeadsTheBucketItWasGiven verifies the data files are looked up in
// the bucket the caller names, not the one the manifest recorded. The manifest names
// where the export was written; a restore reading a copy of it elsewhere would otherwise
// verify objects it never reads, and report the copy as checked.
func TestVerifyChecksumsHeadsTheBucketItWasGiven(t *testing.T) {
	client := &mockS3Client{etags: map[string]string{"data-001.json.gz": testMD5Hex}}
	summary := summaryWithChecksum("data-001.json.gz", testMD5Base64)
	summary.S3Bucket = "where-the-export-was-written"

	if _, err := NewS3Loader(client).VerifyChecksums(context.Background(), "the-copy", summary); err != nil {
		t.Fatalf("expected verification to pass, got %v", err)
	}

	if len(client.buckets) != 1 || client.buckets[0] != "the-copy" {
		t.Errorf("expected the data file headed in the-copy, got %v", client.buckets)
	}
}

// TestVerifyChecksumsRejectsUnusableInputs verifies verification refuses to pass when it
// cannot actually compare: no bucket to read from, no ETag reported, or a checksum that
// is not valid Base64.
func TestVerifyChecksumsRejectsUnusableInputs(t *testing.T) {
	tests := []struct {
		name    string
		bucket  string
		summary Summary
		client  *mockS3Client
	}{
		{
			name:    "no bucket to verify against",
			summary: Summary{DataFiles: []FileMeta{{Key: "data-001.json.gz"}}},
			client:  &mockS3Client{},
		},
		{
			name:    "S3 reports no ETag",
			bucket:  testBucket,
			summary: summaryWithChecksum("data-001.json.gz", testMD5Base64),
			client:  &mockS3Client{etags: map[string]string{"data-001.json.gz": ""}},
		},
		{
			name:    "the data file cannot be read",
			bucket:  testBucket,
			summary: summaryWithChecksum("data-001.json.gz", testMD5Base64),
			client:  &mockS3Client{data: map[string][]byte{}},
		},
		{
			name:    "checksum is not Base64",
			bucket:  testBucket,
			summary: summaryWithChecksum("data-001.json.gz", "not base64!"),
			client:  &mockS3Client{etags: map[string]string{"data-001.json.gz": "abc"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewS3Loader(tt.client).VerifyChecksums(context.Background(), tt.bucket, tt.summary); err == nil {
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

// TestLoaderSendsTheCallersContext verifies every S3 call the loader makes carries the
// caller's context. Detaching from it would leave a shutting-down restore blocked on S3
// with no deadline and no cancellation.
func TestLoaderSendsTheCallersContext(t *testing.T) {
	const summaryKey = "AWSDynamoDB/01768385930622-efd1a093/manifest-summary.json"
	client := &mockS3Client{
		data: map[string][]byte{
			summaryKey:   []byte(`{"itemCount":1,"s3Bucket":"test-bucket","manifestFilesS3Key":"files.json"}`),
			"files.json": []byte(`{"dataFileS3Key":"data-001.json.gz","etag":"abc-1"}`),
		},
		etags: map[string]string{"data-001.json.gz": "abc-1"},
	}
	loader := NewS3Loader(client)

	ctx := context.WithValue(context.Background(), callerContextKey{}, true)
	summary, err := loader.Load(ctx, "s3://test-bucket/"+summaryKey)
	if err != nil {
		t.Fatalf("failed to load manifest: %v", err)
	}
	if _, err := loader.VerifyChecksums(ctx, testBucket, summary); err != nil {
		t.Fatalf("failed to verify checksums: %v", err)
	}

	if client.detached > 0 {
		t.Errorf("%d S3 calls were made outside the caller's context", client.detached)
	}
}

// TestLoadReadsBothManifestsBeforeClosingThem verifies the loader consumes each response
// body while it is still open. Closing one early costs the data files, which the restore
// would then report as an empty export rather than as a failure.
func TestLoadReadsBothManifestsBeforeClosingThem(t *testing.T) {
	const summaryKey = "AWSDynamoDB/01768385930622-efd1a093/manifest-summary.json"
	client := &mockS3Client{data: map[string][]byte{
		summaryKey: []byte(`{"itemCount":3,"manifestFilesS3Key":"files.json"}`),
		"files.json": []byte(`{"dataFileS3Key":"data-001.json.gz"}
{"dataFileS3Key":"data-002.json.gz"}`),
	}}

	summary, err := NewS3Loader(client).Load(context.Background(), "s3://test-bucket/"+summaryKey)
	if err != nil {
		t.Fatalf("failed to load manifest: %v", err)
	}
	if summary.ItemCount != 3 {
		t.Errorf("item count = %d, want 3", summary.ItemCount)
	}
	if len(summary.DataFiles) != 2 {
		t.Errorf("expected both data files parsed, got %v", summary.DataFiles)
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
