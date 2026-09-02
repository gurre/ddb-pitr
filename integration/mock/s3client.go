package mock

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	json "github.com/goccy/go-json"
)

// S3Client is a mock implementation of aws.S3Client interface for testing. Objects are
// addressed by exact bucket and key, as S3 addresses them; every export the fixtures
// hold is loaded under the bucket named by ExportBucket.
type S3Client struct {
	// Maps bucket/key to file content
	Files map[string][]byte
	// Maps bucket/key to metadata
	Metadata map[string]map[string]string
	// Maps bucket/key to ETags
	ETags map[string]*string
	// Base directory for test files
	TestDataDir string
	// Every Range header GetObject was asked for, in order
	ranges []string
	mu     sync.Mutex
}

// ExportBucket is the bucket the fixtures are loaded under.
const ExportBucket = "test-bucket"

// NewS3Client creates a new mock S3 client
func NewS3Client(testDataDir string) *S3Client {
	return &S3Client{
		Files:       make(map[string][]byte),
		Metadata:    make(map[string]map[string]string),
		ETags:       make(map[string]*string),
		TestDataDir: testDataDir,
	}
}

// LoadTestFiles loads files from the test data directory.
// Loads all three export directories and the shared data directory.
func (m *S3Client) LoadTestFiles() error {
	if _, err := os.Stat(m.TestDataDir); os.IsNotExist(err) {
		return fmt.Errorf("test data directory does not exist: %s", m.TestDataDir)
	}

	// All export directories to load
	exportDirs := []string{
		"01768385930622-efd1a093", // FULL export (3 items)
		"01768386924000-d339e52d", // INCREMENTAL export (6 items)
		"01768388186000-4a2fc3ff", // INCREMENTAL export (5 items)
	}

	// Load each export directory
	for _, exportDir := range exportDirs {
		if err := m.loadExportDir(exportDir); err != nil {
			return fmt.Errorf("failed to load export %s: %w", exportDir, err)
		}
	}

	// Load shared data directory (AWSDynamoDB/data/)
	sharedDataDir := filepath.Join(m.TestDataDir, "AWSDynamoDB", "data")
	if err := m.loadDataDir(sharedDataDir); err != nil {
		return fmt.Errorf("failed to load shared data directory: %w", err)
	}

	// The ETags the manifests recorded are applied once every file is in place. Loading
	// a data file stamps a placeholder ETag on it, and exports share a data directory,
	// so applying them any earlier lets a later load overwrite them.
	for _, exportDir := range exportDirs {
		manifestFilesPath := filepath.Join(m.TestDataDir, "AWSDynamoDB", exportDir, "manifest-files.json")
		manifestFiles, err := os.ReadFile(manifestFilesPath)
		if err != nil {
			return fmt.Errorf("failed to read manifest files for %s: %w", exportDir, err)
		}
		if err := m.SetETags(manifestFiles); err != nil {
			return fmt.Errorf("failed to set ETags for %s: %w", exportDir, err)
		}
	}

	return nil
}

// loadExportDir loads manifest and data files for a single export directory.
func (m *S3Client) loadExportDir(exportDir string) error {
	// Load manifest summary
	manifestSummaryPath := filepath.Join(m.TestDataDir, "AWSDynamoDB", exportDir, "manifest-summary.json")
	manifestSummary, err := os.ReadFile(manifestSummaryPath)
	if err != nil {
		return fmt.Errorf("failed to read manifest summary: %w", err)
	}

	// Load manifest files
	manifestFilesPath := filepath.Join(m.TestDataDir, "AWSDynamoDB", exportDir, "manifest-files.json")
	manifestFiles, err := os.ReadFile(manifestFilesPath)
	if err != nil {
		return fmt.Errorf("failed to read manifest files: %w", err)
	}

	// Add manifests to mock S3
	m.addFile(ExportBucket, fmt.Sprintf("AWSDynamoDB/%s/manifest-summary.json", exportDir), manifestSummary)
	m.addFile(ExportBucket, fmt.Sprintf("AWSDynamoDB/%s/manifest-files.json", exportDir), manifestFiles)

	// Load export-local data directory if it exists
	localDataDir := filepath.Join(m.TestDataDir, "AWSDynamoDB", exportDir, "data")
	if err := m.loadDataDir(localDataDir); err != nil {
		// Not an error if data dir doesn't exist (some exports use shared data dir)
		if !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

// loadDataDir loads all .json.gz files from a data directory.
func (m *S3Client) loadDataDir(dataDir string) error {
	stat, err := os.Stat(dataDir)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return nil
	}

	return filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasSuffix(path, ".json.gz") {
			rel, err := filepath.Rel(m.TestDataDir, path)
			if err != nil {
				return err
			}

			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}

			m.addFile(ExportBucket, rel, data)
		}
		return nil
	})
}

// addFile helper to add a file to the mock storage
func (m *S3Client) addFile(bucket, key string, content []byte) {
	bucketKey := fmt.Sprintf("%s/%s", bucket, key)
	m.Files[bucketKey] = content

	// Setup metadata
	m.Metadata[bucketKey] = map[string]string{
		"Content-Type": "application/json",
	}

	// Setup ETags
	if strings.Contains(key, ".json.gz") {
		// For data files, we'll use the actual ETag as provided in the manifest files
		// which will be set later by the LoadTestFiles function
		etag := fmt.Sprintf("\"%x\"", len(content))
		m.ETags[bucketKey] = aws.String(etag)
	} else {
		// For non-data files, generate an ETag
		etag := fmt.Sprintf("\"%x\"", len(content))
		m.ETags[bucketKey] = aws.String(etag)
	}
}

// SetETags sets ETags for data files based on manifest-files.json content
func (m *S3Client) SetETags(manifestFiles []byte) error {
	// Parse manifest files to extract ETags
	scanner := bufio.NewScanner(bytes.NewReader(manifestFiles))

	for scanner.Scan() {
		var file struct {
			ETag          string `json:"etag"`
			DataFileS3Key string `json:"dataFileS3Key"`
		}

		if err := json.Unmarshal(scanner.Bytes(), &file); err != nil {
			return fmt.Errorf("failed to parse manifest file entry: %w", err)
		}

		if file.DataFileS3Key != "" && file.ETag != "" {
			bucketKey := fmt.Sprintf("%s/%s", ExportBucket, file.DataFileS3Key)
			// Store the ETag with quotes
			m.ETags[bucketKey] = aws.String(fmt.Sprintf("\"%s\"", file.ETag))
		}
	}

	return scanner.Err()
}

// rangePattern is the one Range form the streamer emits: a closed byte range.
var rangePattern = regexp.MustCompile(`^bytes=(\d+)-(\d+)$`)

// GetObject implements the S3Client interface for reading objects. A Range header is
// honoured the way S3 honours it: the body is the requested slice, ContentLength is the
// slice's length and ContentRange names it. A range starting past the end is refused,
// as S3 refuses it with 416. Without this the mock is more forgiving than S3: a ranged
// read would receive the whole object, and a streamer reading in chunks would silently
// duplicate every byte after the first chunk.
func (m *S3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	bucketKey := fmt.Sprintf("%s/%s", *params.Bucket, *params.Key)
	content, ok := m.Files[bucketKey]
	if !ok {
		return nil, &types.NoSuchKey{
			Message: aws.String(fmt.Sprintf("The specified key does not exist: %s", bucketKey)),
		}
	}

	metadata := m.Metadata[bucketKey]
	if metadata == nil {
		metadata = make(map[string]string)
	}

	start, end := int64(0), int64(len(content))-1
	var contentRange *string
	if params.Range != nil {
		m.ranges = append(m.ranges, *params.Range)
		match := rangePattern.FindStringSubmatch(*params.Range)
		if match == nil {
			return nil, fmt.Errorf("mock S3: unsupported Range %q", *params.Range)
		}
		start, _ = strconv.ParseInt(match[1], 10, 64)
		end, _ = strconv.ParseInt(match[2], 10, 64)
		if start >= int64(len(content)) || start > end {
			return nil, &types.InvalidObjectState{
				Message: aws.String(fmt.Sprintf("InvalidRange: %s is outside %d bytes", *params.Range, len(content))),
			}
		}
		if end >= int64(len(content)) {
			end = int64(len(content)) - 1
		}
		contentRange = aws.String(fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
	}

	body := content
	if len(content) > 0 {
		body = content[start : end+1]
	}
	contentLength := int64(len(body))

	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(body)),
		Metadata:      metadata,
		ETag:          m.ETags[bucketKey],
		ContentLength: &contentLength,
		ContentRange:  contentRange,
	}, nil
}

// Ranges returns every Range header GetObject was asked for, in order, so a test can
// prove a ranged read covered an object exactly once.
func (m *S3Client) Ranges() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.ranges...)
}

// PutObject implements the S3Client interface for writing objects
func (m *S3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	bucketKey := fmt.Sprintf("%s/%s", *params.Bucket, *params.Key)

	// Read the entire body
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.Files[bucketKey] = data

	// Set up metadata
	if params.Metadata != nil {
		m.Metadata[bucketKey] = params.Metadata
	} else {
		m.Metadata[bucketKey] = make(map[string]string)
	}

	// Set ETag
	etag := fmt.Sprintf("\"%x\"", len(data))
	m.ETags[bucketKey] = aws.String(etag)

	return &s3.PutObjectOutput{
		ETag: aws.String(etag),
	}, nil
}

// HeadObject implements the S3Client interface for retrieving object metadata. Only the
// exact bucket and key are consulted: an object is either there or it is not, so a test
// that names the wrong bucket learns so instead of being served a same-named object
// from another.
func (m *S3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	bucketKey := fmt.Sprintf("%s/%s", *params.Bucket, *params.Key)
	content, ok := m.Files[bucketKey]
	if !ok {
		return nil, &types.NotFound{
			Message: aws.String(fmt.Sprintf("The specified key does not exist: %s", bucketKey)),
		}
	}

	contentLength := int64(len(content))

	return &s3.HeadObjectOutput{
		ETag:          m.ETags[bucketKey],
		Metadata:      m.Metadata[bucketKey],
		ContentLength: &contentLength,
	}, nil
}

// CreateMultipartUpload is a stub implementation for the s3streamer.S3Client interface
func (m *S3Client) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return nil, fmt.Errorf("CreateMultipartUpload not implemented in mock")
}

// UploadPart is a stub implementation for the s3streamer.S3Client interface
func (m *S3Client) UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return nil, fmt.Errorf("UploadPart not implemented in mock")
}

// CompleteMultipartUpload is a stub implementation for the s3streamer.S3Client interface
func (m *S3Client) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return nil, fmt.Errorf("CompleteMultipartUpload not implemented in mock")
}

// AbortMultipartUpload is a stub implementation for the s3streamer.S3Client interface
func (m *S3Client) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return nil, fmt.Errorf("AbortMultipartUpload not implemented in mock")
}
