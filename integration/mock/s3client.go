package mock

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	json "github.com/goccy/go-json"
)

// S3Client is a mock implementation of aws.S3Client interface for testing
type S3Client struct {
	// Maps bucket/key to file content
	Files map[string][]byte
	// Maps bucket/key to metadata
	Metadata map[string]map[string]string
	// Maps bucket/key to ETags
	ETags map[string]*string
	// Base directory for test files
	TestDataDir string
}

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
	m.addFile("test-bucket", fmt.Sprintf("AWSDynamoDB/%s/manifest-summary.json", exportDir), manifestSummary)
	m.addFile("test-bucket", fmt.Sprintf("AWSDynamoDB/%s/manifest-files.json", exportDir), manifestFiles)

	// Set ETags from manifest
	if err := m.SetETags(manifestFiles); err != nil {
		fmt.Printf("Warning: Failed to set ETags from manifest %s: %v\n", exportDir, err)
	}

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

			m.addFile("test-bucket", rel, data)
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
			bucketKey := fmt.Sprintf("test-bucket/%s", file.DataFileS3Key)
			// Store the ETag with quotes
			m.ETags[bucketKey] = aws.String(fmt.Sprintf("\"%s\"", file.ETag))
			fmt.Printf("Set ETag for %s: %s\n", bucketKey, *m.ETags[bucketKey])
		}
	}

	return scanner.Err()
}

// GetObject implements the S3Client interface for reading objects
func (m *S3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	bucketKey := fmt.Sprintf("%s/%s", *params.Bucket, *params.Key)

	content, ok := m.Files[bucketKey]
	if !ok {
		// Try finding by suffix match if exact match fails
		for k, v := range m.Files {
			if strings.HasSuffix(k, *params.Key) {
				content = v
				bucketKey = k
				ok = true
				break
			}
		}

		if !ok {
			// For debugging
			fmt.Printf("Mock S3: Key not found: %s\n", bucketKey)
			fmt.Printf("Available keys: %v\n", m.listKeys())

			return nil, &types.NoSuchKey{
				Message: aws.String(fmt.Sprintf("The specified key does not exist: %s", *params.Key)),
			}
		}
	}

	metadata := m.Metadata[bucketKey]
	if metadata == nil {
		metadata = make(map[string]string)
	}

	contentLength := int64(len(content))

	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(content)),
		Metadata:      metadata,
		ETag:          m.ETags[bucketKey],
		ContentLength: &contentLength,
	}, nil
}

// PutObject implements the S3Client interface for writing objects
func (m *S3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	bucketKey := fmt.Sprintf("%s/%s", *params.Bucket, *params.Key)

	// Read the entire body
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}

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

// HeadObject implements the S3Client interface for retrieving object metadata
func (m *S3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	bucketKey := ""

	// If a bucket is provided, create the bucket/key format
	if params.Bucket != nil {
		bucketKey = fmt.Sprintf("%s/%s", *params.Bucket, *params.Key)
	}

	// Check if we have the file with this exact key
	content, ok := m.Files[bucketKey]

	// If not found by exact match, try to find by suffix
	if !ok {
		for k, v := range m.Files {
			if strings.HasSuffix(k, *params.Key) {
				content = v
				bucketKey = k
				ok = true
				break
			}
		}
	}

	// If still not found, look for the key in our available files
	if !ok {
		fmt.Printf("Mock S3 HeadObject: Key not found: %s\n", bucketKey)
		fmt.Printf("Available keys: %v\n", m.listKeys())
		return nil, &types.NoSuchKey{
			Message: aws.String(fmt.Sprintf("The specified key does not exist: %s", *params.Key)),
		}
	}

	contentLength := int64(len(content))

	// Ensure we have an ETag for this object
	if _, ok := m.ETags[bucketKey]; !ok {
		// Generate an ETag based on content length (simplified for testing)
		etag := fmt.Sprintf("\"%x\"", len(content))
		m.ETags[bucketKey] = aws.String(etag)
	}

	return &s3.HeadObjectOutput{
		ETag:          m.ETags[bucketKey],
		Metadata:      m.Metadata[bucketKey],
		ContentLength: &contentLength,
	}, nil
}

// listKeys returns a list of all keys in the mock S3 bucket (for debugging)
func (m *S3Client) listKeys() []string {
	var keys []string
	for k := range m.Files {
		keys = append(keys, k)
	}
	return keys
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
