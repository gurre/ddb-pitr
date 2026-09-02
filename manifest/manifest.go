// Package manifest loads a DynamoDB export's manifests from S3 and verifies the data
// files they list before a restore reads them.
package manifest

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	json "github.com/goccy/go-json"
	"github.com/gurre/ddb-pitr/aws"
)

// s3URIPattern is compiled once at package level to avoid recompilation per call.
var s3URIPattern = regexp.MustCompile(`^s3://([^/]+)/(.+)$`)

// Summary is what the export's manifests say about it: the summary manifest's fields
// and the data files the files manifest lists.
// Example:
//
//	loader := manifest.NewS3Loader(client)
//	summary, err := loader.Load(ctx, "s3://my-bucket/AWSDynamoDB/123456789012-cc964122/manifest-summary.json")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Export contains %d items\n", summary.ItemCount)
type Summary struct {
	// Fields from manifest-summary.json
	Version            string `json:"version"`            // Format version
	ExportARN          string `json:"exportArn"`          // ARN of the export
	StartTime          string `json:"startTime"`          // Start time of the export
	EndTime            string `json:"endTime"`            // End time of the export
	TableARN           string `json:"tableArn"`           // ARN of the table
	TableID            string `json:"tableId"`            // ID of the table
	ExportTime         string `json:"exportTime"`         // Time of the export (full export)
	ExportFromTime     string `json:"exportFromTime"`     // Start time for incremental export
	ExportToTime       string `json:"exportToTime"`       // End time for incremental export
	S3Bucket           string `json:"s3Bucket"`           // S3 bucket containing the export
	S3Prefix           string `json:"s3Prefix"`           // S3 prefix for the export
	S3SseAlgorithm     string `json:"s3SseAlgorithm"`     // S3 SSE algorithm
	S3SseKmsKeyID      string `json:"s3SseKmsKeyId"`      // S3 SSE KMS key ID
	ManifestFilesS3Key string `json:"manifestFilesS3Key"` // S3 key for manifest files
	BilledSizeBytes    int64  `json:"billedSizeBytes"`    // Size in bytes that was billed
	ItemCount          int64  `json:"itemCount"`          // Total number of items in the export
	OutputFormat       string `json:"outputFormat"`
	OutputView         string `json:"outputView"` // View type for incremental exports
	ExportType         string `json:"exportType"` // Export type field in newer manifest format

	// Parsed from manifest-files.json
	DataFiles []FileMeta // List of data files in the export
}

// FileMeta is one data file as the files manifest describes it.
// Example:
//
//	for _, file := range summary.DataFiles {
//	    fmt.Printf("File: %s, Items: %d\n", file.Key, file.ItemCount)
//	}
type FileMeta struct {
	Key       string `json:"dataFileS3Key"` // S3 key of the data file (new format uses dataFileS3Key)
	ETag      string `json:"etag"`          // S3 ETag for integrity verification
	MD5Base64 string `json:"md5Checksum"`   // Base64-encoded MD5 checksum
	ItemCount int64  `json:"itemCount"`     // Number of items in this file
}

// Verification reports what checksum verification established. Files the manifest
// recorded nothing comparable for are listed rather than counted as good, so a caller
// can tell "checked and matched" from "nothing to check against".
// Fields are ordered largest-to-smallest for memory alignment.
type Verification struct {
	Unverified []string // Keys of data files verification could not establish either way
	Verified   int      // Data files whose S3 object matched what the manifest recorded
}

// Loader interface defines the contract for loading and verifying manifest files.
// Example:
//
//	var loader manifest.Loader
//	summary, err := loader.Load(ctx, "s3://my-bucket/AWSDynamoDB/123456789012-cc964122/manifest-summary.json")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	result, err := loader.VerifyChecksums(ctx, "my-bucket", summary)
type Loader interface {
	Load(ctx context.Context, manifestS3URI string) (Summary, error)
	VerifyChecksums(ctx context.Context, bucket string, summary Summary) (Verification, error)
}

// S3Loader implements the Loader interface using AWS S3.
// Example:
//
//	client := s3.NewFromConfig(cfg)
//	loader := manifest.NewS3Loader(client)
//	summary, err := loader.Load(ctx, "s3://my-bucket/AWSDynamoDB/123456789012-cc964122/manifest-summary.json")
type S3Loader struct {
	client aws.S3Client
}

// NewS3Loader creates a new S3Loader instance.
// Example:
//
//	client := s3.NewFromConfig(cfg)
//	loader := manifest.NewS3Loader(client)
func NewS3Loader(client aws.S3Client) *S3Loader {
	return &S3Loader{client: client}
}

// Load reads the summary manifest at the given URI and the files manifest it points to.
// Example:
//
//	loader := manifest.NewS3Loader(client)
//	summary, err := loader.Load(ctx, "s3://my-bucket/AWSDynamoDB/123456789012-cc964122/manifest-summary.json")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Found %d data files\n", len(summary.DataFiles))
func (l *S3Loader) Load(ctx context.Context, manifestS3URI string) (Summary, error) {
	var summary Summary

	// Extract bucket from the manifestS3URI using regex
	bucket, err := extractBucketFromS3URI(manifestS3URI)
	if err != nil {
		return Summary{}, err
	}

	// Extract the S3 key from the URI
	s3Key, err := extractKeyFromS3URI(manifestS3URI)
	if err != nil {
		return Summary{}, err
	}

	// Load manifest-summary.json
	resp, err := l.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &s3Key,
	})
	if err != nil {
		return Summary{}, fmt.Errorf("failed to get manifest summary: %w", err)
	}
	if resp.Body == nil {
		return Summary{}, fmt.Errorf("manifest summary response body is nil")
	}
	defer func() { _ = resp.Body.Close() }()

	if err = json.NewDecoder(resp.Body).Decode(&summary); err != nil {
		return Summary{}, fmt.Errorf("failed to decode manifest summary: %w", err)
	}

	// Load manifest-files.json
	filesResp, err := l.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &summary.ManifestFilesS3Key,
	})
	if err != nil {
		return Summary{}, fmt.Errorf("failed to get manifest files: %w", err)
	}
	if filesResp.Body == nil {
		return Summary{}, fmt.Errorf("manifest files response body is nil")
	}
	defer func() { _ = filesResp.Body.Close() }()

	// The manifest-files.json format has changed to use dataFileS3Key instead of Key.
	// Preallocate slice with estimated capacity based on typical export patterns.
	// Most exports have dozens to hundreds of files, so 64 is a reasonable default.
	decoder := json.NewDecoder(filesResp.Body)
	summary.DataFiles = make([]FileMeta, 0, 64)
	for {
		var file FileMeta
		if err := decoder.Decode(&file); err == io.EOF {
			break
		} else if err != nil {
			return Summary{}, fmt.Errorf("failed to decode manifest file entry: %w", err)
		}
		summary.DataFiles = append(summary.DataFiles, file)
	}

	return summary, nil
}

// verifyParallelism bounds how many data files are checked at once. Verification is
// one HeadObject per file, and an export can list tens of thousands of files.
const verifyParallelism = 16

// VerifyChecksums checks every data file the manifest lists against the object in the
// given bucket, which must be the bucket the restore is going to read from. The bucket
// the manifest itself names is where the export was written, and an export that has
// since been copied elsewhere would otherwise be verified against objects the restore
// never reads.
//
// Each file is decided by the first of these that applies: the ETag S3 reports equals
// the one the manifest recorded; the manifest recorded an MD5 and S3's ETag is a
// single-part one, so the two are compared directly; the manifest recorded an MD5 but
// S3's ETag is a multipart digest that cannot be compared, so the object is read and
// its MD5 computed. A copied object loses the ETag the export recorded, and the MD5
// is what still identifies it. An error is returned only when a file demonstrably
// differs from what the manifest recorded. A file the manifest recorded nothing
// comparable for, which a real export never produces, comes back in Unverified.
//
// Example:
//
//	loader := manifest.NewS3Loader(client)
//	summary, err := loader.Load(ctx, manifestURI)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	result, err := loader.VerifyChecksums(ctx, "my-bucket", summary)
//	if err != nil {
//	    log.Fatal("Checksum verification failed:", err)
//	}
//	fmt.Printf("%d verified, %d unverifiable\n", result.Verified, len(result.Unverified))
func (l *S3Loader) VerifyChecksums(ctx context.Context, bucket string, summary Summary) (Verification, error) {
	if bucket == "" {
		return Verification{}, fmt.Errorf("no bucket to verify the export against")
	}

	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		result   = Verification{Unverified: make([]string, 0)}
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
		slots    = make(chan struct{}, verifyParallelism)
	)
	for _, file := range summary.DataFiles {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(file FileMeta) {
			defer wg.Done()
			defer func() { <-slots }()
			verified, err := l.verifyFile(ctx, bucket, file)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				if firstErr == nil {
					firstErr = err
				}
				cancel()
			case verified:
				result.Verified++
			default:
				result.Unverified = append(result.Unverified, file.Key)
			}
		}(file)
	}
	wg.Wait()

	if firstErr != nil {
		return Verification{}, firstErr
	}
	// Stopped by the caller before every file was examined: a partial count must not
	// be taken for a verified export.
	if err := parent.Err(); err != nil {
		return Verification{}, err
	}
	sort.Strings(result.Unverified)
	return result, nil
}

// verifyFile decides one data file. It reports true when the object matches what the
// manifest recorded, false when the manifest recorded nothing comparable, and an
// error when the object demonstrably differs or could not be examined.
func (l *S3Loader) verifyFile(ctx context.Context, bucket string, file FileMeta) (bool, error) {
	key := file.Key
	resp, err := l.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return false, fmt.Errorf("failed to get metadata for data file %s: %w", file.Key, err)
	}
	if resp.ETag == nil {
		return false, fmt.Errorf("ETag is nil for data file %s", file.Key)
	}

	// Some S3 implementations quote the ETag and some do not.
	etag := strings.Trim(*resp.ETag, `"`)
	recorded := strings.Trim(file.ETag, `"`)
	if recorded != "" && etag == recorded {
		return true, nil
	}

	if file.MD5Base64 == "" {
		if recorded == "" {
			return false, nil
		}
		return false, fmt.Errorf("ETag mismatch for data file %s: manifest recorded %s, S3 reports %s",
			file.Key, recorded, etag)
	}
	md5Bytes, err := base64.StdEncoding.DecodeString(file.MD5Base64)
	if err != nil {
		return false, fmt.Errorf("failed to decode MD5 Base64 for data file %s: %w", file.Key, err)
	}
	expectedMD5Hex := fmt.Sprintf("%x", md5Bytes)

	// A single-part ETag is the object's MD5, except for an object encrypted with a KMS
	// or customer key, whose ETag is opaque. A multipart ETag is a digest of the parts'
	// digests. In both cases only reading the object can tell whether it matches.
	actualMD5Hex := etag
	if isMultipartETag(etag) || isOpaqueETag(resp) {
		actualMD5Hex, err = l.contentMD5(ctx, bucket, key)
		if err != nil {
			return false, fmt.Errorf("failed to read data file %s to verify it: %w", file.Key, err)
		}
	}
	if actualMD5Hex != expectedMD5Hex {
		return false, fmt.Errorf("checksum mismatch for data file %s: expected %s, got %s",
			file.Key, expectedMD5Hex, actualMD5Hex)
	}
	return true, nil
}

// contentMD5 reads an object and returns the hex MD5 of its bytes.
func (l *S3Loader) contentMD5(ctx context.Context, bucket, key string) (string, error) {
	resp, err := l.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return "", err
	}
	if resp.Body == nil {
		return "", fmt.Errorf("response body is nil")
	}
	defer func() { _ = resp.Body.Close() }()

	digest := md5.New()
	if _, err := io.Copy(digest, resp.Body); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", digest.Sum(nil)), nil
}

// isOpaqueETag reports whether the object is encrypted in a way that stops its ETag
// being its MD5: server-side encryption with a KMS key, or with a customer-provided key.
func isOpaqueETag(resp *s3.HeadObjectOutput) bool {
	switch resp.ServerSideEncryption {
	case s3types.ServerSideEncryptionAwsKms, s3types.ServerSideEncryptionAwsKmsDsse:
		return true
	}
	return resp.SSECustomerAlgorithm != nil
}

// isMultipartETag reports whether S3 built this ETag from a multipart upload, which
// makes it an MD5 of the parts' MD5s followed by the part count. A single-part ETag is
// plain hex, so the hyphen the part count is joined by is what tells them apart.
func isMultipartETag(etag string) bool {
	return strings.Contains(etag, "-")
}

// extractBucketFromS3URI extracts the bucket name from an S3 URI.
// Uses package-level compiled regex for efficiency.
func extractBucketFromS3URI(uri string) (string, error) {
	matches := s3URIPattern.FindStringSubmatch(uri)

	if len(matches) != 3 {
		return "", fmt.Errorf("invalid S3 URI format: %s (must be s3://bucket/key)", uri)
	}

	// matches[0] is the full match, matches[1] is the bucket
	return matches[1], nil
}

// extractKeyFromS3URI extracts the key from an S3 URI.
// Uses package-level compiled regex for efficiency.
func extractKeyFromS3URI(uri string) (string, error) {
	matches := s3URIPattern.FindStringSubmatch(uri)

	if len(matches) != 3 {
		return "", fmt.Errorf("invalid S3 URI format: %s (must be s3://bucket/key)", uri)
	}

	// matches[0] is the full match, matches[2] is the key
	return matches[2], nil
}
