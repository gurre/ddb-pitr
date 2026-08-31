// Package manifest implements the manifest loading and verification as specified in section 4.3
// of the design specification. It handles loading and validating the DynamoDB PITR export
// manifest files from S3.
package manifest

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	json "github.com/goccy/go-json"
	"github.com/gurre/ddb-pitr/aws"
)

// s3URIPattern is compiled once at package level to avoid recompilation per call.
var s3URIPattern = regexp.MustCompile(`^s3://([^/]+)/(.+)$`)

// Summary contains the export metadata as defined in section 4.3 of the spec.
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

// FileMeta contains metadata for a single data file as defined in section 4.3.
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
//	result, err := loader.VerifyChecksums(ctx, summary)
type Loader interface {
	Load(ctx context.Context, manifestS3URI string) (Summary, error)
	VerifyChecksums(ctx context.Context, summary Summary) (Verification, error)
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

// Load implements the manifest loading requirements from section 4.3.
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

	if err := json.NewDecoder(resp.Body).Decode(&summary); err != nil {
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

// VerifyChecksums implements the checksum verification requirements from section 4.3.
// It reports an error only when a data file demonstrably differs from what the manifest
// recorded; files it has nothing to compare against come back in Unverified.
//
// The manifest records both the ETag S3 reported at export time and the object's MD5.
// The ETag is compared first because it is the only one that works for a file S3 stored
// in parts, whose ETag is a digest of the parts' digests rather than of the object.
//
// Example:
//
//	loader := manifest.NewS3Loader(client)
//	summary, err := loader.Load(ctx, manifestURI)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	result, err := loader.VerifyChecksums(ctx, summary)
//	if err != nil {
//	    log.Fatal("Checksum verification failed:", err)
//	}
//	fmt.Printf("%d verified, %d unverifiable\n", result.Verified, len(result.Unverified))
func (l *S3Loader) VerifyChecksums(ctx context.Context, summary Summary) (Verification, error) {
	// We need the bucket for HeadObject operations
	if summary.S3Bucket == "" {
		return Verification{}, fmt.Errorf("no S3 bucket specified in summary")
	}
	bucket := summary.S3Bucket
	result := Verification{Unverified: make([]string, 0)}

	for _, file := range summary.DataFiles {
		// Get the object metadata from S3 using HeadObject
		key := file.Key
		resp, err := l.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
		if err != nil {
			return Verification{}, fmt.Errorf("failed to get metadata for data file %s: %w", file.Key, err)
		}

		if resp.ETag == nil {
			return Verification{}, fmt.Errorf("ETag is nil for data file %s", file.Key)
		}

		// Some S3 implementations quote the ETag and some do not.
		etag := strings.Trim(*resp.ETag, `"`)

		if recorded := strings.Trim(file.ETag, `"`); recorded != "" {
			if etag != recorded {
				return Verification{}, fmt.Errorf("ETag mismatch for data file %s: manifest recorded %s, S3 reports %s",
					file.Key, recorded, etag)
			}
			result.Verified++
			continue
		}

		// Without a recorded ETag the object's MD5 is all that is left, and it cannot
		// speak for an object S3 assembled from parts.
		if file.MD5Base64 == "" || isMultipartETag(etag) {
			result.Unverified = append(result.Unverified, file.Key)
			continue
		}

		// Convert expected MD5 from Base64 to Hex
		md5Bytes, err := base64.StdEncoding.DecodeString(file.MD5Base64)
		if err != nil {
			return Verification{}, fmt.Errorf("failed to decode MD5 Base64 for data file %s: %w", file.Key, err)
		}
		expectedMD5Hex := fmt.Sprintf("%x", md5Bytes)

		if etag != expectedMD5Hex {
			return Verification{}, fmt.Errorf("checksum mismatch for data file %s: expected %s, got %s",
				file.Key, expectedMD5Hex, etag)
		}
		result.Verified++
	}

	return result, nil
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
