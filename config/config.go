// Package config holds what a restore was asked to do and checks it before any AWS
// call is made, so a mistake in the invocation fails at the start rather than deep in
// the run.
package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// manifestSummaryName is the file every export writes its summary to.
const manifestSummaryName = "manifest-summary.json"

// Config holds all configuration for the restore operation.
// Fields are ordered largest-to-smallest for memory alignment.
type Config struct {
	TableName       string        // Target DynamoDB table name
	ExportS3URI     string        // S3 URI of the export's manifest-summary.json, or of the directory holding it
	Region          string        // AWS region; empty means whatever the AWS environment resolves
	ResumeKey       string        // S3 URI for checkpoint file (s3://bucket/key)
	ReportS3URI     string        // S3 URI for the final report
	ShutdownTimeout time.Duration // How long an interrupted restore has to finish in-flight writes and save its checkpoint
	MaxWorkers      int           // Maximum number of concurrent workers
	BatchSize       int           // Batch size for DynamoDB writes (≤25)
	DryRun          bool          // If true, don't actually write to DynamoDB
}

// GetExportBucketName returns the bucket the export URI names, or the empty string
// for a URI Validate rejects.
func (c *Config) GetExportBucketName() string {
	u, err := url.Parse(c.ExportS3URI)
	if err != nil || u.Scheme != "s3" {
		return ""
	}
	return u.Host
}

// ManifestURI returns the S3 URI of the export's summary manifest. The export may be
// given as the manifest itself or as the directory holding it, which is what an
// operator copies from the console; either way this is what the restore reads first.
// Example:
//
//	cfg := &config.Config{ExportS3URI: "s3://my-bucket/AWSDynamoDB/01234567890-abcdef"}
//	cfg.ManifestURI() // "s3://my-bucket/AWSDynamoDB/01234567890-abcdef/manifest-summary.json"
func (c *Config) ManifestURI() string {
	if strings.HasSuffix(c.ExportS3URI, ".json") {
		return c.ExportS3URI
	}
	return strings.TrimSuffix(c.ExportS3URI, "/") + "/" + manifestSummaryName
}

// Validate checks the configuration is one a restore can run with. Every rule here
// fails an invocation that would otherwise fail later and less clearly.
func (c *Config) Validate() error {
	if c.TableName == "" {
		return fmt.Errorf("table name is required")
	}

	if c.ExportS3URI == "" {
		return fmt.Errorf("export S3 URI is required")
	}
	if !strings.HasPrefix(c.ExportS3URI, "s3://") {
		return fmt.Errorf("export S3 URI must start with s3://")
	}

	u, err := url.Parse(c.ExportS3URI)
	if err != nil {
		return fmt.Errorf("invalid export S3 URI: %w", err)
	}
	if u.Scheme != "s3" {
		return fmt.Errorf("export S3 URI must use s3 scheme")
	}
	// "s3://" and "s3:///key" carry no bucket. Accepting them leaves every later
	// request aimed at an empty bucket name, which fails far from its cause.
	if u.Host == "" {
		return fmt.Errorf("export S3 URI must name a bucket")
	}

	if c.MaxWorkers < 1 {
		return fmt.Errorf("max workers must be at least 1")
	}

	if c.BatchSize < 1 || c.BatchSize > 25 {
		return fmt.Errorf("batch size must be between 1 and 25")
	}

	if c.ResumeKey != "" && !strings.HasPrefix(c.ResumeKey, "s3://") {
		return fmt.Errorf("resume S3 URI must start with s3://")
	}

	if c.ReportS3URI != "" && !strings.HasPrefix(c.ReportS3URI, "s3://") {
		return fmt.Errorf("report S3 URI must start with s3://")
	}

	if c.ShutdownTimeout < time.Second {
		return fmt.Errorf("shutdown timeout must be at least 1 second")
	}

	return nil
}
