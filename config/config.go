// Package config implements the configuration management as specified in section 4.1
// of the design specification. It handles parsing and validation of all restore operation
// parameters.
package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Config holds all configuration for the restore operation as defined in section 4.1
// of the design specification. All fields correspond to the required configuration
// parameters for the restore operation.
type Config struct {
	TableName       string        // Target DynamoDB table name
	ExportS3URI     string        // S3 URI for the PITR export (s3://bucket/prefix)
	ExportType      string        // "FULL"|"INCREMENTAL" - matches DynamoDB export types
	ViewType        string        // "NEW"|"NEW_AND_OLD" - matches DynamoDB view types
	Region          string        // AWS region for the operation
	ResumeKey       string        // S3 URI for checkpoint file (s3://bucket/key)
	MaxWorkers      int           // Maximum number of concurrent workers
	ReadAheadParts  int           // Number of S3 parts to read ahead
	BatchSize       int           // Batch size for DynamoDB writes (≤25)
	ReportS3URI     string        // S3 URI for the final report
	DryRun          bool          // If true, don't actually write to DynamoDB
	ManageCapacity  bool          // If true, manage table capacity
	ShutdownTimeout time.Duration // Graceful shutdown timeout

	// Internal fields
	exportBucketName string // Bucket name parsed from ExportS3URI
}

// GetExportBucketName returns the bucket name parsed from ExportS3URI
func (c *Config) GetExportBucketName() string {
	return c.exportBucketName
}

// Validate implements the validation requirements from section 4.1 of the spec.
// It ensures all required fields are present and have valid values.
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

	// Parse the ExportS3URI to extract the bucket name
	u, err := url.Parse(c.ExportS3URI)
	if err != nil {
		return fmt.Errorf("invalid export S3 URI: %w", err)
	}
	if u.Scheme != "s3" {
		return fmt.Errorf("export S3 URI must use s3 scheme")
	}
	c.exportBucketName = u.Host

	if c.ExportType != "FULL" && c.ExportType != "INCREMENTAL" {
		return fmt.Errorf("export type must be FULL or INCREMENTAL")
	}

	if c.ViewType != "NEW" && c.ViewType != "NEW_AND_OLD" {
		return fmt.Errorf("view type must be NEW or NEW_AND_OLD")
	}

	if c.Region == "" {
		return fmt.Errorf("region is required")
	}

	if c.MaxWorkers < 1 {
		return fmt.Errorf("max workers must be at least 1")
	}

	if c.ReadAheadParts < 1 {
		return fmt.Errorf("read ahead parts must be at least 1")
	}

	if c.BatchSize < 1 || c.BatchSize > 25 {
		return fmt.Errorf("batch size must be between 1 and 25")
	}

	if c.ReportS3URI != "" && !strings.HasPrefix(c.ReportS3URI, "s3://") {
		return fmt.Errorf("report S3 URI must start with s3://")
	}

	if c.ShutdownTimeout < time.Second {
		return fmt.Errorf("shutdown timeout must be at least 1 second")
	}

	return nil
}
