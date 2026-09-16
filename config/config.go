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

// checkpointPrefix is where a restore records its progress when it was not told where.
// It sits outside the export's own directory, so a restore leaves the export exactly as
// DynamoDB wrote it.
const checkpointPrefix = "ddb-pitr/checkpoints"

// Config holds all configuration for the restore operation.
// Fields are ordered largest-to-smallest for memory alignment.
type Config struct {
	TableName       string        // Target DynamoDB table name
	ExportS3URI     string        // S3 URI of the export's manifest-summary.json, or of the directory holding it
	Region          string        // AWS region; empty means whatever the AWS environment resolves
	ResumeKey       string        // S3 URI of the checkpoint; empty means the one CheckpointURI derives
	ReportS3URI     string        // S3 URI for the final report
	ShutdownTimeout time.Duration // How long an interrupted restore has to record where it stopped
	MaxWorkers      int           // Maximum number of concurrent workers
	BatchSize       int           // Batch size for DynamoDB writes (≤25)
	DryRun          bool          // If true, don't actually write to DynamoDB
	NoResume        bool          // If true, record no progress; an interrupted restore starts over
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

// exportName is the last named segment of the directory the export's manifest sits in,
// which for an export DynamoDB wrote is its export id. It is the empty string for a
// manifest at the root of a bucket, which names no export. Empty segments are stepped
// over rather than returned, so a URI typed with a doubled slash names the same export
// as the same URI without one.
func (c *Config) exportName() string {
	u, err := url.Parse(c.ManifestURI())
	if err != nil {
		return ""
	}
	segments := strings.Split(strings.Trim(u.Path, "/"), "/")
	// The last segment is the manifest itself; the export is the directory holding it.
	for i := len(segments) - 2; i >= 0; i-- {
		if segments[i] != "" {
			return segments[i]
		}
	}
	return ""
}

// CheckpointURI returns where the restore records its progress: the URI given with
// --resume, or one in the export's own bucket, so a restore is resumable without having
// been asked to be. The empty string means progress is not recorded at all, which is
// what --no-resume asks for and what a dry run always gets, since a later real restore
// must not skip work that was only measured.
//
// A derived key names the export and the target table together, because progress only
// means anything for that pairing: one export restored into two tables is two restores,
// and a shared checkpoint would let the second skip what the first finished.
// Example:
//
//	cfg := &config.Config{TableName: "orders", ExportS3URI: "s3://backups/AWSDynamoDB/01234567890-abcdef"}
//	cfg.CheckpointURI() // "s3://backups/ddb-pitr/checkpoints/01234567890-abcdef.orders.json"
func (c *Config) CheckpointURI() string {
	if c.NoResume || c.DryRun {
		return ""
	}
	if c.ResumeKey != "" {
		return c.ResumeKey
	}
	bucket := c.GetExportBucketName()
	if bucket == "" {
		return ""
	}
	if name := c.exportName(); name != "" {
		return fmt.Sprintf("s3://%s/%s/%s.%s.json", bucket, checkpointPrefix, name, c.TableName)
	}
	return fmt.Sprintf("s3://%s/%s/%s.json", bucket, checkpointPrefix, c.TableName)
}

// validateObjectURI checks a URI that names one S3 object, the way the checkpoint and
// the report do. An empty URI is the flag being left out, which is allowed for both.
func validateObjectURI(flag, uri string) error {
	if uri == "" {
		return nil
	}
	if !strings.HasPrefix(uri, "s3://") {
		return fmt.Errorf("%s S3 URI must start with s3://", flag)
	}
	u, err := url.Parse(uri)
	if err != nil {
		return fmt.Errorf("invalid %s S3 URI: %w", flag, err)
	}
	if u.Host == "" || strings.Trim(u.Path, "/") == "" {
		return fmt.Errorf("%s S3 URI must name a bucket and a key: %s", flag, uri)
	}
	return nil
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

	// The checkpoint and the report are single objects, so each URI has to name a key as
	// well as a bucket. A URI naming only a bucket would otherwise be found out by the
	// request that used it: the first checkpoint load, or the upload at the very end of a
	// restore, which is the worst moment to learn the report has nowhere to go.
	if err := validateObjectURI("resume", c.ResumeKey); err != nil {
		return err
	}
	// The two ask for opposite things, and guessing which the operator meant is the one
	// answer that could quietly restart a restore that was supposed to resume.
	if c.ResumeKey != "" && c.NoResume {
		return fmt.Errorf("--resume names a checkpoint and --no-resume asks for none; pass one or the other")
	}
	if err := validateObjectURI("report", c.ReportS3URI); err != nil {
		return err
	}

	if c.ShutdownTimeout < time.Second {
		return fmt.Errorf("shutdown timeout must be at least 1 second")
	}

	return nil
}
