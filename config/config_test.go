package config

import (
	"strings"
	"testing"
	"time"
)

func validConfig() *Config {
	return &Config{
		TableName:       "test-table",
		ExportS3URI:     "s3://test-bucket/prefix",
		Region:          "us-west-2",
		MaxWorkers:      10,
		BatchSize:       25,
		ShutdownTimeout: time.Minute,
	}
}

func TestValidConfig(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected valid config to pass validation, got: %v", err)
	}
}

// TestRegionIsOptional verifies a configuration without a region is accepted. The AWS
// SDK resolves the region from the environment and profile the same way the AWS CLI
// does, and an operator with that set up should not have to repeat it.
func TestRegionIsOptional(t *testing.T) {
	cfg := validConfig()
	cfg.Region = ""
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected a configuration without a region to be accepted, got %v", err)
	}
}

// TestManifestURIAcceptsTheManifestOrItsDirectory verifies the export may be named by
// its summary manifest or by the directory holding it, with or without a trailing
// slash, since the directory is what an operator copies out of the S3 console.
func TestManifestURIAcceptsTheManifestOrItsDirectory(t *testing.T) {
	const want = "s3://my-bucket/AWSDynamoDB/01234567890-abcdef/manifest-summary.json"
	for _, uri := range []string{
		want,
		"s3://my-bucket/AWSDynamoDB/01234567890-abcdef/",
		"s3://my-bucket/AWSDynamoDB/01234567890-abcdef",
	} {
		cfg := &Config{ExportS3URI: uri}
		if got := cfg.ManifestURI(); got != want {
			t.Errorf("ManifestURI(%q) = %q, want %q", uri, got, want)
		}
	}
}

// TestGetExportBucketNameNeedsNoValidation verifies the bucket is read straight from
// the export URI, so it is the same whether or not Validate has run, and empty for a
// URI Validate would reject.
func TestGetExportBucketNameNeedsNoValidation(t *testing.T) {
	cfg := &Config{ExportS3URI: "s3://my-bucket/some/prefix"}
	if got := cfg.GetExportBucketName(); got != "my-bucket" {
		t.Errorf("expected bucket name 'my-bucket', got %q", got)
	}
	cfg.ExportS3URI = "https://my-bucket/some/prefix"
	if got := cfg.GetExportBucketName(); got != "" {
		t.Errorf("expected no bucket for a non-S3 URI, got %q", got)
	}
}

func TestMissingTableName(t *testing.T) {
	cfg := validConfig()
	cfg.TableName = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for missing table name")
	}
}

func TestMissingExportURI(t *testing.T) {
	cfg := validConfig()
	cfg.ExportS3URI = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for missing export URI")
	}
}

// TestInvalidS3URI verifies an export location that is not an S3 URI naming a bucket is
// rejected at startup. Every later request is addressed from this value, so accepting a
// bucketless or wrong-scheme URI turns one clear error into a confusing failure deep in
// the restore.
func TestInvalidS3URI(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
	}{
		{"http scheme", "http://bucket/key"},
		{"https scheme", "https://bucket/key"},
		{"no scheme", "bucket/key"},
		{"file scheme", "file:///path/to/file"},
		{"no bucket at all", "s3://"},
		{"empty bucket before the key", "s3:///some/prefix"},
		{"unparseable", "s3://bucket/%zz"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.ExportS3URI = tc.uri
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected error for invalid S3 URI: %s", tc.uri)
			}
		})
	}
}

// TestValidateReportsTheFirstProblem verifies each rule names what it rejected, so an
// operator fixing a configuration is told which field is wrong rather than that
// something is.
func TestValidateReportsTheFirstProblem(t *testing.T) {
	tests := []struct {
		name    string
		spoil   func(*Config)
		mention string
	}{
		{"table name", func(c *Config) { c.TableName = "" }, "table name"},
		{"export URI", func(c *Config) { c.ExportS3URI = "" }, "export S3 URI"},
		{"max workers", func(c *Config) { c.MaxWorkers = 0 }, "max workers"},
		{"batch size", func(c *Config) { c.BatchSize = 26 }, "batch size"},
		{"resume URI", func(c *Config) { c.ResumeKey = "/tmp/checkpoint.json" }, "resume S3 URI"},
		{"report URI", func(c *Config) { c.ReportS3URI = "http://bucket/report" }, "report S3 URI"},
		{"shutdown timeout", func(c *Config) { c.ShutdownTimeout = 0 }, "shutdown timeout"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.spoil(cfg)

			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected an error for an invalid %s", tt.name)
			}
			if !strings.Contains(err.Error(), tt.mention) {
				t.Errorf("error %q does not name %q", err, tt.mention)
			}
		})
	}
}

// TestExportURIIsCheckedInOrder verifies each export URI rule reports its own problem
// rather than a later one's. An operator who omitted the flag entirely should be told it
// is required, not that it must start with s3://.
func TestExportURIIsCheckedInOrder(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		mention string
	}{
		{"omitted entirely", "", "required"},
		{"wrong prefix", "https://bucket/key", "must start with s3://"},
		{"no bucket", "s3://", "bucket"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.ExportS3URI = tt.uri

			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected %q to be rejected", tt.uri)
			}
			if !strings.Contains(err.Error(), tt.mention) {
				t.Errorf("error %q does not mention %q", err, tt.mention)
			}
		})
	}
}

func TestInvalidMaxWorkers(t *testing.T) {
	testCases := []int{0, -1, -100}
	for _, workers := range testCases {
		t.Run("workers", func(t *testing.T) {
			cfg := validConfig()
			cfg.MaxWorkers = workers
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected error for invalid max workers: %d", workers)
			}
		})
	}
}

// TestValidMaxWorkers verifies a single worker is accepted. It is the smallest pool that
// can make progress, and rejecting it would force concurrency the operator did not ask for.
func TestValidMaxWorkers(t *testing.T) {
	for _, workers := range []int{1, 10, 100} {
		t.Run("workers", func(t *testing.T) {
			cfg := validConfig()
			cfg.MaxWorkers = workers
			if err := cfg.Validate(); err != nil {
				t.Errorf("expected valid max workers %d to pass, got: %v", workers, err)
			}
		})
	}
}

// TestValidShutdownTimeout verifies one second, the shortest allowed grace period, is
// accepted so an operator can ask for a fast shutdown.
func TestValidShutdownTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{time.Second, time.Minute} {
		t.Run(timeout.String(), func(t *testing.T) {
			cfg := validConfig()
			cfg.ShutdownTimeout = timeout
			if err := cfg.Validate(); err != nil {
				t.Errorf("expected valid shutdown timeout %s to pass, got: %v", timeout, err)
			}
		})
	}
}

func TestInvalidBatchSize(t *testing.T) {
	testCases := []int{0, -1, 26, 100}
	for _, size := range testCases {
		t.Run("size", func(t *testing.T) {
			cfg := validConfig()
			cfg.BatchSize = size
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected error for invalid batch size: %d", size)
			}
		})
	}
}

func TestValidBatchSizes(t *testing.T) {
	for _, size := range []int{1, 10, 25} {
		t.Run("size", func(t *testing.T) {
			cfg := validConfig()
			cfg.BatchSize = size
			if err := cfg.Validate(); err != nil {
				t.Errorf("expected valid batch size %d to pass, got: %v", size, err)
			}
		})
	}
}

func TestInvalidReportURI(t *testing.T) {
	testCases := []string{"http://bucket/report", "https://bucket/report", "file:///report"}
	for _, uri := range testCases {
		t.Run(uri, func(t *testing.T) {
			cfg := validConfig()
			cfg.ReportS3URI = uri
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected error for invalid report URI: %s", uri)
			}
		})
	}
}

func TestValidReportURI(t *testing.T) {
	cfg := validConfig()
	cfg.ReportS3URI = "s3://bucket/report.json"
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected valid report URI to pass, got: %v", err)
	}
}

func TestEmptyReportURI(t *testing.T) {
	cfg := validConfig()
	cfg.ReportS3URI = ""
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected empty report URI to pass (optional), got: %v", err)
	}
}

func TestInvalidShutdownTimeout(t *testing.T) {
	testCases := []time.Duration{0, 500 * time.Millisecond, -time.Second}
	for _, timeout := range testCases {
		t.Run("timeout", func(t *testing.T) {
			cfg := validConfig()
			cfg.ShutdownTimeout = timeout
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected error for invalid shutdown timeout: %v", timeout)
			}
		})
	}
}
