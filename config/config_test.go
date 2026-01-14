package config

import (
	"testing"
	"time"
)

func validConfig() *Config {
	return &Config{
		TableName:       "test-table",
		ExportS3URI:     "s3://test-bucket/prefix",
		ExportType:      "FULL",
		ViewType:        "NEW",
		Region:          "us-west-2",
		MaxWorkers:      10,
		ReadAheadParts:  5,
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

func TestInvalidS3URI(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
	}{
		{"http scheme", "http://bucket/key"},
		{"https scheme", "https://bucket/key"},
		{"no scheme", "bucket/key"},
		{"file scheme", "file:///path/to/file"},
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

func TestInvalidExportType(t *testing.T) {
	testCases := []string{"full", "PARTIAL", "incremental", ""}
	for _, exportType := range testCases {
		t.Run(exportType, func(t *testing.T) {
			cfg := validConfig()
			cfg.ExportType = exportType
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected error for invalid export type: %s", exportType)
			}
		})
	}
}

func TestValidExportTypes(t *testing.T) {
	for _, exportType := range []string{"FULL", "INCREMENTAL"} {
		t.Run(exportType, func(t *testing.T) {
			cfg := validConfig()
			cfg.ExportType = exportType
			if err := cfg.Validate(); err != nil {
				t.Errorf("expected valid export type %s to pass, got: %v", exportType, err)
			}
		})
	}
}

func TestInvalidViewType(t *testing.T) {
	testCases := []string{"new", "OLD", "new_and_old", ""}
	for _, viewType := range testCases {
		t.Run(viewType, func(t *testing.T) {
			cfg := validConfig()
			cfg.ViewType = viewType
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected error for invalid view type: %s", viewType)
			}
		})
	}
}

func TestValidViewTypes(t *testing.T) {
	for _, viewType := range []string{"NEW", "NEW_AND_OLD"} {
		t.Run(viewType, func(t *testing.T) {
			cfg := validConfig()
			cfg.ViewType = viewType
			if err := cfg.Validate(); err != nil {
				t.Errorf("expected valid view type %s to pass, got: %v", viewType, err)
			}
		})
	}
}

func TestMissingRegion(t *testing.T) {
	cfg := validConfig()
	cfg.Region = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for missing region")
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

func TestInvalidReadAheadParts(t *testing.T) {
	testCases := []int{0, -1, -100}
	for _, parts := range testCases {
		t.Run("parts", func(t *testing.T) {
			cfg := validConfig()
			cfg.ReadAheadParts = parts
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected error for invalid read ahead parts: %d", parts)
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

func TestGetExportBucketName(t *testing.T) {
	cfg := validConfig()
	cfg.ExportS3URI = "s3://my-bucket/some/prefix"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validation failed: %v", err)
	}
	if got := cfg.GetExportBucketName(); got != "my-bucket" {
		t.Errorf("expected bucket name 'my-bucket', got '%s'", got)
	}
}
