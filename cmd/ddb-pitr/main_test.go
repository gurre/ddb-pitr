package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// TestPositionalArgumentsAreRejectedByName verifies a word typed before the flags ends
// the command with that word named. The flag parser stops at the first word that is
// not a flag, so without this check every flag after it would silently go unread and
// the operator would be told the table name is missing when it plainly is not.
func TestPositionalArgumentsAreRejectedByName(t *testing.T) {
	var out bytes.Buffer
	_, err := parseArgs([]string{"restore", "--table", "orders", "--export", "s3://bucket/export/"}, &out)
	if err == nil || !strings.Contains(err.Error(), `"restore"`) {
		t.Fatalf("expected the stray word named in the error, got %v", err)
	}
	if !strings.Contains(out.String(), "Usage:") {
		t.Errorf("expected usage printed with the rejection, got %q", out.String())
	}
}

// TestFlagsAloneMakeAConfiguration verifies the documented invocation, flags only with
// the export given as a directory, produces a runnable configuration.
func TestFlagsAloneMakeAConfiguration(t *testing.T) {
	var out bytes.Buffer
	cfg, err := parseArgs([]string{"--table", "orders", "--export", "s3://bucket/AWSDynamoDB/01234567890-abcdef/"}, &out)
	if err != nil {
		t.Fatalf("expected the flags accepted, got %v", err)
	}
	if cfg.TableName != "orders" || cfg.GetExportBucketName() != "bucket" {
		t.Errorf("expected the flags carried into the configuration, got %+v", cfg)
	}
	if cfg.Region != "" {
		t.Errorf("expected no region when none was given, got %q", cfg.Region)
	}
}

// TestVersionNeedsNoOtherFlags verifies --version answers on its own, before the
// configuration is validated, so the build can be identified without a table or export.
func TestVersionNeedsNoOtherFlags(t *testing.T) {
	var out bytes.Buffer
	_, err := parseArgs([]string{"--version"}, &out)
	if !errors.Is(err, errVersionShown) {
		t.Fatalf("expected the version shown and nothing else, got %v", err)
	}
	if !strings.Contains(out.String(), "ddb-pitr") {
		t.Errorf("expected the build identity printed, got %q", out.String())
	}
}
