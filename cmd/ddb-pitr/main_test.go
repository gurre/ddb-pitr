package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/gurre/ddb-pitr/checkpoint"
	"github.com/gurre/ddb-pitr/config"
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

// TestTheDocumentedInvocationIsResumable verifies the shortest invocation the README
// shows comes out resumable, without the operator having named a checkpoint. A restore
// that only records progress when someone remembered a flag loses the whole export the
// first time a machine goes away mid-run.
func TestTheDocumentedInvocationIsResumable(t *testing.T) {
	var out bytes.Buffer
	cfg, err := parseArgs([]string{"--table", "orders", "--export", "s3://bucket/AWSDynamoDB/01234567890-abcdef/"}, &out)
	if err != nil {
		t.Fatalf("expected the flags accepted, got %v", err)
	}
	const want = "s3://bucket/ddb-pitr/checkpoints/01234567890-abcdef.orders.json"
	if got := cfg.CheckpointURI(); got != want {
		t.Errorf("CheckpointURI() = %q, want %q", got, want)
	}
}

// TestNoResumeIsCarriedIntoTheConfiguration verifies --no-resume reaches the
// configuration, which is the only way an operator can turn off a checkpoint they have
// nowhere to write, such as an export in a bucket they may only read.
func TestNoResumeIsCarriedIntoTheConfiguration(t *testing.T) {
	var out bytes.Buffer
	cfg, err := parseArgs([]string{"--table", "orders", "--export", "s3://bucket/export/", "--no-resume"}, &out)
	if err != nil {
		t.Fatalf("expected the flags accepted, got %v", err)
	}
	if got := cfg.CheckpointURI(); got != "" {
		t.Errorf("expected no checkpoint with --no-resume, got %q", got)
	}
}

// TestDefaultInvocationGetsADurableCheckpoint verifies the store behind a plain restore
// is the one that outlives the process, and that --no-resume gets the one that does not.
// Which store is wired in is the whole of whether a restore can be resumed.
func TestDefaultInvocationGetsADurableCheckpoint(t *testing.T) {
	cfg := &config.Config{TableName: "orders", ExportS3URI: "s3://bucket/AWSDynamoDB/0123-abc/"}
	store, err := newCheckpointStore(cfg, nil)
	if err != nil {
		t.Fatalf("expected a checkpoint store, got %v", err)
	}
	if _, ok := store.(*checkpoint.S3Store); !ok {
		t.Errorf("expected progress recorded in S3, got %T", store)
	}

	cfg.NoResume = true
	store, err = newCheckpointStore(cfg, nil)
	if err != nil {
		t.Fatalf("expected a checkpoint store, got %v", err)
	}
	if _, ok := store.(*checkpoint.MemoryStore); !ok {
		t.Errorf("expected --no-resume to record nothing durable, got %T", store)
	}
}

// TestHelpIsNotAFailure verifies --help prints usage and ends the command without an
// error. The rejection of a stray argument points the operator at --help; answering
// that with an error and exit status 1 would look like another failure.
func TestHelpIsNotAFailure(t *testing.T) {
	var out bytes.Buffer
	_, err := parseArgs([]string{"--help"}, &out)
	if !errors.Is(err, errNothingToDo) {
		t.Fatalf("expected help answered and nothing else, got %v", err)
	}
	if !strings.Contains(out.String(), "Usage:") {
		t.Errorf("expected usage printed, got %q", out.String())
	}
}

// TestVersionNeedsNoOtherFlags verifies --version answers on its own, before the
// configuration is validated, so the build can be identified without a table or export.
func TestVersionNeedsNoOtherFlags(t *testing.T) {
	var out bytes.Buffer
	_, err := parseArgs([]string{"--version"}, &out)
	if !errors.Is(err, errNothingToDo) {
		t.Fatalf("expected the version shown and nothing else, got %v", err)
	}
	if !strings.Contains(out.String(), "ddb-pitr") {
		t.Errorf("expected the build identity printed, got %q", out.String())
	}
}
