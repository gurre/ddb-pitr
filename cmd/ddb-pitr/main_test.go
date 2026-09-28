package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/gurre/ddb-pitr/checkpoint"
	"github.com/gurre/ddb-pitr/config"
	"github.com/gurre/ddb-pitr/coordinator"
	"github.com/gurre/ddb-pitr/lease"
	"github.com/gurre/ddb-pitr/writer"
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

// TestExitStatusTellsTheOutcomesApart verifies each outcome that calls for a different
// response exits with its own status, however deeply the cause is wrapped. A script
// that restarts a failed restore must not restart one that found the table held by
// another, which would only be refused again, nor one that skipped records, which
// running again cannot change.
func TestExitStatusTellsTheOutcomesApart(t *testing.T) {
	tests := []struct {
		err  error
		want int
	}{
		{fmt.Errorf("claim: %w", lease.ErrLeaseHeld), exitHeld},
		{fmt.Errorf("restore: %w", coordinator.ErrRecordsSkipped), exitSkipped},
		{fmt.Errorf("restore stopped: %w", lease.ErrLeaseLost), exitFailed},
		{errors.New("anything else"), exitFailed},
	}
	for _, tt := range tests {
		if got := exitStatus(tt.err); got != tt.want {
			t.Errorf("exitStatus(%v) = %d, want %d", tt.err, got, tt.want)
		}
	}
}

// TestNoWriteIsSentOnceTheClaimLapses verifies neither a write to the table nor a save
// of progress goes out once the restore can no longer show it holds the table, and both
// go out while it can. A restore that went on writing after its claim lapsed could
// interleave with the restore that took the table over, which the claim exists to stop.
func TestNoWriteIsSentOnceTheClaimLapses(t *testing.T) {
	claim := &stubClaim{}
	table := &countingTable{}
	store := &fencedStore{store: checkpoint.NewMemoryStore(), claim: claim}
	fenced := &fencedTable{table: table, claim: claim}

	if _, err := fenced.BatchWriteItem(t.Context(), &dynamodb.BatchWriteItemInput{}); err != nil || table.calls != 1 {
		t.Fatalf("write with the claim held: err %v, %d calls", err, table.calls)
	}
	if err := store.Save(t.Context(), checkpoint.State{}); err != nil {
		t.Fatalf("save with the claim held: %v", err)
	}

	claim.err = lease.ErrLeaseLost
	if _, err := fenced.BatchWriteItem(t.Context(), &dynamodb.BatchWriteItemInput{}); !errors.Is(err, lease.ErrLeaseLost) || table.calls != 1 {
		t.Errorf("write after the claim lapsed: err %v, %d calls; want refused and unsent", err, table.calls)
	}
	if err := store.Save(t.Context(), checkpoint.State{}); !errors.Is(err, lease.ErrLeaseLost) {
		t.Errorf("save after the claim lapsed = %v, want refused", err)
	}
}

// stubClaim reports whatever error the test sets.
type stubClaim struct{ err error }

func (c *stubClaim) Check() error { return c.err }

// countingTable accepts every write and counts them.
type countingTable struct{ calls int }

func (c *countingTable) BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	c.calls++
	return &dynamodb.BatchWriteItemOutput{}, nil
}

// TestMaxInFlightDefaultsToTheWritersCeilingAndCanBeSet verifies the in-flight ceiling
// reaches the configuration as given, and is the writer's own default when not given. An
// operator bounding a restore's memory or connections needs the number they typed to be
// the number the restore holds to.
func TestMaxInFlightDefaultsToTheWritersCeilingAndCanBeSet(t *testing.T) {
	base := []string{"--table", "orders", "--export", "s3://backups/AWSDynamoDB/0123-abc"}
	cfg, err := parseArgs(base, &bytes.Buffer{})
	if err != nil || cfg.MaxInFlight != writer.DefaultMaxInFlight {
		t.Fatalf("default: %v, max in flight %d; want %d", err, cfg.MaxInFlight, writer.DefaultMaxInFlight)
	}
	cfg, err = parseArgs(append(base, "--max-in-flight", "64"), &bytes.Buffer{})
	if err != nil || cfg.MaxInFlight != 64 {
		t.Fatalf("--max-in-flight 64: %v, max in flight %d", err, cfg.MaxInFlight)
	}
	if _, err := parseArgs(append(base, "--max-in-flight", "0"), &bytes.Buffer{}); err == nil {
		t.Error("--max-in-flight 0 was accepted")
	}
}

// TestMaxInFlightIsBoundedWhereTheWriterIsBounded verifies the command accepts every
// in-flight ceiling the writer does and refuses the first it would not. Were the two to
// disagree, a ceiling the command let through would stop the restore at startup with a
// panic instead of a message saying which flag is wrong.
func TestMaxInFlightIsBoundedWhereTheWriterIsBounded(t *testing.T) {
	base := []string{"--table", "orders", "--export", "s3://backups/AWSDynamoDB/0123-abc", "--max-in-flight"}
	if _, err := parseArgs(append(base, fmt.Sprint(writer.HighestMaxInFlight)), &bytes.Buffer{}); err != nil {
		t.Errorf("--max-in-flight %d, the writer's highest, was refused: %v", writer.HighestMaxInFlight, err)
	}
	if _, err := parseArgs(append(base, fmt.Sprint(writer.HighestMaxInFlight+1)), &bytes.Buffer{}); err == nil {
		t.Errorf("--max-in-flight %d, above the writer's highest, was accepted", writer.HighestMaxInFlight+1)
	}
}
