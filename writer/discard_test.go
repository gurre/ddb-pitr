package writer

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gurre/ddb-pitr/itemimage"
)

// TestDiscardWritesNothing verifies a dry run reaches no DynamoDB client at all. This is
// the whole guarantee of --dry-run: an operator pointing one at a production table must
// not have it written to.
func TestDiscardWritesNothing(t *testing.T) {
	// The nil client is the assertion: any call at all panics the test.
	w := NewDiscard(Callbacks{})

	ops := []itemimage.Operation{putOps(1)[0], updateOp(), {
		Type: itemimage.OpDelete,
		Keys: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "USER#9"}},
	}}
	if err := w.WriteBatch(context.Background(), ops); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
}

// TestDiscardReportsWhatARestoreWouldWrite verifies a dry run still measures the export,
// since a report saying nothing was written would make the run pointless. It reports
// the same items and bytes the real writer reports for the same operations, so the
// dry run's figures are the restore's figures.
func TestDiscardReportsWhatARestoreWouldWrite(t *testing.T) {
	ops := append(putOps(2), itemimage.Operation{
		Type:  itemimage.OpDelete,
		Bytes: 70,
		Keys: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "USER#9"},
		},
	})

	var dryItems, dryBytes int
	dry := NewDiscard(Callbacks{OnWrite: func(items, bytes int) { dryItems, dryBytes = items, bytes }})
	if err := dry.WriteBatch(context.Background(), ops); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	var realItems, realBytes int
	restore := NewDynamoDBWriter(&scriptedClient{}, "test-table", 25,
		Callbacks{OnWrite: func(items, bytes int) { realItems, realBytes = items, bytes }},
		WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))
	if err := restore.WriteBatch(context.Background(), ops); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if dryItems != realItems || dryBytes != realBytes {
		t.Errorf("dry run reported %d items and %d bytes, the restore %d and %d",
			dryItems, dryBytes, realItems, realBytes)
	}
	if dryItems != 3 || dryBytes == 0 {
		t.Errorf("expected 3 items and their bytes reported, got %d and %d", dryItems, dryBytes)
	}
}

// TestDiscardReportsNothingForAnEmptyBatch verifies an empty batch is not reported as a
// write, which would inflate the batch count a dry run reports.
func TestDiscardReportsNothingForAnEmptyBatch(t *testing.T) {
	calls := 0
	w := NewDiscard(Callbacks{OnWrite: func(items, bytes int) { calls++ }})

	if err := w.WriteBatch(context.Background(), nil); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
	if calls != 0 {
		t.Errorf("expected no write reported, got %d", calls)
	}
}

// TestDiscardHonoursCancellation verifies a dry run stops when the context ends, the
// same way a real restore does. A dry run that ignored cancellation would keep reading
// the whole export after the operator asked it to stop.
func TestDiscardHonoursCancellation(t *testing.T) {
	w := NewDiscard(Callbacks{OnWrite: func(items, bytes int) {
		t.Error("a cancelled dry run reported a write")
	}})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := w.WriteBatch(ctx, putOps(1)); !errors.Is(err, context.Canceled) {
		t.Errorf("WriteBatch = %v, want context.Canceled", err)
	}
}
