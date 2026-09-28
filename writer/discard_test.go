package writer

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gurre/ddb-pitr/itemimage"
)

// TestDiscardReportsEveryBatchWritten verifies a dry run reaches no DynamoDB client and
// still reports the batch done with nothing rejected, which is what lets the restore
// around it measure the export end to end. This is the whole guarantee of --dry-run: an
// operator pointing one at a production table must not have it written to.
func TestDiscardReportsEveryBatchWritten(t *testing.T) {
	ops := []itemimage.Operation{putOps(1)[0], updateOp(), {
		Type: itemimage.OpDelete,
		Keys: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "USER#9"}},
	}}
	var got *Rejection
	if err := NewDiscard().Submit(t.Context(), ops, func(r Rejection, err error) {
		if err != nil {
			t.Errorf("done reported %v", err)
		}
		got = &r
	}); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if got == nil || len(got.Refused)+len(got.HandedBack) != 0 {
		t.Errorf("expected the batch reported written whole, got %+v", got)
	}
}

// TestDiscardHonoursCancellation verifies a dry run stops when the context ends, the
// same way a real restore does. A dry run that ignored cancellation would keep reading
// the whole export after the operator asked it to stop.
func TestDiscardHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := NewDiscard().Submit(ctx, putOps(1), func(Rejection, error) {
		t.Error("a cancelled dry run reported a batch")
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Submit = %v, want context.Canceled", err)
	}
}
