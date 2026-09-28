package writer

import (
	"context"

	"github.com/gurre/ddb-pitr/itemimage"
)

// Discard takes batches the way DynamoDBWriter does and sends them nowhere. It backs a
// dry run: the export is read, decoded and measured end to end, and the final report
// says what a real restore would have written, while the target table is untouched.
type Discard struct{}

// NewDiscard creates a writer that accepts every batch without sending it.
// Example:
//
//	w := writer.NewDiscard()
//	err := w.Submit(ctx, ops, done) // done reports every operation written
func NewDiscard() *Discard {
	return &Discard{}
}

// Submit reports the whole batch written, before returning. Cancellation is honoured
// so a dry run stops as promptly as a real restore does.
func (d *Discard) Submit(ctx context.Context, ops []itemimage.Operation, done func(Rejection, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	done(Rejection{}, nil)
	return nil
}
