package writer

import (
	"context"

	"github.com/gurre/ddb-pitr/itemimage"
)

// Discard implements Writer by accounting for operations without sending them anywhere.
// It backs a dry run: the export is read, decoded and measured end to end, and the final
// report says what a real restore would have written, while the target table is untouched.
type Discard struct {
	callbacks Callbacks
}

// NewDiscard creates a Writer that measures operations instead of writing them.
// Example:
//
//	w := writer.NewDiscard(writer.Callbacks{OnWrite: func(items, bytes int) {
//	    m.RecordBytes(int64(bytes))
//	}})
func NewDiscard(callbacks Callbacks) *Discard {
	return &Discard{callbacks: callbacks}
}

// WriteBatch reports the batch to the OnWrite callback and discards it. Cancellation is
// honoured so a dry run stops as promptly as a real restore does.
func (d *Discard) WriteBatch(ctx context.Context, ops []itemimage.Operation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ops) == 0 || d.callbacks.OnWrite == nil {
		return nil
	}

	// Measured the same way the real writer measures them, so the reported throughput
	// of a dry run is the throughput of the restore it is standing in for.
	bytes := 0
	for _, op := range ops {
		bytes += int(op.Bytes)
	}

	d.callbacks.OnWrite(len(ops), bytes)
	return nil
}
