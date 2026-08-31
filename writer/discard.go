package writer

import (
	"context"

	"github.com/gurre/ddb-pitr/itemimage"
)

// Discard implements Writer by accounting for operations without sending them anywhere.
// It backs a dry run: the export is read, decoded and measured end to end, and the final
// report says what a real restore would have written, while the target table is untouched.
//
// It reports every operation it is handed, including an update that changes nothing but
// the key, which DynamoDBWriter skips as an empty expression. A dry run therefore counts
// at or above what a real restore would write, never below.
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

	// Sized the same way the real writer sizes them, so the reported throughput of a
	// dry run is comparable with the restore it is standing in for.
	bytes := 0
	for _, op := range ops {
		if op.Type == itemimage.OpDelete {
			bytes += estimateItemSize(op.Keys)
			continue
		}
		bytes += estimateItemSize(op.NewImage)
	}

	d.callbacks.OnWrite(len(ops), bytes)
	return nil
}

// Flush is a no-op: nothing is ever held back.
func (d *Discard) Flush(ctx context.Context) error {
	return ctx.Err()
}
