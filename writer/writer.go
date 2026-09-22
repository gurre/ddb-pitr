// Package writer applies decoded export operations to a DynamoDB table in batches,
// surviving throttling and transient failures with paced retries.
package writer

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/gurre/ddb-pitr/aws"
	"github.com/gurre/ddb-pitr/itemimage"
)

// Writer applies batches of operations to the target table. A batch that returns
// without error was written in full; nothing is held back for a later call.
type Writer interface {
	WriteBatch(ctx context.Context, ops []itemimage.Operation) error
}

// Callbacks allows the writer to report metrics without coupling to a specific metrics implementation.
// All callbacks are optional - nil callbacks are safely ignored.
type Callbacks struct {
	OnThrottle func()                     // Called on throttle event
	OnRetry    func()                     // Called on successful retry after transient failure
	OnLost     func(count int)            // Called with the items still unwritten when a batch is given up
	OnWrite    func(items, bytes int)     // Called on successful write with item count and the export bytes behind them
	OnPace     func(wcuPerSecond float64) // Called when the rate the table is being held to changes
}

// Backoffer paces the wait between retry attempts.
// Wait reports false when the wait was cut short because the context ended,
// which callers must treat as "stop retrying".
type Backoffer interface {
	Wait(ctx context.Context, attempt int) bool
}

// errBackoffStopped stands in when a wait was cut short but the context reports no
// error. Returning the context's nil error there would hand a surrendered batch back as
// a successful write, and the restore would count items it never sent.
var errBackoffStopped = errors.New("writer: backoff stopped before the batch was written")

// stopRetrying reports why the retry loop is giving up. It never returns nil.
func stopRetrying(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errBackoffStopped
}

// Option adjusts optional DynamoDBWriter behaviour.
type Option func(*DynamoDBWriter)

// withPaceClock replaces where the pacer reads the time and does its waiting, so a
// test can drive the rate the table is being held to without spending the time it
// describes.
func withPaceClock(c paceClock) Option {
	return func(w *DynamoDBWriter) {
		w.pacer.clock = c
	}
}

// WithBackoff replaces the pacing of retries after a failure that is not the table
// refusing work. A throttled request instead waits for the capacity it needs, which is
// what the pacer decides. The default doubles from 100ms up to 30s.
// Example:
//
//	w := writer.NewDynamoDBWriter(client, "my-table", 25, cb,
//	    writer.WithBackoff(writer.NewExponentialBackoff(time.Second, time.Minute)))
func WithBackoff(b Backoffer) Option {
	return func(w *DynamoDBWriter) {
		w.backoff = b
	}
}

// DynamoDBWriter implements Writer with BatchWriteItem, retrying throttled batches for
// as long as the context lives and other failures a bounded number of times.
// Fields are ordered largest-to-smallest for memory alignment.
type DynamoDBWriter struct {
	callbacks Callbacks
	client    aws.DynamoDBClient
	backoff   Backoffer
	tableName string
	pacer     *pacer
	batchSize int // Largest number of operations the writer will put in one request (≤25)
}

// maxBatchSize is DynamoDB's hard limit on the number of requests BatchWriteItem accepts.
const maxBatchSize = 25

// maxTransientAttempts bounds how often a batch is retried after a failure that is not
// throttling. Throttling is retried for as long as the context lives, since capacity
// refills; anything else is given this many further attempts and then given up.
const maxTransientAttempts = 5

// NewDynamoDBWriter creates a new DynamoDBWriter instance with the specified batch size and callbacks.
// The batch size must be within DynamoDB's limits: a non-positive one leaves the split
// loop unable to advance and a larger one builds a request DynamoDB rejects, so both are
// wiring mistakes worth failing on here rather than mid-restore.
// Example:
//
//	w := writer.NewDynamoDBWriter(client, "my-table", 25, writer.Callbacks{
//	    OnThrottle: m.RecordThrottle,
//	})
func NewDynamoDBWriter(client aws.DynamoDBClient, tableName string, batchSize int, callbacks Callbacks, opts ...Option) *DynamoDBWriter {
	if batchSize < 1 || batchSize > maxBatchSize {
		panic(fmt.Sprintf("writer: batch size %d is outside 1..%d", batchSize, maxBatchSize))
	}
	w := &DynamoDBWriter{
		client:    client,
		tableName: tableName,
		batchSize: batchSize,
		callbacks: callbacks,
		backoff:   NewExponentialBackoff(100*time.Millisecond, 30*time.Second),
		pacer:     newPacer(callbacks.OnPace),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// isThrottlingError reports whether DynamoDB refused the request for want of capacity,
// which waiting will fix. Provisioned tables raise ProvisionedThroughputExceededException
// and account-level limits RequestLimitExceeded, both of which the SDK types. On-demand
// tables raise ThrottlingException, which the SDK does not type and which arrives as a
// generic API error carrying that code.
func isThrottlingError(err error) bool {
	var throughputErr *types.ProvisionedThroughputExceededException
	var requestLimitErr *types.RequestLimitExceeded
	if errors.As(err, &throughputErr) || errors.As(err, &requestLimitErr) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "ThrottlingException", "RequestThrottled", "ThrottledException":
			return true
		}
	}
	return false
}

// ExponentialBackoff doubles the wait after every attempt up to Max, then adds
// jitter of up to one further interval so concurrent workers do not retry in lockstep.
// Fields are exported for inspection; use NewExponentialBackoff to build one.
type ExponentialBackoff struct {
	Base time.Duration // Wait before the first retry
	Max  time.Duration // Ceiling for the doubled wait, before jitter
}

// NewExponentialBackoff returns a backoff that starts at base and doubles up to max.
// Both durations must be positive: a non-positive interval would spin, so it is a
// wiring mistake and panics here rather than degrading a restore into a busy loop.
// Example:
//
//	b := writer.NewExponentialBackoff(100*time.Millisecond, 30*time.Second)
func NewExponentialBackoff(base, max time.Duration) *ExponentialBackoff {
	if base <= 0 || max < base {
		panic(fmt.Sprintf("writer: invalid backoff base=%s max=%s", base, max))
	}
	return &ExponentialBackoff{Base: base, Max: max}
}

// Delay returns the wait before the given attempt, jitter included.
//
// The shift is clamped before it is applied. Throttling is retried for as long as
// the context lives, so attempt is unbounded: an unclamped shift overflows the
// duration during a long throttling storm and yields a negative delay, which the
// Max ceiling does not catch and the jitter draw cannot accept.
func (b *ExponentialBackoff) Delay(attempt int) time.Duration {
	shift := attempt
	if shift < 0 {
		shift = 0
	}
	// Beyond this the doubled value already exceeds any Max we would honour.
	if limit := shiftLimit(b.Base, b.Max); shift > limit {
		shift = limit
	}

	delay := b.Base << uint(shift)
	if delay > b.Max {
		delay = b.Max
	}

	return delay + time.Duration(rand.Int64N(int64(delay)))
}

// Wait sleeps for Delay(attempt), reporting false if the context ends first.
func (b *ExponentialBackoff) Wait(ctx context.Context, attempt int) bool {
	timer := time.NewTimer(b.Delay(attempt))
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// shiftLimit returns the smallest shift for which base doubled reaches max.
// Doubling stops there, which keeps the shift far below the width of a Duration.
func shiftLimit(base, max time.Duration) int {
	limit := 0
	for d := base; d < max; d <<= 1 {
		limit++
	}
	return limit
}

// WriteBatch splits the operations into requests and writes each with BatchWriteItem.
// Puts and updates both replace the whole item with its new image: an export's new
// image is the item's complete state, so replacing is exactly what applying the change
// means, and it needs no per-attribute expression that an attribute name could break.
// Deletes remove the item by key.
//
// HOT PATH: Called for every batch of decoded items.
// Profiling shows ~13% CPU time with most overhead in:
//   - BatchWriteItem API calls (network latency)
//   - Waiting for write capacity when the table is the limit
func (w *DynamoDBWriter) WriteBatch(ctx context.Context, ops []itemimage.Operation) error {
	if len(ops) == 0 {
		return nil
	}

	for i := 0; i < len(ops); i += w.batchSize {
		end := i + w.batchSize
		if end > len(ops) {
			end = len(ops)
		}
		batch := ops[i:end]

		requests := make([]types.WriteRequest, 0, len(batch))
		batchBytes := 0
		units := 0.0
		for _, op := range batch {
			switch op.Type {
			case itemimage.OpPut, itemimage.OpUpdate:
				requests = append(requests, types.WriteRequest{
					PutRequest: &types.PutRequest{Item: op.NewImage},
				})
			case itemimage.OpDelete:
				requests = append(requests, types.WriteRequest{
					DeleteRequest: &types.DeleteRequest{Key: op.Keys},
				})
			default:
				// A kind the writer does not know. Dropping it would send a short or
				// empty batch and lose the item without anything saying so.
				return fmt.Errorf("writer: operation type %d is not one the writer knows", op.Type)
			}
			batchBytes += int(op.Bytes)
			units += writeCost(op)
		}

		if err := w.writeRequests(ctx, requests, batchBytes, units); err != nil {
			return err
		}
	}

	return nil
}

// writeRequests writes the given requests, sending as many at a time as the target
// table is accepting and resending whatever it does not take.
//
// How many go in one call is the batch size at most, and fewer once the table has shown
// it cannot take that many. A table provisioned at a few units a second can never accept
// twenty-five items at once: it hands most of them straight back, and a writer that
// resends the same twenty-five spends the restore collecting the same refusal. Sizing
// each call to the rate the table is accepting is what makes such a table restore at its
// capacity rather than below it, without anyone having to find the right --batch.
//
// Two things send a call round again, and they are budgeted apart. Throttling, whether
// as an error or as items handed back unprocessed, is retried for as long as the context
// lives: capacity refills, and giving up would lose items to a condition that waiting
// cures. Any other failure is retried maxTransientAttempts times and then what is left
// is given up, so a poisoned batch cannot stall the restore. Counting both against one
// budget would let a hot partition's partial acceptances spend the transient budget, and
// a single passing fault would then lose the batch.
//
// The two also wait differently. A throttled call waits for the capacity it needs, which
// arrives as soon as the table has it; an exponential sleep would keep waiting long
// after that. Anything else still backs off exponentially, since nothing about it says
// when it will clear.
func (w *DynamoDBWriter) writeRequests(ctx context.Context, requests []types.WriteRequest, batchBytes int, units float64) error {
	items := len(requests)
	// Items are charged their share of the batch: the response does not say which of
	// them came back, and the capacity the table reports corrects the estimate anyway.
	unitCost := units / float64(items)

	pending := requests
	throttleRounds := 0
	transientAttempts := 0
	for len(pending) > 0 {
		n, ok := w.pacer.admit(ctx, unitCost, len(pending))
		if !ok {
			return stopRetrying(ctx)
		}
		input := &dynamodb.BatchWriteItemInput{
			RequestItems:           map[string][]types.WriteRequest{w.tableName: pending[:n]},
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		}

		output, err := w.client.BatchWriteItem(ctx, input)
		if err != nil {
			if isThrottlingError(err) {
				w.reportThrottle()
				w.pacer.throttled()
				throttleRounds++
				continue
			}
			if transientAttempts < maxTransientAttempts {
				if !w.backoff.Wait(ctx, throttleRounds+transientAttempts) {
					return stopRetrying(ctx)
				}
				transientAttempts++
				continue
			}
			// Whatever has not been accepted by now is what the restore loses.
			if w.callbacks.OnLost != nil {
				w.callbacks.OnLost(len(pending))
			}
			return fmt.Errorf("failed to write batch after %d retries: %w", maxTransientAttempts, err)
		}

		w.pacer.settle(unitCost*float64(n), consumedUnits(output.ConsumedCapacity))

		// Items handed back unprocessed are DynamoDB throttling part of the call.
		unprocessed := output.UnprocessedItems[w.tableName]
		if len(unprocessed) > 0 {
			w.reportThrottle()
			w.pacer.throttled()
			throttleRounds++
		}
		// What is left is whatever came back plus whatever capacity did not stretch to.
		// The rejected items are written over the tail of the part just sent, which is
		// at least as long, so the remainder is reassembled without another allocation.
		// Their order among themselves does not matter: a batch write is unordered.
		copy(pending[n-len(unprocessed):n], unprocessed)
		pending = pending[n-len(unprocessed):]
	}

	if throttleRounds+transientAttempts > 0 && w.callbacks.OnRetry != nil {
		w.callbacks.OnRetry()
	}
	if w.callbacks.OnWrite != nil {
		w.callbacks.OnWrite(items, batchBytes)
	}
	return nil
}

// consumedUnits totals the capacity DynamoDB reported for a request. A response that
// reports none leaves the pacer on its own estimate, which is what happens against a
// client that does not fill the field in.
func consumedUnits(capacity []types.ConsumedCapacity) float64 {
	total := 0.0
	for _, c := range capacity {
		if c.CapacityUnits != nil {
			total += *c.CapacityUnits
		}
	}
	return total
}

func (w *DynamoDBWriter) reportThrottle() {
	if w.callbacks.OnThrottle != nil {
		w.callbacks.OnThrottle()
	}
}
