// Package writer applies decoded export operations to a DynamoDB table in batches. It
// tunes itself to the table: how many calls are in flight follows the latency the table
// answers with, how large each call is follows the rate the table accepts, and what the
// table does not take is handed back to the caller to send again.
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

// Callbacks allows the writer to report metrics without coupling to a specific metrics implementation.
// All callbacks are optional - nil callbacks are safely ignored.
type Callbacks struct {
	OnThrottle    func()                                    // Called when the table refuses a call or hands items back
	OnRetry       func()                                    // Called when a call succeeds after a transient failure
	OnLost        func(kind itemimage.OperationType, n int) // Called with the operations of each kind given up with a failed batch
	OnPace        func(wcuPerSecond float64)                // Called when the rate the table is being held to changes
	OnConcurrency func(limit int)                           // Called when how many calls may be in flight changes
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
// WithMaxInFlight sets the most calls the writer lets be in flight at once, in place of
// DefaultMaxInFlight. It bounds memory and connections; a restore held at it by a slow
// path to the table writes slower than it could. It must be at least one, since a writer
// that may have no call in flight can never write, and at most HighestMaxInFlight, so a
// restore cannot flood the table and the network; anything outside is a wiring mistake.
// Example:
//
//	w := writer.NewDynamoDBWriter(client, "my-table", cb, writer.WithMaxInFlight(256))
func WithMaxInFlight(n int) Option {
	if n < 1 || n > HighestMaxInFlight {
		panic(fmt.Sprintf("writer: %d calls in flight is outside 1..%d", n, HighestMaxInFlight))
	}
	return func(w *DynamoDBWriter) {
		w.maxInFlight = n
	}
}

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
//	w := writer.NewDynamoDBWriter(client, "my-table", cb,
//	    writer.WithBackoff(writer.NewExponentialBackoff(time.Second, time.Minute)))
func WithBackoff(b Backoffer) Option {
	return func(w *DynamoDBWriter) {
		w.backoff = b
	}
}

// Rejection names the operations of a submitted batch the table did not take, by their
// index in the batch as submitted. Everything else in the batch was written.
//
// The two differ in what they say. A handed back operation came back from a call the
// table otherwise took, which means its partition is short while others are not; whoever
// sends that partition's items is the one to slow down. A refused operation was in a
// call the table turned away whole, which it does when none of the call's partitions had
// capacity: the table as a whole when the call spanned many, one partition when it was
// small. Either way the writer's pacing weighs it, and the operation is sent again.
type Rejection struct {
	Refused    []int // In calls the table refused outright
	HandedBack []int // Handed back unprocessed from calls the table otherwise took
}

// DynamoDBWriter implements batch submission with BatchWriteItem. It decides how many
// calls are in flight, from how the table's latency responds, and how large each one
// is, from the rate the table accepts. Operations the table does not take are handed
// back to the caller rather than resent here, so a hot partition's items do not hold a
// call slot while the rest of the table waits behind them. Failures that are not the
// table refusing work are retried a bounded number of times.
// Fields are ordered largest-to-smallest for memory alignment.
type DynamoDBWriter struct {
	callbacks   Callbacks
	client      aws.DynamoDBClient
	backoff     Backoffer
	tableName   string
	pacer       *pacer
	limiter     *concurrencyLimiter
	maxInFlight int // Most calls in flight at once; set before the limiter is built
}

// MaxBatch is DynamoDB's hard limit on the number of requests one BatchWriteItem
// accepts, and so the most operations one Submit takes.
const MaxBatch = 25

// maxTransientAttempts bounds how often a call is retried after a failure that is not
// the table refusing work. Anything else is given this many further attempts and then
// given up, so a poisoned batch cannot stall the restore.
const maxTransientAttempts = 5

// NewDynamoDBWriter creates a writer for the named table.
// Example:
//
//	w := writer.NewDynamoDBWriter(client, "my-table", writer.Callbacks{
//	    OnThrottle: m.RecordThrottle,
//	})
func NewDynamoDBWriter(client aws.DynamoDBClient, tableName string, callbacks Callbacks, opts ...Option) *DynamoDBWriter {
	w := &DynamoDBWriter{
		client:      client,
		tableName:   tableName,
		callbacks:   callbacks,
		backoff:     NewExponentialBackoff(100*time.Millisecond, 30*time.Second),
		pacer:       newPacer(callbacks.OnPace),
		maxInFlight: DefaultMaxInFlight,
	}
	for _, opt := range opts {
		opt(w)
	}
	w.limiter = newConcurrencyLimiter(w.maxInFlight, callbacks.OnConcurrency)
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
// jitter of up to one further interval so calls in flight together do not retry in lockstep.
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

// Submit writes a batch of at most MaxBatch operations in the background, and calls
// done once with what the table did not take, or with the error that made the batch
// fail. It blocks while as many batches are in flight as the table is rewarding, and
// returns the context's error if that ends first, in which case done is never called.
//
// done runs on a goroutine of the writer's own, except for an empty batch, which is
// answered before Submit returns; either way it must not block on whoever called Submit. A batch that fails with an error was given up; the operations named in its
// Rejection were not written either, and neither was anything else not yet sent.
//
// Puts and updates both replace the whole item with its new image: an export's new
// image is the item's complete state, so replacing is exactly what applying the change
// means, and it needs no per-attribute expression that an attribute name could break.
// Deletes remove the item by key.
//
// HOT PATH: called for every batch of decoded items. The cost is the BatchWriteItem
// call itself, and waiting for write capacity when the table is the limit.
// Example:
//
//	err := w.Submit(ctx, ops, func(r writer.Rejection, err error) {
//	    // requeue r.Refused and r.HandedBack; acknowledge the rest
//	})
func (w *DynamoDBWriter) Submit(ctx context.Context, ops []itemimage.Operation, done func(Rejection, error)) error {
	if len(ops) > MaxBatch {
		panic(fmt.Sprintf("writer: batch of %d operations is more than the %d one call takes", len(ops), MaxBatch))
	}
	if len(ops) == 0 {
		done(Rejection{}, nil)
		return nil
	}
	if err := w.limiter.acquire(ctx); err != nil {
		return err
	}
	go func() {
		rejection, err := w.write(ctx, ops)
		w.limiter.release()
		done(rejection, err)
	}()
	return nil
}

// write sends the batch in as many calls as the rate the table accepts calls for, and
// reports what it did not take.
//
// How many go in one call is the whole batch at most, and fewer once the table has
// shown it cannot take that many. A table provisioned at a few units a second can never
// accept twenty-five items at once, and sizing each call to the rate it is accepting is
// what makes such a table restore at its capacity, without anyone having to find the
// right batch size.
//
// A call the table refuses, or items it hands back, are reported rather than resent:
// the caller mixes them into later batches, so capacity elsewhere in the table is not
// left waiting on the partition that refused them. Any other failure is retried
// maxTransientAttempts times with exponential backoff, since nothing about it says when
// it will clear, and then the batch is given up.
func (w *DynamoDBWriter) write(ctx context.Context, ops []itemimage.Operation) (Rejection, error) {
	var rejection Rejection
	requests := make([]types.WriteRequest, len(ops))
	units := 0.0
	for i, op := range ops {
		switch op.Type {
		case itemimage.OpPut, itemimage.OpUpdate:
			requests[i] = types.WriteRequest{PutRequest: &types.PutRequest{Item: op.NewImage}}
		case itemimage.OpDelete:
			requests[i] = types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: op.Keys}}
		default:
			// A kind the writer does not know. Dropping it would send a short batch and
			// lose the item without anything saying so.
			return rejection, fmt.Errorf("writer: operation type %d is not one the writer knows", op.Type)
		}
		units += writeCost(op)
	}
	// Items are charged their share of the batch: the response does not say what each
	// of them cost, and the capacity the table reports corrects the estimate anyway.
	unitCost := units / float64(len(ops))

	// One input carries every call; only which items go differs between them.
	input := &dynamodb.BatchWriteItemInput{
		RequestItems:           map[string][]types.WriteRequest{w.tableName: requests},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	}

	sent := 0
	transientAttempts := 0
	for sent < len(requests) {
		n, ok := w.pacer.admit(ctx, unitCost, len(requests)-sent)
		if !ok {
			return rejection, stopRetrying(ctx)
		}
		call := requests[sent : sent+n]
		input.RequestItems[w.tableName] = call

		start := time.Now()
		output, err := w.client.BatchWriteItem(ctx, input)
		if err != nil {
			if isThrottlingError(err) {
				// The table answered, so the call says how long one takes on the wire.
				w.limiter.sample(time.Since(start))
				w.reportThrottle()
				w.pacer.refused(unitCost * float64(n))
				for i := sent; i < sent+n; i++ {
					rejection.Refused = append(rejection.Refused, i)
				}
				sent += n
				continue
			}
			if transientAttempts < maxTransientAttempts {
				if !w.backoff.Wait(ctx, transientAttempts) {
					return rejection, stopRetrying(ctx)
				}
				transientAttempts++
				continue
			}
			// Whatever has not been sent by now is what the restore loses.
			w.reportLost(ops[sent:])
			return rejection, fmt.Errorf("failed to write batch after %d retries: %w", maxTransientAttempts, err)
		}
		// Only the call itself counts as time on the wire; time spent waiting for the
		// table's capacity before it was sent does not.
		w.limiter.sample(time.Since(start))

		unprocessed := output.UnprocessedItems[w.tableName]
		w.pacer.settle(unitCost*float64(n), consumedUnits(output.ConsumedCapacity), unitCost*float64(len(unprocessed)))
		if len(unprocessed) > 0 {
			w.reportThrottle()
			rejection.HandedBack = appendHandedBack(rejection.HandedBack, call, sent, unprocessed)
		}
		sent += n
	}

	if transientAttempts > 0 && w.callbacks.OnRetry != nil {
		w.callbacks.OnRetry()
	}
	return rejection, nil
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

// reportLost reports the operations given up, by kind, so the report can say which
// kind of change the table is missing.
func (w *DynamoDBWriter) reportLost(ops []itemimage.Operation) {
	if w.callbacks.OnLost == nil {
		return
	}
	var counts [itemimage.OperationKinds]int
	for _, op := range ops {
		counts[op.Type]++
	}
	for kind, n := range counts {
		if n > 0 {
			w.callbacks.OnLost(itemimage.OperationType(kind), n)
		}
	}
}
