package writer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gurre/ddb-pitr/itemimage"
)

// TestBackoffDelayNeverCollapses covers the whole attempt range a throttled restore
// can reach. Throttling retries are unbounded, so the exponent grows without limit;
// an unclamped shift overflows the duration and produces a zero or negative delay,
// which would turn the retry loop into a busy loop and reject the jitter draw.
func TestBackoffDelayNeverCollapses(t *testing.T) {
	b := NewExponentialBackoff(100*time.Millisecond, 30*time.Second)

	for attempt := 0; attempt <= 128; attempt++ {
		if d := b.Delay(attempt); d < b.Base {
			t.Fatalf("attempt %d: delay %s is below the base interval %s", attempt, d, b.Base)
		}
	}
}

// TestBackoffDelayDoublesUntilCapped pins the pacing contract: each attempt waits
// twice as long as the previous one until Max, and jitter never adds more than one
// further interval. Backoff that stops growing hammers a throttled table.
func TestBackoffDelayDoublesUntilCapped(t *testing.T) {
	b := NewExponentialBackoff(100*time.Millisecond, 30*time.Second)

	tests := []struct {
		name    string
		attempt int
		floor   time.Duration
	}{
		{name: "first attempt waits the base interval", attempt: 0, floor: 100 * time.Millisecond},
		{name: "second attempt waits twice the base", attempt: 1, floor: 200 * time.Millisecond},
		{name: "third attempt waits four times the base", attempt: 2, floor: 400 * time.Millisecond},
		{name: "late attempts are capped at max", attempt: 60, floor: 30 * time.Second},
		{name: "a negative attempt waits the base interval", attempt: -1, floor: 100 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := b.Delay(tt.attempt)
			if d < tt.floor || d >= 2*tt.floor {
				t.Errorf("delay %s outside [%s, %s)", d, tt.floor, 2*tt.floor)
			}
		})
	}
}

// TestBackoffWaitReportsCancellation verifies Wait distinguishes "waited" from
// "context ended". Callers treat false as "stop retrying and surrender the batch",
// so reporting true after cancellation would keep a shutting-down restore writing.
func TestBackoffWaitReportsCancellation(t *testing.T) {
	b := NewExponentialBackoff(time.Millisecond, time.Millisecond)

	if !b.Wait(context.Background(), 0) {
		t.Error("Wait reported cancellation on a live context")
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if b.Wait(cancelled, 0) {
		t.Error("Wait reported success on a cancelled context")
	}
}

// TestNewExponentialBackoffRejectsSpinningIntervals verifies construction fails loudly
// on intervals that would spin, rather than silently converting a restore into a busy loop.
func TestNewExponentialBackoffRejectsSpinningIntervals(t *testing.T) {
	tests := []struct {
		name string
		base time.Duration
		max  time.Duration
	}{
		{name: "zero base", base: 0, max: time.Second},
		{name: "negative base", base: -time.Second, max: time.Second},
		{name: "max below base", base: time.Second, max: time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected a panic for an interval that would spin")
				}
			}()
			NewExponentialBackoff(tt.base, tt.max)
		})
	}
}

// TestSubmitSendsTheCallersContext verifies every DynamoDB call is made under the
// context the caller passed. Detaching from it would leave a shutting-down restore
// writing to the table with no deadline and no way to stop it.
func TestSubmitSendsTheCallersContext(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	ctx := context.WithValue(context.Background(), callerContextKey{}, true)
	ops := append(putOps(1), updateOp())
	if err := writeAndWait(ctx, w, ops); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	if client.detached > 0 {
		t.Errorf("%d DynamoDB calls were made outside the caller's context", client.detached)
	}
}

// TestBatchWriteRetriesTransientErrorWithoutCountingThrottle verifies a non-throttling
// failure is retried and counted as a retry, but not as a throttle: the two metrics
// drive different operator decisions (raise capacity versus investigate the failure).
func TestBatchWriteRetriesTransientErrorWithoutCountingThrottle(t *testing.T) {
	client := &scriptedClient{batchErrs: []error{errors.New("connection reset"), nil}}
	counts := &callbackCounts{}
	backoff := &instantBackoff{}
	w := NewDynamoDBWriter(client, "test-table", counts.callbacks(), WithBackoff(backoff), withPaceClock(newTestClock()))

	if err := writeAndWait(context.Background(), w, putOps(1)); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	if counts.retries != 1 {
		t.Errorf("expected 1 retry report, got %d", counts.retries)
	}
	if counts.throttles != 0 {
		t.Errorf("expected no throttle report, got %d", counts.throttles)
	}
	if len(backoff.attempts) != 1 || backoff.attempts[0] != 0 {
		t.Errorf("expected a single wait for attempt 0, got %v", backoff.attempts)
	}
}

// TestBatchWriteSurrendersBatchAfterMaxRetries verifies a batch that keeps failing for
// a non-throttling reason is given a bounded number of attempts and its items are then
// reported lost. Retrying forever would stall the restore behind one poisoned batch.
func TestBatchWriteSurrendersBatchAfterMaxRetries(t *testing.T) {
	client := &scriptedClient{batchErrs: []error{errors.New("internal server error")}}
	counts := &callbackCounts{}
	w := NewDynamoDBWriter(client, "test-table", counts.callbacks(), WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	err := writeAndWait(context.Background(), w, putOps(4))
	if err == nil {
		t.Fatal("expected an error after the retry budget is spent")
	}

	// One initial attempt plus the retry budget.
	if len(client.batchRequests) != 6 {
		t.Errorf("expected 6 BatchWriteItem attempts, got %d", len(client.batchRequests))
	}
	if counts.lost != 4 {
		t.Errorf("expected all 4 items reported lost, got %d", counts.lost)
	}
}

// TestSubmitSendsNothingOnceStopped verifies a batch submitted after the restore was
// told to stop is refused with the context's error and never reaches the table. A
// stopping restore that went on writing would write past what its checkpoint records.
func TestSubmitSendsNothingOnceStopped(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := writeAndWait(ctx, w, putOps(1)); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if len(client.batchRequests) != 0 {
		t.Errorf("expected nothing sent, got %d calls", len(client.batchRequests))
	}
}

// TestWriteSurrendersBatchWhenBackoffStops verifies a write whose retry wait is cut
// short reports a failure rather than nothing.
//
// The wait is only cut short when the restore is stopping, and the batch has not been
// written. Returning no error would let the coordinator count those items as written and
// checkpoint past them, so an interrupted restore would resume having silently skipped a
// batch.
func TestWriteSurrendersBatchWhenBackoffStops(t *testing.T) {
	client := &scriptedClient{batchErrs: []error{errors.New("connection reset")}}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{}, WithBackoff(&stoppedBackoff{}))

	// The context is live, so a caller reading the context's own error would find
	// nothing wrong and report the batch as written.
	if err := writeAndWait(context.Background(), w, putOps(1)); err == nil {
		t.Error("expected a surrendered batch to be reported as an error")
	}
}

// TestBatchWriteReportsNoRetryOnFirstAttempt verifies a batch accepted immediately is
// not counted as a retry, keeping the retry metric a signal of transient trouble.
func TestBatchWriteReportsNoRetryOnFirstAttempt(t *testing.T) {
	client := &scriptedClient{}
	counts := &callbackCounts{}
	w := NewDynamoDBWriter(client, "test-table", counts.callbacks(), WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	if err := writeAndWait(context.Background(), w, putOps(1)); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	if counts.retries != 0 {
		t.Errorf("expected no retry reported, got %d", counts.retries)
	}
}

// TestUpdateOnlyBatchIsOneBatchWrite verifies a batch made up entirely of updates is
// written as one BatchWriteItem, the same as puts, rather than as one call per item.
func TestUpdateOnlyBatchIsOneBatchWrite(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	ops := []itemimage.Operation{updateOp(), updateOp(), updateOp()}
	if err := writeAndWait(context.Background(), w, ops); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	if len(client.batchRequests) != 1 || len(client.batchRequests[0]["test-table"]) != 3 {
		t.Errorf("expected one batch of 3 puts, got %v", client.batchRequests)
	}
}

// TestUnknownOperationTypeIsRefused verifies an operation of a kind the writer does
// not know fails the batch at once, rather than being dropped from it silently.
func TestUnknownOperationTypeIsRefused(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	if err := writeAndWait(context.Background(), w, []itemimage.Operation{{Type: itemimage.OperationType(99)}}); err == nil {
		t.Fatal("expected an unknown operation type refused")
	}
	if len(client.batchRequests) != 0 {
		t.Errorf("expected nothing sent, got %d batches", len(client.batchRequests))
	}
}

// putOps builds n single-attribute put operations with distinct keys.
func putOps(n int) []itemimage.Operation {
	ops := make([]itemimage.Operation, 0, n)
	for i := 0; i < n; i++ {
		ops = append(ops, itemimage.Operation{
			Type:  itemimage.OpPut,
			Bytes: int32(40 + i),
			NewImage: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "USER#" + string(rune('A'+i))},
			},
		})
	}
	return ops
}

// updateOp builds an update that sets one non-key attribute.
func updateOp() itemimage.Operation {
	return itemimage.Operation{
		Type:  itemimage.OpUpdate,
		Bytes: 60,
		Keys: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "USER#1"},
		},
		OldImage: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "USER#1"},
		},
		NewImage: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "USER#1"},
			"name": &types.AttributeValueMemberS{Value: "Jane"},
		},
	}
}

// throttle returns the error DynamoDB raises when capacity is exhausted.
func throttle() error {
	return &types.ProvisionedThroughputExceededException{Message: ptr("throttled")}
}

// callbackCounts tallies the writer's metric callbacks. The writer calls them from its
// own goroutines, so every count is taken under the lock.
type callbackCounts struct {
	lostByKind [itemimage.OperationKinds]int
	throttles  int
	retries    int
	lost       int
	mu         sync.Mutex
}

func (c *callbackCounts) callbacks() Callbacks {
	return Callbacks{
		OnThrottle: func() { c.mu.Lock(); c.throttles++; c.mu.Unlock() },
		OnRetry:    func() { c.mu.Lock(); c.retries++; c.mu.Unlock() },
		OnLost: func(kind itemimage.OperationType, n int) {
			c.mu.Lock()
			c.lost += n
			c.lostByKind[kind] += n
			c.mu.Unlock()
		},
	}
}

// submitAndWait submits ops and waits for the batch to finish, which is how a test reads
// the outcome of an asynchronous Submit.
func submitAndWait(ctx context.Context, w *DynamoDBWriter, ops []itemimage.Operation) (Rejection, error) {
	type outcome struct {
		err       error
		rejection Rejection
	}
	result := make(chan outcome, 1)
	if err := w.Submit(ctx, ops, func(r Rejection, err error) { result <- outcome{rejection: r, err: err} }); err != nil {
		return Rejection{}, err
	}
	out := <-result
	return out.rejection, out.err
}

// writeAndWait submits ops and reports only whether the batch failed.
func writeAndWait(ctx context.Context, w *DynamoDBWriter, ops []itemimage.Operation) error {
	_, err := submitAndWait(ctx, w, ops)
	return err
}

// instantBackoff removes the real waiting from retry tests while still honouring
// cancellation, which is the only property of the wait the retry loops depend on.
// It records the attempt numbers it was given so tests can assert the loop keeps
// counting up and therefore keeps backing off further.
type instantBackoff struct {
	attempts []int
}

func (b *instantBackoff) Wait(ctx context.Context, attempt int) bool {
	b.attempts = append(b.attempts, attempt)
	return ctx.Err() == nil
}

// stoppedBackoff stands for a wait cut short before the delay elapsed.
type stoppedBackoff struct{}

func (b *stoppedBackoff) Wait(ctx context.Context, attempt int) bool { return false }

// callerContextKey marks the context a test passed in, so a double can tell the caller's
// context from one the code under test substituted for it.
type callerContextKey struct{}

// scriptedClient replays a fixed sequence of DynamoDB outcomes so the retry loops can
// be driven deterministically. Once a script is exhausted its last entry repeats,
// which expresses a persistent failure as a single entry.
type scriptedClient struct {
	batchOutputs  []*dynamodb.BatchWriteItemOutput
	batchErrs     []error
	batchRequests []map[string][]types.WriteRequest
	detached      int // Calls that arrived without the caller's context
}

func (c *scriptedClient) noteContext(ctx context.Context) {
	if ctx.Value(callerContextKey{}) == nil {
		c.detached++
	}
}

func (c *scriptedClient) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	c.noteContext(ctx)
	// The writer mutates RequestItems in place when retrying, so snapshot the call.
	snapshot := make(map[string][]types.WriteRequest, len(params.RequestItems))
	for table, requests := range params.RequestItems {
		snapshot[table] = append([]types.WriteRequest(nil), requests...)
	}
	call := len(c.batchRequests)
	c.batchRequests = append(c.batchRequests, snapshot)

	if err := scriptedAt(c.batchErrs, call); err != nil {
		return nil, err
	}
	if out := scriptedAt(c.batchOutputs, call); out != nil {
		return out, nil
	}
	return &dynamodb.BatchWriteItemOutput{}, nil
}

// scriptedAt returns the entry for the given call, repeating the last one once the
// script runs out and the zero value when there is no script at all.
func scriptedAt[T any](script []T, call int) T {
	var zero T
	if len(script) == 0 {
		return zero
	}
	if call >= len(script) {
		return script[len(script)-1]
	}
	return script[call]
}
