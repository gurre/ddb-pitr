package writer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
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

// TestNewDynamoDBWriterRejectsUnusableBatchSizes verifies construction fails loudly on a
// batch size DynamoDB cannot serve. A non-positive size leaves the split loop unable to
// advance, which hangs the restore rather than failing it; a size above 25 builds a
// request DynamoDB rejects on every batch.
func TestNewDynamoDBWriterRejectsUnusableBatchSizes(t *testing.T) {
	for _, size := range []int{0, -1, 26, 100} {
		t.Run(fmt.Sprintf("size %d", size), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("expected a panic for batch size %d", size)
				}
			}()
			NewDynamoDBWriter(&scriptedClient{}, "test-table", size, Callbacks{})
		})
	}
}

// TestNewDynamoDBWriterAcceptsTheBatchSizeLimits verifies the ends of DynamoDB's range
// are usable, so the fail-fast check does not reject a legitimate configuration.
func TestNewDynamoDBWriterAcceptsTheBatchSizeLimits(t *testing.T) {
	for _, size := range []int{1, 25} {
		t.Run(fmt.Sprintf("size %d", size), func(t *testing.T) {
			client := &scriptedClient{}
			w := NewDynamoDBWriter(client, "test-table", size, Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))
			if err := w.WriteBatch(context.Background(), putOps(size)); err != nil {
				t.Fatalf("WriteBatch failed: %v", err)
			}
			if len(client.batchRequests) != 1 {
				t.Errorf("expected 1 BatchWriteItem call, got %d", len(client.batchRequests))
			}
		})
	}
}

// TestWriteBatchSendsTheCallersContext verifies every DynamoDB call is made under the
// context the caller passed. Detaching from it would leave a shutting-down restore
// writing to the table with no deadline and no way to stop it.
func TestWriteBatchSendsTheCallersContext(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	ctx := context.WithValue(context.Background(), callerContextKey{}, true)
	ops := append(putOps(1), updateOp())
	if err := w.WriteBatch(ctx, ops); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if client.detached > 0 {
		t.Errorf("%d DynamoDB calls were made outside the caller's context", client.detached)
	}
}

// TestBackoffWaitSendsTheCallersContext verifies the retry wait watches the caller's
// context. A wait detached from it would ignore shutdown and hold the restore open for
// the full backoff, which grows to tens of seconds under throttling.
func TestBackoffWaitSendsTheCallersContext(t *testing.T) {
	client := &scriptedClient{batchErrs: []error{throttle(), nil}}
	backoff := &contextRecordingBackoff{}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{}, WithBackoff(backoff), withPaceClock(newTestClock()))

	ctx := context.WithValue(context.Background(), callerContextKey{}, true)
	if err := w.WriteBatch(ctx, putOps(1)); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if backoff.detached > 0 {
		t.Errorf("%d waits were made outside the caller's context", backoff.detached)
	}
}

// TestBatchWriteRetriesThrottlingUntilSuccess verifies throttling is survived rather
// than surfaced: every throttle is reported, the batch is retried, and the eventual
// success counts as one retry regardless of how many throttles preceded it.
func TestBatchWriteRetriesThrottlingUntilSuccess(t *testing.T) {
	client := &scriptedClient{batchErrs: []error{throttle(), throttle(), nil}}
	counts := &callbackCounts{}
	clock := newTestClock()
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(),
		WithBackoff(&instantBackoff{}), withPaceClock(clock))

	if err := w.WriteBatch(context.Background(), putOps(1)); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if counts.throttles != 2 {
		t.Errorf("expected 2 throttle reports, got %d", counts.throttles)
	}
	if counts.retries != 1 {
		t.Errorf("expected 1 retry report, got %d", counts.retries)
	}
	if counts.lost != 0 {
		t.Errorf("expected no lost items, got %d", counts.lost)
	}
	// Each throttled attempt waits for the capacity it needs before it goes again, or
	// a throttled table is simply asked the same question until it answers.
	if waits := clock.waits(); len(waits) != 2 || waits[0] <= 0 || waits[1] <= 0 {
		t.Errorf("expected both retries to wait for capacity, got %v", waits)
	}
}

// TestBatchWriteRetriesTransientErrorWithoutCountingThrottle verifies a non-throttling
// failure is retried and counted as a retry, but not as a throttle: the two metrics
// drive different operator decisions (raise capacity versus investigate the failure).
func TestBatchWriteRetriesTransientErrorWithoutCountingThrottle(t *testing.T) {
	client := &scriptedClient{batchErrs: []error{errors.New("connection reset"), nil}}
	counts := &callbackCounts{}
	backoff := &instantBackoff{}
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(), WithBackoff(backoff), withPaceClock(newTestClock()))

	if err := w.WriteBatch(context.Background(), putOps(1)); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
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

// TestBatchWriteResubmitsOnlyUnprocessedItems verifies a partially accepted batch is
// retried with exactly the items DynamoDB rejected. Resubmitting the whole batch would
// duplicate work; dropping the remainder would silently lose items.
func TestBatchWriteResubmitsOnlyUnprocessedItems(t *testing.T) {
	leftover := []types.WriteRequest{{PutRequest: &types.PutRequest{
		Item: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "USER#1"}},
	}}}
	client := &scriptedClient{batchOutputs: []*dynamodb.BatchWriteItemOutput{
		{UnprocessedItems: map[string][]types.WriteRequest{"test-table": leftover}},
		{UnprocessedItems: map[string][]types.WriteRequest{"test-table": leftover}},
		{},
	}}
	counts := &callbackCounts{}
	clock := newTestClock()
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(),
		WithBackoff(&instantBackoff{}), withPaceClock(clock))

	if err := w.WriteBatch(context.Background(), putOps(3)); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if len(client.batchRequests) != 3 {
		t.Fatalf("expected 3 BatchWriteItem calls, got %d", len(client.batchRequests))
	}
	if got := len(client.batchRequests[1]["test-table"]); got != 1 {
		t.Errorf("expected the retry to carry 1 unprocessed item, got %d", got)
	}
	if counts.throttles != 2 {
		t.Errorf("expected unprocessed items to report 2 throttles, got %d", counts.throttles)
	}
	if counts.retries != 1 {
		t.Errorf("expected the resubmission to be reported as 1 retry, got %d", counts.retries)
	}
	// Each rejection waits for the capacity the resubmission needs; resending it at
	// once would just collect the same rejection.
	if waits := clock.waits(); len(waits) != 2 || waits[0] <= 0 || waits[1] <= 0 {
		t.Errorf("expected both resubmissions to wait for capacity, got %v", waits)
	}
}

// TestBatchWriteSurrendersBatchAfterMaxRetries verifies a batch that keeps failing for
// a non-throttling reason is given a bounded number of attempts and its items are then
// reported lost. Retrying forever would stall the restore behind one poisoned batch.
func TestBatchWriteSurrendersBatchAfterMaxRetries(t *testing.T) {
	client := &scriptedClient{batchErrs: []error{errors.New("internal server error")}}
	counts := &callbackCounts{}
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(), WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	err := w.WriteBatch(context.Background(), putOps(4))
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
	if counts.writes != 0 {
		t.Errorf("expected no write reported, got %d", counts.writes)
	}
}

// TestBatchWriteStopsRetryingWhenContextEnds verifies cancellation ends the retry loop
// with the context error. Throttling retries are otherwise unbounded, so cancellation
// is the only thing that stops them during shutdown.
func TestBatchWriteStopsRetryingWhenContextEnds(t *testing.T) {
	client := &scriptedClient{batchErrs: []error{throttle()}}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := w.WriteBatch(ctx, putOps(1)); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
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
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{}, WithBackoff(&stoppedBackoff{}))

	// The context is live, so a caller reading the context's own error would find
	// nothing wrong and report the batch as written.
	if err := w.WriteBatch(context.Background(), putOps(1)); err == nil {
		t.Error("expected a surrendered batch to be reported as an error")
	}
}

// TestWriteSurrendersBatchWhenTheWaitForCapacityIsCutShort verifies the same for the
// other wait: a throttled batch waits for the table to have capacity, and a restore
// stopping during that wait must surrender the batch rather than report it written.
func TestWriteSurrendersBatchWhenTheWaitForCapacityIsCutShort(t *testing.T) {
	client := &scriptedClient{batchErrs: []error{throttle()}}
	clock := newTestClock()
	clock.refuse = true
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{},
		WithBackoff(&instantBackoff{}), withPaceClock(clock))

	if err := w.WriteBatch(context.Background(), putOps(1)); err == nil {
		t.Error("expected a surrendered batch to be reported as an error")
	}
}

// TestBatchWriteReportsNoRetryOnFirstAttempt verifies a batch accepted immediately is
// not counted as a retry, keeping the retry metric a signal of transient trouble.
func TestBatchWriteReportsNoRetryOnFirstAttempt(t *testing.T) {
	client := &scriptedClient{}
	counts := &callbackCounts{}
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(), WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	if err := w.WriteBatch(context.Background(), putOps(1)); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if counts.retries != 0 {
		t.Errorf("expected no retry reported, got %d", counts.retries)
	}
}

// TestBatchWriteSplitsOversizedInput verifies operations beyond the batch size are
// split across calls instead of being sent in one oversized request, which DynamoDB rejects.
func TestBatchWriteSplitsOversizedInput(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", 2, Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	if err := w.WriteBatch(context.Background(), putOps(5)); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	var sizes []int
	for _, req := range client.batchRequests {
		sizes = append(sizes, len(req["test-table"]))
	}
	if len(sizes) != 3 || sizes[0] != 2 || sizes[1] != 2 || sizes[2] != 1 {
		t.Errorf("expected batches of 2, 2 and 1, got %v", sizes)
	}
}

// TestUpdateOnlyBatchIsOneBatchWrite verifies a batch made up entirely of updates is
// written as one BatchWriteItem, the same as puts, rather than as one call per item.
func TestUpdateOnlyBatchIsOneBatchWrite(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	ops := []itemimage.Operation{updateOp(), updateOp(), updateOp()}
	if err := w.WriteBatch(context.Background(), ops); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if len(client.batchRequests) != 1 || len(client.batchRequests[0]["test-table"]) != 3 {
		t.Errorf("expected one batch of 3 puts, got %v", client.batchRequests)
	}
}

// TestReportedBytesAreTheLineLengthsRead verifies the byte count handed to the metrics
// callback is the sum of the export lines behind the batch, deletes included. That is
// what the report calls data read, so it has to be bytes that were actually
// transferred rather than a figure derived from attribute counts.
func TestReportedBytesAreTheLineLengthsRead(t *testing.T) {
	client := &scriptedClient{}
	var gotItems, gotBytes int
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{
		OnWrite: func(items, bytes int) { gotItems, gotBytes = items, bytes },
	}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	ops := append(putOps(2), itemimage.Operation{
		Type:  itemimage.OpDelete,
		Bytes: 70,
		Keys: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "USER#9"},
		},
	})
	want := 0
	for _, op := range ops {
		want += int(op.Bytes)
	}
	if err := w.WriteBatch(context.Background(), ops); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if gotItems != 3 {
		t.Errorf("expected 3 items reported, got %d", gotItems)
	}
	if gotBytes != want {
		t.Errorf("expected %d bytes reported, got %d", want, gotBytes)
	}
}

// TestUnprocessedRoundsDoNotConsumeTheTransientBudget verifies a batch that DynamoDB
// keeps accepting in part, then fails once for a passing reason, is still written. A
// hot partition routinely hands items back unprocessed several rounds running; if
// those rounds counted against the bounded budget, the first passing fault after them
// would declare the batch lost.
func TestUnprocessedRoundsDoNotConsumeTheTransientBudget(t *testing.T) {
	leftover := []types.WriteRequest{{PutRequest: &types.PutRequest{
		Item: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "USER#1"}},
	}}}
	partial := &dynamodb.BatchWriteItemOutput{UnprocessedItems: map[string][]types.WriteRequest{"test-table": leftover}}
	client := &scriptedClient{
		batchOutputs: []*dynamodb.BatchWriteItemOutput{partial, partial, partial, partial, partial, nil, nil},
		batchErrs:    []error{nil, nil, nil, nil, nil, &types.InternalServerError{Message: ptr("500")}, nil},
	}
	counts := &callbackCounts{}
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(), WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	if err := w.WriteBatch(context.Background(), putOps(3)); err != nil {
		t.Fatalf("expected the batch written after the fault, got %v", err)
	}

	if counts.lost != 0 {
		t.Errorf("expected no items lost, got %d", counts.lost)
	}
	if counts.throttles != 5 {
		t.Errorf("expected the 5 partial rounds counted as throttles, got %d", counts.throttles)
	}
	if counts.writes != 3 {
		t.Errorf("expected the 3 items reported written, got %d", counts.writes)
	}
}

// TestEveryThrottleKindIsRetriedUntilSuccess verifies each way DynamoDB says "not now"
// is treated as throttling: retried past the bounded budget and counted as a throttle.
// Missing one would send a throttled restore down the bounded path and lose batches
// under sustained pressure, which is exactly when a restore is most likely to be
// running against a table it shares.
func TestEveryThrottleKindIsRetriedUntilSuccess(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"provisioned throughput exceeded", &types.ProvisionedThroughputExceededException{Message: ptr("throttled")}},
		{"request limit exceeded", &types.RequestLimitExceeded{Message: ptr("throttled")}},
		{"ThrottlingException", &smithy.GenericAPIError{Code: "ThrottlingException", Message: "Rate exceeded"}},
		{"RequestThrottled", &smithy.GenericAPIError{Code: "RequestThrottled", Message: "Rate exceeded"}},
		{"ThrottledException", &smithy.GenericAPIError{Code: "ThrottledException", Message: "Rate exceeded"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// More throttles than the bounded budget allows, then success.
			errs := make([]error, 0, maxTransientAttempts+2)
			for i := 0; i <= maxTransientAttempts; i++ {
				errs = append(errs, tt.err)
			}
			errs = append(errs, nil)
			client := &scriptedClient{batchErrs: errs}
			counts := &callbackCounts{}
			w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(), WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

			if err := w.WriteBatch(context.Background(), putOps(1)); err != nil {
				t.Fatalf("expected the batch written once throttling eased, got %v", err)
			}
			if counts.throttles != maxTransientAttempts+1 || counts.lost != 0 {
				t.Errorf("expected %d throttles and nothing lost, got %d and %d",
					maxTransientAttempts+1, counts.throttles, counts.lost)
			}
		})
	}
}

// TestUnknownOperationTypeIsRefused verifies an operation of a kind the writer does
// not know fails the batch at once, rather than being dropped from it silently.
func TestUnknownOperationTypeIsRefused(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	if err := w.WriteBatch(context.Background(), []itemimage.Operation{{Type: itemimage.OperationType(99)}}); err == nil {
		t.Fatal("expected an unknown operation type refused")
	}
	if len(client.batchRequests) != 0 {
		t.Errorf("expected nothing sent, got %d batches", len(client.batchRequests))
	}
}

// TestLostCountIsWhatRemainedUnwritten verifies a batch given up after part of it was
// accepted reports only the leftover as lost. Reporting the original batch size would
// overstate the loss, and the lost count is what an operator uses to judge the restore.
func TestLostCountIsWhatRemainedUnwritten(t *testing.T) {
	leftover := []types.WriteRequest{{PutRequest: &types.PutRequest{
		Item: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "USER#1"}},
	}}}
	client := &scriptedClient{
		batchOutputs: []*dynamodb.BatchWriteItemOutput{{UnprocessedItems: map[string][]types.WriteRequest{"test-table": leftover}}},
		batchErrs:    []error{nil, errors.New("validation failed")},
	}
	counts := &callbackCounts{}
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(), WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	if err := w.WriteBatch(context.Background(), putOps(3)); err == nil {
		t.Fatal("expected the batch given up")
	}

	if counts.lost != 1 {
		t.Errorf("expected only the 1 unwritten item reported lost, got %d", counts.lost)
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

// callbackCounts tallies the writer's metric callbacks.
type callbackCounts struct {
	throttles int
	retries   int
	lost      int
	writes    int
}

func (c *callbackCounts) callbacks() Callbacks {
	return Callbacks{
		OnThrottle: func() { c.throttles++ },
		OnRetry:    func() { c.retries++ },
		OnLost:     func(count int) { c.lost += count },
		OnWrite:    func(items, bytes int) { c.writes += items },
	}
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

// contextRecordingBackoff counts waits that arrived without the caller's context.
type contextRecordingBackoff struct {
	detached int
}

func (b *contextRecordingBackoff) Wait(ctx context.Context, attempt int) bool {
	if ctx.Value(callerContextKey{}) == nil {
		b.detached++
	}
	return ctx.Err() == nil
}

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
