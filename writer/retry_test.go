package writer

import (
	"context"
	"errors"
	"strings"
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

// TestBatchWriteRetriesThrottlingUntilSuccess verifies throttling is survived rather
// than surfaced: every throttle is reported, the batch is retried, and the eventual
// success counts as one retry regardless of how many throttles preceded it.
func TestBatchWriteRetriesThrottlingUntilSuccess(t *testing.T) {
	client := &scriptedClient{batchErrs: []error{throttle(), throttle(), nil}}
	counts := &callbackCounts{}
	backoff := &instantBackoff{}
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(), WithBackoff(backoff))

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
	// Each successive throttle must back off further, or a throttled table never recovers.
	if len(backoff.attempts) != 2 || backoff.attempts[0] != 0 || backoff.attempts[1] != 1 {
		t.Errorf("expected waits for attempts 0 and 1, got %v", backoff.attempts)
	}
}

// TestBatchWriteRetriesTransientErrorWithoutCountingThrottle verifies a non-throttling
// failure is retried and counted as a retry, but not as a throttle: the two metrics
// drive different operator decisions (raise capacity versus investigate the failure).
func TestBatchWriteRetriesTransientErrorWithoutCountingThrottle(t *testing.T) {
	client := &scriptedClient{batchErrs: []error{errors.New("connection reset"), nil}}
	counts := &callbackCounts{}
	backoff := &instantBackoff{}
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(), WithBackoff(backoff))

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
	backoff := &instantBackoff{}
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(), WithBackoff(backoff))

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
	// Repeated rejection must back off further each time, as it signals capacity pressure.
	if len(backoff.attempts) != 2 || backoff.attempts[0] != 0 || backoff.attempts[1] != 1 {
		t.Errorf("expected waits for attempts 0 and 1, got %v", backoff.attempts)
	}
}

// TestBatchWriteSurrendersBatchAfterMaxRetries verifies a batch that keeps failing for
// a non-throttling reason is given a bounded number of attempts and its items are then
// reported lost. Retrying forever would stall the restore behind one poisoned batch.
func TestBatchWriteSurrendersBatchAfterMaxRetries(t *testing.T) {
	client := &scriptedClient{batchErrs: []error{errors.New("internal server error")}}
	counts := &callbackCounts{}
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(), WithBackoff(&instantBackoff{}))

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
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{}, WithBackoff(&instantBackoff{}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := w.WriteBatch(ctx, putOps(1)); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// TestBatchWriteReportsNoRetryOnFirstAttempt verifies a batch accepted immediately is
// not counted as a retry, keeping the retry metric a signal of transient trouble.
func TestBatchWriteReportsNoRetryOnFirstAttempt(t *testing.T) {
	client := &scriptedClient{}
	counts := &callbackCounts{}
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(), WithBackoff(&instantBackoff{}))

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
	w := NewDynamoDBWriter(client, "test-table", 2, Callbacks{}, WithBackoff(&instantBackoff{}))

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

// TestBatchWriteContinuesPastUpdateOnlyBatch verifies a batch made up entirely of
// updates does not end the split loop: updates go out individually and the batches
// after them must still be written.
func TestBatchWriteContinuesPastUpdateOnlyBatch(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", 1, Callbacks{}, WithBackoff(&instantBackoff{}))

	ops := []itemimage.Operation{updateOp(), putOps(1)[0]}
	if err := w.WriteBatch(context.Background(), ops); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if len(client.batchRequests) != 1 {
		t.Errorf("expected the put following the update to be written, got %d batches", len(client.batchRequests))
	}
}

// TestBatchWriteReportsEstimatedBytes verifies the byte count handed to the metrics
// callback accumulates over the batch and covers deletes as well as puts, since it
// drives the reported throughput.
func TestBatchWriteReportsEstimatedBytes(t *testing.T) {
	client := &scriptedClient{}
	var gotItems, gotBytes int
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{
		OnWrite: func(items, bytes int) { gotItems, gotBytes = items, bytes },
	}, WithBackoff(&instantBackoff{}))

	// Items are estimated at 100 bytes base plus 50 per attribute: two single-attribute
	// puts at 150 each, plus a delete carrying two key attributes at 200.
	ops := append(putOps(2), itemimage.Operation{
		Type: itemimage.OpDelete,
		Keys: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "USER#9"},
			"SK": &types.AttributeValueMemberS{Value: "PROFILE"},
		},
	})
	if err := w.WriteBatch(context.Background(), ops); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if gotItems != 3 {
		t.Errorf("expected 3 items reported, got %d", gotItems)
	}
	if gotBytes != 500 {
		t.Errorf("expected 500 bytes reported, got %d", gotBytes)
	}
}

// TestUpdateItemBuildsSetAndRemoveExpression pins the update expression for an item
// that gains one attribute and loses another. Attributes present in the old image but
// absent from the new one must be removed, or a restored item keeps stale data.
func TestUpdateItemBuildsSetAndRemoveExpression(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{}, WithBackoff(&instantBackoff{}))

	op := itemimage.Operation{
		Type: itemimage.OpUpdate,
		Keys: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "USER#1"},
		},
		// "name" appears in both images and must be set, not removed.
		OldImage: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "USER#1"},
			"name": &types.AttributeValueMemberS{Value: "Janet"},
			"city": &types.AttributeValueMemberS{Value: "Stockholm"},
		},
		NewImage: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "USER#1"},
			"name": &types.AttributeValueMemberS{Value: "Jane"},
		},
	}

	if err := w.WriteBatch(context.Background(), []itemimage.Operation{op}); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if len(client.updateCalls) != 1 {
		t.Fatalf("expected 1 UpdateItem call, got %d", len(client.updateCalls))
	}
	input := client.updateCalls[0]

	const want = "SET #name = :name REMOVE #city"
	if got := *input.UpdateExpression; got != want {
		t.Errorf("update expression = %q, want %q", got, want)
	}
	if got := input.ExpressionAttributeNames; got["#name"] != "name" || got["#city"] != "city" || len(got) != 2 {
		t.Errorf("attribute names = %v, want #name and #city", got)
	}
	if got := input.ExpressionAttributeValues; len(got) != 1 || got[":name"] == nil {
		t.Errorf("attribute values = %v, want only :name", got)
	}
}

// TestUpdateItemOmitsValuesWhenOnlyRemoving verifies a removal-only update carries no
// expression values. DynamoDB rejects an UpdateItem that declares values it never uses.
func TestUpdateItemOmitsValuesWhenOnlyRemoving(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{}, WithBackoff(&instantBackoff{}))

	op := itemimage.Operation{
		Type: itemimage.OpUpdate,
		Keys: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "USER#1"},
		},
		OldImage: map[string]types.AttributeValue{
			"PK":      &types.AttributeValueMemberS{Value: "USER#1"},
			"city":    &types.AttributeValueMemberS{Value: "Stockholm"},
			"zip":     &types.AttributeValueMemberS{Value: "11122"},
			"street":  &types.AttributeValueMemberS{Value: "Kungsgatan"},
			"country": &types.AttributeValueMemberS{Value: "SE"},
			"phone":   &types.AttributeValueMemberS{Value: "+46"},
			"email":   &types.AttributeValueMemberS{Value: "jane@example.com"},
		},
		NewImage: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "USER#1"},
		},
	}

	if err := w.WriteBatch(context.Background(), []itemimage.Operation{op}); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if len(client.updateCalls) != 1 {
		t.Fatalf("expected 1 UpdateItem call, got %d", len(client.updateCalls))
	}
	input := client.updateCalls[0]

	if input.ExpressionAttributeValues != nil {
		t.Errorf("expected no expression values, got %v", input.ExpressionAttributeValues)
	}
	// Every non-key attribute of the old image must be removed, whatever order the
	// image is walked in; a partial REMOVE leaves stale attributes on the item.
	expr := *input.UpdateExpression
	if !strings.HasPrefix(expr, "REMOVE ") {
		t.Fatalf("update expression = %q, want a REMOVE clause", expr)
	}
	for _, attr := range []string{"#city", "#zip", "#street", "#country", "#phone", "#email"} {
		if !strings.Contains(expr, attr) {
			t.Errorf("update expression %q is missing %s", expr, attr)
		}
	}
	if got := len(input.ExpressionAttributeNames); got != 6 {
		t.Errorf("expected 6 attribute names, got %d", got)
	}
}

// TestUpdateItemSkipsKeyOnlyChange verifies an update that touches nothing but the key
// attributes is not sent. DynamoDB rejects an UpdateItem with an empty expression.
func TestUpdateItemSkipsKeyOnlyChange(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{}, WithBackoff(&instantBackoff{}))

	op := itemimage.Operation{
		Type: itemimage.OpUpdate,
		Keys: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "USER#1"},
		},
		OldImage: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "USER#1"},
		},
		NewImage: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "USER#1"},
		},
	}

	if err := w.WriteBatch(context.Background(), []itemimage.Operation{op}); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if len(client.updateCalls) != 0 {
		t.Errorf("expected no UpdateItem call, got %d", len(client.updateCalls))
	}
}

// TestUpdateItemOnlySetsChangedAttributes verifies an update whose old image holds
// nothing but the key produces a bare SET, with no dangling REMOVE clause.
func TestUpdateItemOnlySetsChangedAttributes(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{}, WithBackoff(&instantBackoff{}))

	if err := w.WriteBatch(context.Background(), []itemimage.Operation{updateOp()}); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if len(client.updateCalls) != 1 {
		t.Fatalf("expected 1 UpdateItem call, got %d", len(client.updateCalls))
	}
	const want = "SET #name = :name"
	if got := *client.updateCalls[0].UpdateExpression; got != want {
		t.Errorf("update expression = %q, want %q", got, want)
	}
}

// TestUpdateItemRetriesThrottlingUntilSuccess verifies individual updates survive
// throttling the same way batches do; updates bypass BatchWriteItem entirely.
func TestUpdateItemRetriesThrottlingUntilSuccess(t *testing.T) {
	client := &scriptedClient{updateErrs: []error{throttle(), throttle(), nil}}
	counts := &callbackCounts{}
	backoff := &instantBackoff{}
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(), WithBackoff(backoff))

	if err := w.WriteBatch(context.Background(), []itemimage.Operation{updateOp()}); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if counts.throttles != 2 {
		t.Errorf("expected 2 throttle reports, got %d", counts.throttles)
	}
	if counts.retries != 1 {
		t.Errorf("expected 1 retry report, got %d", counts.retries)
	}
	if len(backoff.attempts) != 2 || backoff.attempts[0] != 0 || backoff.attempts[1] != 1 {
		t.Errorf("expected waits for attempts 0 and 1, got %v", backoff.attempts)
	}
}

// TestUpdateItemRetriesTransientErrorWithoutCountingThrottle verifies a non-throttling
// update failure is retried and counted as a retry rather than as capacity pressure.
func TestUpdateItemRetriesTransientErrorWithoutCountingThrottle(t *testing.T) {
	client := &scriptedClient{updateErrs: []error{errors.New("connection reset"), nil}}
	counts := &callbackCounts{}
	backoff := &instantBackoff{}
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(), WithBackoff(backoff))

	if err := w.WriteBatch(context.Background(), []itemimage.Operation{updateOp()}); err != nil {
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

// TestUpdateItemSurrendersAfterMaxRetries verifies a persistently failing update is
// given the same bounded budget as a batch and is then reported as a single lost item.
func TestUpdateItemSurrendersAfterMaxRetries(t *testing.T) {
	client := &scriptedClient{updateErrs: []error{errors.New("internal server error")}}
	counts := &callbackCounts{}
	w := NewDynamoDBWriter(client, "test-table", 25, counts.callbacks(), WithBackoff(&instantBackoff{}))

	if err := w.WriteBatch(context.Background(), []itemimage.Operation{updateOp()}); err == nil {
		t.Fatal("expected an error after the retry budget is spent")
	}

	if len(client.updateCalls) != 6 {
		t.Errorf("expected 6 UpdateItem attempts, got %d", len(client.updateCalls))
	}
	if counts.lost != 1 {
		t.Errorf("expected 1 lost item, got %d", counts.lost)
	}
}

// TestUpdateItemReportsOneItemWritten verifies an update reports exactly one item to
// the metrics callback, since updates are not batched, and reports no retry when it
// is accepted on the first attempt.
func TestUpdateItemReportsOneItemWritten(t *testing.T) {
	client := &scriptedClient{}
	counts := &callbackCounts{}
	var gotItems, gotBytes int
	callbacks := counts.callbacks()
	callbacks.OnWrite = func(items, bytes int) { gotItems, gotBytes = items, bytes }
	w := NewDynamoDBWriter(client, "test-table", 25, callbacks, WithBackoff(&instantBackoff{}))

	if err := w.WriteBatch(context.Background(), []itemimage.Operation{updateOp()}); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if gotItems != 1 {
		t.Errorf("expected 1 item reported, got %d", gotItems)
	}
	// The new image carries the key plus one attribute: 100 base + 2*50.
	if gotBytes != 200 {
		t.Errorf("expected 200 bytes reported, got %d", gotBytes)
	}
	if counts.retries != 0 {
		t.Errorf("expected no retry reported, got %d", counts.retries)
	}
}

// putOps builds n single-attribute put operations with distinct keys.
func putOps(n int) []itemimage.Operation {
	ops := make([]itemimage.Operation, 0, n)
	for i := 0; i < n; i++ {
		ops = append(ops, itemimage.Operation{
			Type: itemimage.OpPut,
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
		Type: itemimage.OpUpdate,
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

// scriptedClient replays a fixed sequence of DynamoDB outcomes so the retry loops can
// be driven deterministically. Once a script is exhausted its last entry repeats,
// which expresses a persistent failure as a single entry.
type scriptedClient struct {
	batchOutputs  []*dynamodb.BatchWriteItemOutput
	batchErrs     []error
	updateErrs    []error
	batchRequests []map[string][]types.WriteRequest
	updateCalls   []*dynamodb.UpdateItemInput
}

func (c *scriptedClient) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
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

func (c *scriptedClient) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	call := len(c.updateCalls)
	c.updateCalls = append(c.updateCalls, params)

	if err := scriptedAt(c.updateErrs, call); err != nil {
		return nil, err
	}
	return &dynamodb.UpdateItemOutput{}, nil
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
