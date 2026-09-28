package writer

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/gurre/ddb-pitr/itemimage"
)

// TestSubmitReportsNothingRejectedWhenTheTableTakesEverything verifies a batch the table
// takes whole comes back with nothing to send again, in one call. Anything named as
// rejected here would be written twice, and a second call would be a round trip for
// nothing.
func TestSubmitReportsNothingRejectedWhenTheTableTakesEverything(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	rejection, err := submitAndWait(t.Context(), w, putOps(25))
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if len(rejection.Refused)+len(rejection.HandedBack) != 0 || len(client.batchRequests) != 1 {
		t.Errorf("expected one call and nothing rejected, got %d calls and %+v", len(client.batchRequests), rejection)
	}
}

// TestSubmitReportsAnEmptyBatchDone verifies an empty batch completes without a call,
// so a caller waiting on every batch it submitted is not left waiting on this one.
func TestSubmitReportsAnEmptyBatchDone(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{})

	if err := writeAndWait(t.Context(), w, nil); err != nil || len(client.batchRequests) != 0 {
		t.Errorf("expected no call and no error, got %d calls and %v", len(client.batchRequests), err)
	}
}

// TestSubmitRefusesMoreThanOneCallTakes verifies a batch larger than DynamoDB accepts in
// one call is a wiring mistake that fails at once, rather than a request DynamoDB would
// reject on every batch.
func TestSubmitRefusesMoreThanOneCallTakes(t *testing.T) {
	w := NewDynamoDBWriter(&scriptedClient{}, "test-table", Callbacks{})
	defer func() {
		if recover() == nil {
			t.Error("expected a batch of 26 refused")
		}
	}()
	_ = w.Submit(t.Context(), putOps(MaxBatch+1), func(Rejection, error) {})
}

// TestRefusedCallIsReportedRatherThanResent verifies a call the table refused comes back
// to the caller at once, named in full, with the refusal counted and the rate cut when
// its window closes. The
// writer holding it to resend itself would keep a slot occupied waiting on capacity,
// while the caller can mix those items into batches that go out when capacity returns.
func TestRefusedCallIsReportedRatherThanResent(t *testing.T) {
	throttles := []struct {
		name string
		err  error
	}{
		{"provisioned throughput exceeded", &types.ProvisionedThroughputExceededException{Message: ptr("throttled")}},
		{"request limit exceeded", &types.RequestLimitExceeded{Message: ptr("throttled")}},
		{"ThrottlingException", &smithy.GenericAPIError{Code: "ThrottlingException"}},
		{"RequestThrottled", &smithy.GenericAPIError{Code: "RequestThrottled"}},
		{"ThrottledException", &smithy.GenericAPIError{Code: "ThrottledException"}},
	}
	for _, tt := range throttles {
		t.Run(tt.name, func(t *testing.T) {
			client := &scriptedClient{batchErrs: []error{tt.err}}
			counts := &callbackCounts{}
			clock := newTestClock()
			w := NewDynamoDBWriter(client, "test-table", counts.callbacks(), WithBackoff(&instantBackoff{}), withPaceClock(clock))

			rejection, err := submitAndWait(t.Context(), w, putOps(3))
			if err != nil {
				t.Fatalf("expected a refusal reported, not a failure: %v", err)
			}
			if !slices.Equal(rejection.Refused, []int{0, 1, 2}) || len(client.batchRequests) != 1 {
				t.Errorf("expected all 3 reported refused after one call, got %+v after %d calls", rejection, len(client.batchRequests))
			}
			// The refusal is weighed when the window it fell in closes.
			clock.advance(paceWindow)
			w.pacer.take(1, 1)
			if _, paced := w.pacer.current(); !paced || counts.throttles != 1 {
				t.Errorf("expected the refusal counted and the rate cut once its window closed, got %d throttles (paced %t)", counts.throttles, paced)
			}
		})
	}
}

// TestHandedBackItemsAreNamedByWhatTheyHold verifies the items a call hands back are
// named by their place in the batch, however DynamoDB orders them. Naming the wrong ones
// writes some items twice and never writes the others, and nothing reports it: the
// restore finishes clean with items missing from the table.
func TestHandedBackItemsAreNamedByWhatTheyHold(t *testing.T) {
	ops := putOps(5)
	back := []types.WriteRequest{
		{PutRequest: &types.PutRequest{Item: clone(ops[3].NewImage)}},
		{PutRequest: &types.PutRequest{Item: clone(ops[1].NewImage)}},
	}
	client := &scriptedClient{batchOutputs: []*dynamodb.BatchWriteItemOutput{
		{UnprocessedItems: map[string][]types.WriteRequest{"test-table": back}},
	}}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	rejection, err := submitAndWait(t.Context(), w, ops)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	got := slices.Sorted(slices.Values(rejection.HandedBack))
	if !slices.Equal(got, []int{1, 3}) || len(client.batchRequests) != 1 {
		t.Errorf("expected items 1 and 3 handed back after one call, got %v after %d calls", got, len(client.batchRequests))
	}
}

// TestUnrecognisedHandedBackItemReportsTheWholeCall verifies a response handing back an
// item that matches nothing sent names every item of the call. Guessing would risk
// leaving an item unwritten; writing some twice costs only capacity.
func TestUnrecognisedHandedBackItemReportsTheWholeCall(t *testing.T) {
	stranger := []types.WriteRequest{{PutRequest: &types.PutRequest{
		Item: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "NOBODY"}},
	}}}
	client := &scriptedClient{batchOutputs: []*dynamodb.BatchWriteItemOutput{
		{UnprocessedItems: map[string][]types.WriteRequest{"test-table": stranger}},
	}}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	rejection, err := submitAndWait(t.Context(), w, putOps(3))
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if !slices.Equal(rejection.HandedBack, []int{0, 1, 2}) {
		t.Errorf("expected the whole call handed back, got %v", rejection.HandedBack)
	}
}

// TestHandedBackMatchingTellsEveryAttributeTypeApart verifies two items differing only in
// one attribute of each type DynamoDB stores are told apart, and identical ones are not.
// An item wrongly taken for another is an item never sent again.
func TestHandedBackMatchingTellsEveryAttributeTypeApart(t *testing.T) {
	values := []struct {
		a, b types.AttributeValue
	}{
		{&types.AttributeValueMemberS{Value: "x"}, &types.AttributeValueMemberS{Value: "y"}},
		{&types.AttributeValueMemberN{Value: "1"}, &types.AttributeValueMemberN{Value: "2"}},
		{&types.AttributeValueMemberB{Value: []byte{1}}, &types.AttributeValueMemberB{Value: []byte{2}}},
		{&types.AttributeValueMemberBOOL{Value: true}, &types.AttributeValueMemberBOOL{Value: false}},
		{&types.AttributeValueMemberNULL{Value: true}, &types.AttributeValueMemberS{Value: ""}},
		{&types.AttributeValueMemberSS{Value: []string{"a"}}, &types.AttributeValueMemberSS{Value: []string{"a", "b"}}},
		{&types.AttributeValueMemberNS{Value: []string{"1"}}, &types.AttributeValueMemberNS{Value: []string{"2"}}},
		{&types.AttributeValueMemberBS{Value: [][]byte{{1}}}, &types.AttributeValueMemberBS{Value: [][]byte{{2}}}},
		{&types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberS{Value: "x"}}},
			&types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberN{Value: "x"}}}},
		{&types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"k": &types.AttributeValueMemberS{Value: "x"}}},
			&types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"k": &types.AttributeValueMemberS{Value: "y"}}}},
	}
	for _, v := range values {
		if !sameValue(v.a, v.a) {
			t.Errorf("%T not equal to itself", v.a)
		}
		if sameValue(v.a, v.b) {
			t.Errorf("%T %v taken for %v", v.a, v.a, v.b)
		}
	}
}

// TestTransientErrorIsRetriedWithoutCountingAThrottle verifies a failure that is not the
// table refusing work is retried, counted as a retry and not as a throttle: the two
// counts drive different operator decisions, raising capacity against investigating.
func TestTransientErrorIsRetriedWithoutCountingAThrottle(t *testing.T) {
	client := &scriptedClient{batchErrs: []error{errors.New("connection reset"), nil}}
	counts := &callbackCounts{}
	w := NewDynamoDBWriter(client, "test-table", counts.callbacks(), WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	rejection, err := submitAndWait(t.Context(), w, putOps(1))
	if err != nil || len(rejection.Refused)+len(rejection.HandedBack) != 0 {
		t.Fatalf("expected the batch written after the retry, got %+v, %v", rejection, err)
	}
	if counts.retries != 1 || counts.throttles != 0 {
		t.Errorf("expected 1 retry and no throttle, got %d and %d", counts.retries, counts.throttles)
	}
}

// TestLostItemsAreCountedByKind verifies a batch given up names what it lost by kind,
// covering only what had not been sent. The report tells an operator which kind of
// change the table is missing, and counting items already accepted would overstate it.
func TestLostItemsAreCountedByKind(t *testing.T) {
	del := itemimage.Operation{
		Type: itemimage.OpDelete,
		Keys: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "USER#9"}},
	}
	ops := []itemimage.Operation{putOps(1)[0], putOps(2)[1], updateOp(), del}
	// The first call carries two items and is accepted; the second fails for good.
	client := &scriptedClient{batchErrs: []error{nil, errors.New("validation failed")}}
	counts := &callbackCounts{}
	clock := newTestClock()
	w := NewDynamoDBWriter(client, "test-table", counts.callbacks(), WithBackoff(&instantBackoff{}), withPaceClock(clock))
	w.pacer.setRateLocked(clock.Now(), 2)
	clock.advance(time.Second)

	if err := writeAndWait(t.Context(), w, ops); err == nil {
		t.Fatal("expected the batch given up")
	}
	want := [itemimage.OperationKinds]int{itemimage.OpUpdate: 1, itemimage.OpDelete: 1}
	if counts.lostByKind != want {
		t.Errorf("lost by kind = %v, want %v", counts.lostByKind, want)
	}
}

// TestSubmitWaitsForASlotAndGivesUpWhenStopped verifies Submit holds a batch back while
// as many are in flight as the table is being given, and returns the context's error if
// the restore stops meanwhile, without calling done. A caller counting batches in
// flight would otherwise wait for a batch that never started.
func TestSubmitWaitsForASlotAndGivesUpWhenStopped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		w := NewDynamoDBWriter(blockingClient{release: release}, "test-table", Callbacks{})
		defer close(release)
		for range initialConcurrency {
			if err := w.Submit(t.Context(), putOps(1), func(Rejection, error) {}); err != nil {
				t.Fatalf("Submit within the limit failed: %v", err)
			}
		}

		ctx, cancel := context.WithCancel(t.Context())
		called := false
		submitted := make(chan error, 1)
		go func() {
			submitted <- w.Submit(ctx, putOps(1), func(Rejection, error) { called = true })
		}()
		synctest.Wait() // Waiting for a slot.
		cancel()
		if err := <-submitted; !errors.Is(err, context.Canceled) || called {
			t.Errorf("expected Submit waiting for a slot to give up with the context, got %v (done called %t)", err, called)
		}
	})
}

// blockingClient holds every call until released, standing in for calls in flight.
type blockingClient struct {
	release chan struct{}
}

func (c blockingClient) BatchWriteItem(ctx context.Context, _ *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	select {
	case <-c.release:
	case <-ctx.Done():
	}
	return &dynamodb.BatchWriteItemOutput{}, nil
}

// clone copies an item the way decoding a response does, so a match cannot rest on the
// two sharing a map.
func clone(item map[string]types.AttributeValue) map[string]types.AttributeValue {
	out := make(map[string]types.AttributeValue, len(item))
	for k, v := range item {
		if s, ok := v.(*types.AttributeValueMemberS); ok {
			v = &types.AttributeValueMemberS{Value: s.Value}
		}
		out[k] = v
	}
	return out
}

// BenchmarkHandedBackMatching measures naming a full call's worth of handed-back items,
// the worst case of the comparison the rejection path pays for.
func BenchmarkHandedBackMatching(b *testing.B) {
	ops := putOps(MaxBatch)
	call := make([]types.WriteRequest, len(ops))
	back := make([]types.WriteRequest, len(ops))
	for i, op := range ops {
		call[i] = types.WriteRequest{PutRequest: &types.PutRequest{Item: op.NewImage}}
		back[len(ops)-1-i] = types.WriteRequest{PutRequest: &types.PutRequest{Item: clone(op.NewImage)}}
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = appendHandedBack(nil, call, 0, back)
	}
}

// TestSubmitHoldsBatchesPastTheLimitUntilOneFinishes verifies Submit lets no more batches
// be in flight than the limit, and lets the next go as soon as one finishes. A slot never
// given back would stop the restore once the limit's worth of batches had been sent; one
// not enforced would hold as many batches in memory as the readers could produce.
func TestSubmitHoldsBatchesPastTheLimitUntilOneFinishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		w := NewDynamoDBWriter(blockingClient{release: release}, "test-table", Callbacks{})
		var finished sync.WaitGroup
		submit := func() {
			finished.Add(1)
			if err := w.Submit(t.Context(), putOps(1), func(Rejection, error) { finished.Done() }); err != nil {
				t.Errorf("Submit: %v", err)
			}
		}
		for range initialConcurrency {
			submit()
		}

		next := make(chan struct{})
		go func() { submit(); close(next) }()
		synctest.Wait()
		select {
		case <-next:
			t.Fatal("a batch past the limit was let through")
		default:
		}

		release <- struct{}{} // One call answers.
		<-next
		close(release)
		finished.Wait()
	})
}

// TestSubmitGivesEverySlotBack verifies a writer used for many more batches than its
// limit keeps accepting them, which it only does if every batch gives its slot back.
func TestSubmitGivesEverySlotBack(t *testing.T) {
	w := NewDynamoDBWriter(&scriptedClient{}, "test-table", Callbacks{}, withPaceClock(newTestClock()))
	for i := range 4 * initialConcurrency {
		if err := writeAndWait(t.Context(), w, putOps(1)); err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
	}
}

// TestSubmitGivesUpWhenTheWaitForCapacityIsCutShort verifies a batch waiting for the
// table's capacity when the restore stops is reported failed rather than written. Told it
// was written, the coordinator would checkpoint past items never sent.
func TestSubmitGivesUpWhenTheWaitForCapacityIsCutShort(t *testing.T) {
	clock := newTestClock()
	clock.refuse = true
	w := NewDynamoDBWriter(&scriptedClient{}, "test-table", Callbacks{}, withPaceClock(clock))
	w.pacer.setRateLocked(clock.Now(), 1)
	w.pacer.tokens = 0

	if err := writeAndWait(context.Background(), w, putOps(3)); err == nil {
		t.Error("a batch whose wait for capacity was cut short was reported written")
	}
}

// TestRefusalPartWayThroughABatchNamesOnlyWhatItRefused verifies a batch sent in more than
// one call, because the table's rate pays for only part of it at once, reports refused
// only the call the table refused, and still sends the rest. Naming too little loses
// items; naming too much writes them twice; stopping at the refusal loses the remainder.
func TestRefusalPartWayThroughABatchNamesOnlyWhatItRefused(t *testing.T) {
	clock := newTestClock()
	client := &scriptedClient{batchErrs: []error{throttle(), nil}}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{}, withPaceClock(clock))
	w.pacer.setRateLocked(clock.Now(), 2)
	clock.advance(time.Second) // Two units banked: two items per call.

	rejection, err := submitAndWait(context.Background(), w, putOps(4))
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if !slices.Equal(rejection.Refused, []int{0, 1}) {
		t.Errorf("refused = %v, want the first call's items 0 and 1", rejection.Refused)
	}
	if n := len(client.batchRequests); n < 2 {
		t.Fatalf("expected the rest of the batch sent after the refusal, got %d calls", n)
	}
	last := client.batchRequests[len(client.batchRequests)-1]["test-table"]
	if len(last) == 0 || itemPK(last[len(last)-1]) != itemPK(types.WriteRequest{PutRequest: &types.PutRequest{Item: putOps(4)[3].NewImage}}) {
		t.Errorf("the last call did not carry the batch's last item: %v", last)
	}
}

// TestHandedBackItemsAreCountedAsThrottles verifies items handed back count as the table
// throttling, since that is what they are, and an operator deciding whether to raise the
// table's capacity reads the throttle count to decide.
func TestHandedBackItemsAreCountedAsThrottles(t *testing.T) {
	ops := putOps(2)
	client := &scriptedClient{batchOutputs: []*dynamodb.BatchWriteItemOutput{
		{UnprocessedItems: map[string][]types.WriteRequest{"test-table": {{PutRequest: &types.PutRequest{Item: clone(ops[0].NewImage)}}}}},
	}}
	counts := &callbackCounts{}
	w := NewDynamoDBWriter(client, "test-table", counts.callbacks(), withPaceClock(newTestClock()))

	if _, err := submitAndWait(t.Context(), w, ops); err != nil {
		t.Fatal(err)
	}
	if counts.throttles != 1 {
		t.Errorf("throttles = %d, want 1", counts.throttles)
	}
}

// TestHandedBackMatchingNamesEachItemOnce verifies the first item of a call can be named,
// that two handed back items identical in content are named as two, and that a delete
// is never taken for a put. A wrong name is an item never sent again.
func TestHandedBackMatchingNamesEachItemOnce(t *testing.T) {
	put := types.WriteRequest{PutRequest: &types.PutRequest{Item: putOps(1)[0].NewImage}}
	del := types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: putOps(1)[0].NewImage}}
	call := []types.WriteRequest{put, put, del}

	got := slices.Sorted(slices.Values(appendHandedBack(nil, call, 0, []types.WriteRequest{del, put, put})))
	if !slices.Equal(got, []int{0, 1, 2}) {
		t.Errorf("named %v, want 0, 1 and 2", got)
	}
	if got := appendHandedBack(nil, call[:2], 0, []types.WriteRequest{put}); !slices.Equal(got, []int{0}) {
		t.Errorf("named %v for the first item, want 0", got)
	}
}

// TestHandedBackMatchingTellsApartItemsOfDifferentShape verifies items differ when one
// holds an attribute the other does not, or a list or binary set of a different length,
// even where everything they share is equal.
func TestHandedBackMatchingTellsApartItemsOfDifferentShape(t *testing.T) {
	pk := &types.AttributeValueMemberS{Value: "x"}
	if sameItem(map[string]types.AttributeValue{"pk": pk}, map[string]types.AttributeValue{"pk": pk, "n": pk}) {
		t.Error("an item taken for one with an extra attribute")
	}
	if sameValue(&types.AttributeValueMemberBS{Value: [][]byte{{1}}}, &types.AttributeValueMemberBS{Value: [][]byte{{1}, {2}}}) {
		t.Error("binary sets of different lengths taken as equal")
	}
	if sameValue(&types.AttributeValueMemberL{Value: []types.AttributeValue{pk}}, &types.AttributeValueMemberL{Value: []types.AttributeValue{pk, pk}}) {
		t.Error("lists of different lengths taken as equal")
	}
}

// itemPK reads a put request's partition key.
func itemPK(r types.WriteRequest) string {
	return r.PutRequest.Item["PK"].(*types.AttributeValueMemberS).Value
}

// TestWithMaxInFlightRefusesCeilingsOutsideItsRange verifies a ceiling below one call, or
// above HighestMaxInFlight, is refused where the writer is built, and the ends of the
// range are accepted. A writer that may have no call in flight would never write; one
// allowed any number could flood the table and the network it shares with others.
func TestWithMaxInFlightRefusesCeilingsOutsideItsRange(t *testing.T) {
	for _, n := range []int{0, HighestMaxInFlight + 1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("a ceiling of %d calls in flight was accepted", n)
				}
			}()
			WithMaxInFlight(n)
		}()
	}
	for _, n := range []int{1, HighestMaxInFlight} {
		NewDynamoDBWriter(&scriptedClient{}, "test-table", Callbacks{}, WithMaxInFlight(n))
	}
}

// TestSubmitHoldsToTheCeilingItWasGiven verifies a writer given a ceiling holds a batch
// past it until a call finishes, from the first batch on.
func TestSubmitHoldsToTheCeilingItWasGiven(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		w := NewDynamoDBWriter(blockingClient{release: release}, "test-table", Callbacks{}, WithMaxInFlight(2))
		defer close(release)
		for range 2 {
			if err := w.Submit(t.Context(), putOps(1), func(Rejection, error) {}); err != nil {
				t.Fatalf("Submit within the ceiling failed: %v", err)
			}
		}
		third := make(chan struct{})
		go func() {
			_ = w.Submit(t.Context(), putOps(1), func(Rejection, error) {})
			close(third)
		}()
		synctest.Wait()
		select {
		case <-third:
			t.Fatal("a third batch went out past a ceiling of two")
		default:
		}
		release <- struct{}{}
		<-third
	})
}
