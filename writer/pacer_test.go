package writer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gurre/ddb-pitr/itemimage"
)

// testClock stands in for the pacer's clock: time only moves when the pacer waits, so
// a test drives the rate the table is being held to without spending the time it
// describes. refuse makes every wait report that it was cut short, which is what a
// restore stopping mid-wait looks like from inside the pacer.
type testClock struct {
	now    time.Time
	slept  []time.Duration
	mu     sync.Mutex
	refuse bool
}

// newTestClock starts a clock at a fixed instant. The instant is arbitrary and derived
// from nothing outside the test, so the same test passes on any future run.
func newTestClock() *testClock {
	return &testClock{now: time.Unix(0, 0).UTC()}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Sleep advances the clock by the wait rather than performing it.
func (c *testClock) Sleep(ctx context.Context, d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.slept = append(c.slept, d)
	if c.refuse {
		return false
	}
	if d > 0 {
		c.now = c.now.Add(d)
	}
	return ctx.Err() == nil
}

// advance moves the clock on without a wait, standing in for time passing while the
// restore is doing something else.
func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// waits reports every wait the pacer asked for, in order.
func (c *testClock) waits() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.slept...)
}

// newTestPacer returns a pacer on a clock the test drives, limiting nothing until the
// table first refuses a write.
func newTestPacer(c *testClock) *pacer {
	return &pacer{clock: c}
}

// TestWriteCostChargesAKilobyteAUnit verifies the capacity an operation is expected to
// cost follows its size, and that a delete costs one. The estimate is what decides how
// many items go in a request, so charging every item the same would make requests too
// large for a table holding big items and needlessly small for one holding small ones.
func TestWriteCostChargesAKilobyteAUnit(t *testing.T) {
	tests := []struct {
		name string
		op   itemimage.Operation
		want float64
	}{
		{"a small item costs one unit", itemimage.Operation{Type: itemimage.OpPut, Bytes: 40}, 1},
		{"an item of no measured size still costs one", itemimage.Operation{Type: itemimage.OpPut}, 1},
		{"a three kilobyte item costs three", itemimage.Operation{Type: itemimage.OpPut, Bytes: 2049}, 3},
		{"a delete is charged by its key", itemimage.Operation{Type: itemimage.OpDelete, Bytes: 4096}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := writeCost(tt.op); got != tt.want {
				t.Errorf("writeCost = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestPacerCutsTheRateOnceWithinAWindow verifies repeated refusals in quick succession
// step the rate down once rather than once each.
//
// A batch is deliberately spread across partitions, so one hot partition hands items
// back to several writers in the same instant. Treating those as separate signals would
// collapse the rate to the floor over a condition that warranted one step down, and the
// restore would then crawl until it had climbed all the way back.
func TestPacerCutsTheRateOnceWithinAWindow(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)

	// A first refusal with nothing observed yet puts the rate at the floor, so start
	// from a rate there is somewhere to fall from.
	p.setRateLocked(clock.Now(), 64)
	p.achieved = 64

	p.throttled()
	p.throttled()
	p.throttled()

	rate, _ := p.current()
	if rate != 32 {
		t.Errorf("expected three refusals in one window to halve the rate once to 32, got %v", rate)
	}
}

// TestPacerCutsAgainInTheNextWindow verifies the rate keeps stepping down while the
// table keeps refusing, so a restore aimed far above a table's capacity converges on it
// instead of stalling at the first guess.
func TestPacerCutsAgainInTheNextWindow(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 64)
	p.achieved = 64

	p.throttled()
	clock.advance(paceWindow)
	p.throttled()

	rate, _ := p.current()
	if rate != 16 {
		t.Errorf("expected a refusal in each of two windows to reach 16, got %v", rate)
	}
}

// TestPacerRaisesTheRateAfterASaturatedWindow verifies a window that used its whole
// allowance without being refused raises the rate. Autoscaling and partition splits add
// capacity during a restore, and a rate that only ever fell would hold the restore at
// the worst moment the table ever had.
func TestPacerRaisesTheRateAfterASaturatedWindow(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)

	p.throttled() // The table refuses, putting the rate at the floor of one unit.
	clock.advance(paceWindow)
	p.take(1, 1) // Opens the first clean window.
	p.settle(1, 1)
	clock.advance(paceWindow)
	p.take(1, 1) // Closes it, having used the whole allowance.

	rate, _ := p.current()
	if rate != 2 {
		t.Errorf("expected a saturated clean window to raise the rate to 2, got %v", rate)
	}
}

// TestPacerHoldsTheRateWhenTheAllowanceGoesUnused verifies a window that did not use
// what it was given does not raise the rate. A restore held back by how fast it reads
// the export would otherwise grow the rate without limit, and the next refusal would
// have to climb down from a number the table never accepted.
func TestPacerHoldsTheRateWhenTheAllowanceGoesUnused(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 100)

	clock.advance(paceWindow)
	p.take(1, 1) // Opens a window.
	p.settle(1, 1)
	clock.advance(paceWindow)
	p.take(1, 1) // Closes it, having used one unit of a hundred.

	rate, _ := p.current()
	if rate != 100 {
		t.Errorf("expected an unused allowance to leave the rate at 100, got %v", rate)
	}
}

// TestPacerNeverStopsAltogether verifies the rate has a floor. A table with no capacity
// at all still gets a request a second, which is what notices capacity coming back; a
// rate that could reach zero would leave the restore waiting forever.
func TestPacerNeverStopsAltogether(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)

	for i := 0; i < 10; i++ {
		p.throttled()
		clock.advance(paceWindow)
	}

	rate, paced := p.current()
	if !paced || rate != minPaceRate {
		t.Errorf("expected the rate to settle at the floor of %v, got %v (paced %t)",
			minPaceRate, rate, paced)
	}
}

// TestPacerChargesWhatTheTableActuallyConsumed verifies a request that cost more than
// its items' export lines suggested is paid for out of the next request's allowance.
// Without that the restore would keep spending capacity it was never granted and sit in
// a permanent refusal.
func TestPacerChargesWhatTheTableActuallyConsumed(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.throttled() // Rate at the floor: one unit a second.
	clock.advance(paceWindow)

	if n, _ := p.take(1, 1); n != 1 {
		t.Fatalf("expected the accrued unit to be spendable, got %d", n)
	}
	p.settle(1, 5) // The table charged five units for what was estimated at one.

	_, wait := p.take(1, 1)
	if wait != 5*time.Second {
		t.Errorf("expected the next request to wait 5s for the overspend, got %s", wait)
	}
}

// TestPacerAdmitsEverythingUntilTheTableRefuses verifies nothing is limited before the
// first refusal. An on-demand table or a generously provisioned one should restore
// exactly as fast as it would with no pacing at all.
func TestPacerAdmitsEverythingUntilTheTableRefuses(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)

	n, ok := p.admit(context.Background(), 1, 25)
	if !ok || n != 25 {
		t.Errorf("expected all 25 admitted without limit, got %d (ok %t)", n, ok)
	}
	if waits := clock.waits(); len(waits) != 0 {
		t.Errorf("expected no waiting before the table refuses anything, got %v", waits)
	}
	if _, paced := p.current(); paced {
		t.Error("expected no rate to be reported before the table refuses anything")
	}
}

// BenchmarkPacerAdmit measures the admission every request pays for, under the
// contention of a full writer pool sharing one pacer.
func BenchmarkPacerAdmit(b *testing.B) {
	p := newPacer(nil)
	p.setRateLocked(time.Now(), 1e9) // High enough that nothing ever waits.
	ctx := context.Background()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.admit(ctx, 1, 25)
		}
	})
}

// capacityClient stands for a table with little provisioned capacity: it accepts a
// fixed number of items per call and hands the rest straight back unprocessed, which is
// exactly how DynamoDB refuses part of a batch.
type capacityClient struct {
	sizes    []int   // Items in each call, in order
	charge   float64 // Capacity to report per accepted item; 0 reports none
	limit    int     // Items one call may write
	reported int     // Calls that asked what they consumed
	mu       sync.Mutex
}

func (c *capacityClient) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if params.ReturnConsumedCapacity == types.ReturnConsumedCapacityTotal {
		c.reported++
	}
	out := &dynamodb.BatchWriteItemOutput{}
	for table, requests := range params.RequestItems {
		c.sizes = append(c.sizes, len(requests))
		accepted := len(requests)
		if accepted > c.limit {
			accepted = c.limit
			out.UnprocessedItems = map[string][]types.WriteRequest{
				table: append([]types.WriteRequest(nil), requests[c.limit:]...),
			}
		}
		// A table that reports nothing is reported as an entry with no units, which is
		// what the field looks like when the caller did not ask for it.
		units := c.charge * float64(accepted)
		entry := types.ConsumedCapacity{TableName: &table}
		if units > 0 {
			entry.CapacityUnits = &units
		}
		out.ConsumedCapacity = append(out.ConsumedCapacity, entry)
	}
	return out, nil
}

// requestSizes reports how many items went in each call, in order.
func (c *capacityClient) requestSizes() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.sizes...)
}

// TestWriteBatchShrinksRequestsToWhatTheTableAccepts verifies a request that the table
// refuses most of is followed by requests the table can take whole.
//
// This is the failure a small provisioned table produces: twenty-five items go out,
// twenty-three come back, and a writer that keeps resending twenty-three collects the
// same refusal until the restore is nothing but refusals. Sizing the next request to
// the rate the table was observed to accept is what ends that.
func TestWriteBatchShrinksRequestsToWhatTheTableAccepts(t *testing.T) {
	const accepts = 2
	client := &capacityClient{limit: accepts}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{},
		WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	if err := w.WriteBatch(context.Background(), putOps(10)); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	sizes := client.requestSizes()
	if len(sizes) < 2 {
		t.Fatalf("expected the refused items to be resent, got %v", sizes)
	}
	if sizes[0] != 10 {
		t.Errorf("expected the first request to carry all 10 items, got %d", sizes[0])
	}
	for i, size := range sizes[1:] {
		if size > accepts {
			t.Errorf("request %d carried %d items, more than the %d the table accepts",
				i+1, size, accepts)
		}
	}
}

// TestWriteBatchSendsFullRequestsUntilTheTableRefuses verifies a table that accepts
// everything is written to at full batch size with no waiting. Holding back a table
// that has capacity would make every restore slower to guard against the small ones.
func TestWriteBatchSendsFullRequestsUntilTheTableRefuses(t *testing.T) {
	client := &capacityClient{limit: 25}
	clock := newTestClock()
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{},
		WithBackoff(&instantBackoff{}), withPaceClock(clock))

	if err := w.WriteBatch(context.Background(), putOps(50)); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	sizes := client.requestSizes()
	if len(sizes) != 2 || sizes[0] != 25 || sizes[1] != 25 {
		t.Errorf("expected two full requests of 25, got %v", sizes)
	}
	if waits := clock.waits(); len(waits) != 0 {
		t.Errorf("expected no waiting against a table that accepts everything, got %v", waits)
	}
}

// TestWriteBatchReportsThePaceItSettledOn verifies the rate the table is being held to
// reaches the caller. An operator watching a restore crawl needs to see the limit that
// is holding it, or the only visible symptom is a number that will not move.
func TestWriteBatchReportsThePaceItSettledOn(t *testing.T) {
	client := &capacityClient{limit: 2}
	var paces []float64
	var mu sync.Mutex
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{
		OnPace: func(rate float64) {
			mu.Lock()
			defer mu.Unlock()
			paces = append(paces, rate)
		},
	}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	if err := w.WriteBatch(context.Background(), putOps(10)); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(paces) == 0 {
		t.Fatal("expected the rate the table imposed to be reported")
	}
	for _, rate := range paces {
		if rate <= 0 {
			t.Errorf("reported a rate of %v, which reads as a stalled restore", rate)
		}
	}
}

// TestPacerClimbsBackWhenTheTableReportsNoCapacity verifies the rate still recovers
// against an endpoint that does not report what a write consumed. Treating a silent
// response as no consumption would make every window look idle, and a rate that had
// been cut once would stay cut for the rest of the restore.
func TestPacerClimbsBackWhenTheTableReportsNoCapacity(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)

	p.throttled() // Rate at the floor: one unit a second.
	clock.advance(paceWindow)
	p.take(1, 1) // Opens the first clean window.
	p.settle(1, 0)
	clock.advance(paceWindow)
	p.take(1, 1) // Closes it.

	rate, _ := p.current()
	if rate <= minPaceRate {
		t.Errorf("expected the rate to climb past the floor, got %v", rate)
	}
}

// TestPacerGrantsRatherThanSpinningOnAnUnmeasurableWait verifies a shortfall too small
// for the clock to express is granted rather than waited on.
//
// The wait until the bucket can pay is the shortfall divided by the rate, and at a high
// rate that lands below a nanosecond and rounds to nothing. Sleeping for nothing and
// asking again is a busy loop that never refills, which is a restore burning a core and
// writing nothing.
func TestPacerGrantsRatherThanSpinningOnAnUnmeasurableWait(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 3)
	p.tokens = 1 - 1e-12

	n, wait := p.take(1, 1)
	if n != 1 {
		t.Errorf("expected the request granted, got %d after a wait of %s", n, wait)
	}
}

// TestPacerCutsToHalfOfWhatTheTableWasTaking verifies the first refusal steps down from
// the rate the table had been accepting, not from nothing.
//
// A large table that throttles once during a burst would otherwise be held to the floor
// of one unit a second and have to climb all the way back, turning a moment's pressure
// into minutes of a restore running at a thousandth of the table's capacity.
func TestPacerCutsToHalfOfWhatTheTableWasTaking(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)

	p.take(1, 25)                   // The first write, where measurement starts.
	p.settle(1000, 1000)            // A thousand units go through...
	clock.advance(10 * time.Second) // ...over ten seconds, so a hundred a second.

	p.throttled()

	rate, _ := p.current()
	if rate != 50 {
		t.Errorf("expected the rate cut to half of the 100/s observed, got %v", rate)
	}
}

// pickyClient accepts only the last item of each call and hands every other one back,
// which is what a table does when the items of one write fall on partitions with
// different amounts of capacity left. It records the key of each item it accepted.
type pickyClient struct {
	accepted []string
	mu       sync.Mutex
}

func (c *pickyClient) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := &dynamodb.BatchWriteItemOutput{}
	for table, requests := range params.RequestItems {
		last := len(requests) - 1
		c.accepted = append(c.accepted, itemKey(requests[last]))
		if last > 0 {
			out.UnprocessedItems = map[string][]types.WriteRequest{
				table: append([]types.WriteRequest(nil), requests[:last]...),
			}
		}
	}
	return out, nil
}

// itemKey reads the partition key out of a write request, which is what identifies the
// item the request carries.
func itemKey(r types.WriteRequest) string {
	return r.PutRequest.Item["PK"].(*types.AttributeValueMemberS).Value
}

// TestWriteBatchResendsTheItemsTheTableRejected verifies the items resent after a
// partial acceptance are the ones the table actually rejected.
//
// DynamoDB hands back a subset of the write in no particular order, and it overlaps
// whatever the writer held back for want of capacity. Resending the wrong subset writes
// some items twice and never writes the others, and nothing reports it: the restore
// finishes clean with items missing from the table.
func TestWriteBatchResendsTheItemsTheTableRejected(t *testing.T) {
	const items = 10
	client := &pickyClient{}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{},
		WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	if err := w.WriteBatch(context.Background(), putOps(items)); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	seen := make(map[string]int, items)
	for _, key := range client.accepted {
		seen[key]++
	}
	for _, op := range putOps(items) {
		key := op.NewImage["PK"].(*types.AttributeValueMemberS).Value
		if seen[key] != 1 {
			t.Errorf("item %s was written %d times, want once", key, seen[key])
		}
	}
}

// TestWriteBatchFillsARequestToTheCapacityAvailable verifies a request carries as many
// items as the capacity on hand pays for. The per-item cost is what converts a rate
// into a request size, so an estimate that drifts high sends needlessly small requests
// and one that drifts low sends ones the table refuses.
func TestWriteBatchFillsARequestToTheCapacityAvailable(t *testing.T) {
	clock := newTestClock()
	client := &capacityClient{limit: 25}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{},
		WithBackoff(&instantBackoff{}), withPaceClock(clock))

	// The table is being held to ten units a second, with a second's worth banked, and
	// the ten items are a unit each.
	w.pacer.setRateLocked(clock.Now(), 10)
	clock.advance(time.Second)

	if err := w.WriteBatch(context.Background(), putOps(10)); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if sizes := client.requestSizes(); len(sizes) != 1 || sizes[0] != 10 {
		t.Errorf("expected one request carrying all 10 items, got %v", sizes)
	}
}

// TestWriteBatchAsksWhatEachWriteConsumed verifies every write asks the table what it
// cost. Without it the restore paces on its own estimate of item sizes for the whole
// run, and an export whose lines are a poor guide to the items would be held at a rate
// that has nothing to do with the table.
func TestWriteBatchAsksWhatEachWriteConsumed(t *testing.T) {
	client := &capacityClient{limit: 25}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{},
		WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	if err := w.WriteBatch(context.Background(), putOps(30)); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if client.reported != len(client.sizes) {
		t.Errorf("%d of %d writes asked what they consumed", client.reported, len(client.sizes))
	}
}

// TestWriteBatchClimbsBackWhileTheTableKeepsUp verifies a restore held to a rate raises
// it again while the table accepts everything at that rate. A rate that only ever fell
// would hold a restore at the worst moment the table ever had, and capacity added
// during a long restore, by autoscaling or a partition split, would go unused.
func TestWriteBatchClimbsBackWhileTheTableKeepsUp(t *testing.T) {
	clock := newTestClock()
	client := &capacityClient{limit: 25}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{},
		WithBackoff(&instantBackoff{}), withPaceClock(clock))
	w.pacer.setRateLocked(clock.Now(), 10)

	// Four seconds of writing the whole allowance, none of it refused.
	for i := 0; i < 4; i++ {
		clock.advance(time.Second)
		if err := w.WriteBatch(context.Background(), putOps(10)); err != nil {
			t.Fatalf("WriteBatch failed: %v", err)
		}
	}

	rate, _ := w.pacer.current()
	if rate <= 10 {
		t.Errorf("expected the rate to rise above 10, got %v", rate)
	}
}

// TestWallClockSleepWaitsAndHonoursCancellation verifies the clock a restore actually
// runs on waits for the delay it is given and gives up when the context ends. Every
// other test replaces it, so without this the one implementation that ships is the one
// nothing exercises.
func TestWallClockSleepWaitsAndHonoursCancellation(t *testing.T) {
	var c wallClock

	start := time.Now()
	if !c.Sleep(context.Background(), 2*time.Millisecond) {
		t.Error("expected a completed wait to report success")
	}
	if elapsed := time.Since(start); elapsed < time.Millisecond {
		t.Errorf("expected the wait to take at least a millisecond, took %s", elapsed)
	}

	// A delay of nothing is not a wait at all, and must not report the restore stopping.
	if !c.Sleep(context.Background(), 0) {
		t.Error("expected no wait to report success")
	}

	stopped, cancel := context.WithCancel(context.Background())
	cancel()
	if c.Sleep(stopped, time.Hour) {
		t.Error("expected a wait cut short by the context to report failure")
	}
	if c.Sleep(stopped, 0) {
		t.Error("expected no wait under a stopped context to report failure")
	}
}

// TestWriteBatchChargesTheCapacityTheTableReported verifies the capacity a response
// reports is what the restore is charged, not what it guessed.
//
// The export line an item was decoded from is only a guide to what DynamoDB will
// measure. A restore that kept spending against its own guess would drift away from the
// rate the table actually granted and sit in a permanent refusal.
func TestWriteBatchChargesTheCapacityTheTableReported(t *testing.T) {
	clock := newTestClock()
	// Each item is estimated at one unit but costs four.
	client := &capacityClient{limit: 25, charge: 4}
	w := NewDynamoDBWriter(client, "test-table", 25, Callbacks{},
		WithBackoff(&instantBackoff{}), withPaceClock(clock))
	w.pacer.setRateLocked(clock.Now(), 10)
	clock.advance(time.Second)

	if err := w.WriteBatch(context.Background(), putOps(10)); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Ten units were banked and forty were charged, so the next request waits for the
	// thirty the restore is now behind.
	_, wait := w.pacer.take(1, 1)
	if wait < 3*time.Second {
		t.Errorf("expected the overcharge to delay the next request, waited %s", wait)
	}
}

// TestPacerDropsBankedCapacityWhenTheRateFalls verifies capacity banked at a high rate
// is not spent at a low one. A refusal means the table stopped taking what it was
// taking, so a bucket left full would send one more request of the old size straight
// back into it.
func TestPacerDropsBankedCapacityWhenTheRateFalls(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 100)
	p.achieved = 100
	clock.advance(time.Second) // A hundred units accrue.

	// One small request, so most of the hundred stays banked.
	if n, _ := p.take(1, 10); n != 10 {
		t.Fatalf("expected the request granted from the banked capacity, got %d", n)
	}

	p.throttled() // Which halves the rate to fifty.

	n, _ := p.take(1, 100)
	if n > 50 {
		t.Errorf("expected the next request held to the new rate of 50, got %d", n)
	}
}

// TestPacerSizesTheRequestByWhatEachItemCosts verifies the capacity on hand is divided
// by what an item costs, not applied per item.
//
// A table's limit is in capacity units, and items are not a unit each: a four-kilobyte
// item is four. A restore that counted items would send four times the work it was
// allowed whenever the export held large items, which is the case where getting the
// rate right matters most.
func TestPacerSizesTheRequestByWhatEachItemCosts(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 30)
	clock.advance(time.Second) // Thirty units accrue.

	// Items of three units each, so thirty units pay for ten of them.
	n, _ := p.take(3, 20)
	if n != 10 {
		t.Errorf("expected thirty units to pay for ten three-unit items, got %d", n)
	}
}

// TestPacerMeasuresTheRateOverTheWindowsLength verifies what the table was taking is
// measured as capacity per second, not as capacity. Every window happening to be a
// second long would hide the difference, and a longer one would then be read as a far
// higher rate than the table ever granted.
func TestPacerMeasuresTheRateOverTheWindowsLength(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 10)
	clock.advance(time.Second)

	p.take(1, 1)    // Opens a window.
	p.settle(1, 20) // Twenty units go through it...
	clock.advance(4 * time.Second)
	p.take(1, 1) // ...over four seconds, so five a second.

	p.throttled()

	rate, _ := p.current()
	if rate != 2.5 {
		t.Errorf("expected the rate cut to half of the 5/s observed, got %v", rate)
	}
}
