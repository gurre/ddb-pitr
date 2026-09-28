package writer

import (
	"context"
	"math"
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

// TestPacerCutsTheRateOnceWithinAWindow verifies repeated refusals within one window
// step the rate down once rather than once each.
//
// A batch is deliberately spread across partitions, so one hot partition hands items
// back to several writers in the same instant. Treating those as separate signals would
// collapse the rate to the floor over a condition that warranted one step down, and the
// restore would then crawl until it had climbed all the way back.
func TestPacerCutsTheRateOnceWithinAWindow(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)

	p.setRateLocked(clock.Now(), 64)
	p.take(1, 1) // Opens a window.
	p.refused(10)
	p.refused(10)
	p.refused(10)
	clock.advance(paceWindow)
	p.take(1, 1) // Closes it.

	rate, _ := p.current()
	if !near(rate, 44.8) {
		t.Errorf("expected three refusals in one window to cut the rate once to 44.8, got %v", rate)
	}
}

// TestPacerCutsAgainInTheNextWindow verifies the rate keeps stepping down while the
// table keeps refusing, so a restore aimed far above a table's capacity converges on it
// instead of stalling at the first guess.
func TestPacerCutsAgainInTheNextWindow(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 64)
	refuseWindow(p, clock)
	refuseWindow(p, clock)

	rate, _ := p.current()
	if !near(rate, 31.36) {
		t.Errorf("expected a refusal in each of two windows to reach 31.36, got %v", rate)
	}
}

// TestPacerRaisesTheRateAfterASaturatedWindow verifies a window that used its whole
// allowance without being refused raises the rate. Autoscaling and partition splits add
// capacity during a restore, and a rate that only ever fell would hold the restore at
// the worst moment the table ever had.
func TestPacerRaisesTheRateAfterASaturatedWindow(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)

	p.setRateLocked(clock.Now(), 1) // Held to one unit a second.
	clock.advance(paceWindow)
	p.take(1, 1) // Opens the first clean window.
	p.settle(1, 1, 0)
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
	p.settle(1, 1, 0)
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

	for range 20 {
		refuseWindow(p, clock)
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
	p.setRateLocked(clock.Now(), 1) // One unit a second.
	clock.advance(paceWindow)

	if n, _ := p.take(1, 1); n != 1 {
		t.Fatalf("expected the accrued unit to be spendable, got %d", n)
	}
	p.settle(1, 5, 0) // The table charged five units for what was estimated at one.

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

// TestSubmitSendsFullRequestsUntilTheTableRefuses verifies a table that accepts
// everything is written to at full batch size with no waiting. Holding back a table
// that has capacity would make every restore slower to guard against the small ones.
func TestSubmitSendsFullRequestsUntilTheTableRefuses(t *testing.T) {
	client := &capacityClient{limit: 25}
	clock := newTestClock()
	w := NewDynamoDBWriter(client, "test-table", Callbacks{},
		WithBackoff(&instantBackoff{}), withPaceClock(clock))

	if err := writeAndWait(context.Background(), w, putOps(25)); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	sizes := client.requestSizes()
	if len(sizes) != 1 || sizes[0] != 25 {
		t.Errorf("expected one full request of 25, got %v", sizes)
	}
	if waits := clock.waits(); len(waits) != 0 {
		t.Errorf("expected no waiting against a table that accepts everything, got %v", waits)
	}
}

// TestSubmitReportsThePaceItSettledOn verifies the rate the table is being held to
// reaches the caller. An operator watching a restore crawl needs to see the limit that
// is holding it, or the only visible symptom is a number that will not move.
func TestSubmitReportsThePaceItSettledOn(t *testing.T) {
	client := &scriptedClient{batchErrs: []error{throttle(), nil}}
	clock := newTestClock()
	var paces []float64
	var mu sync.Mutex
	w := NewDynamoDBWriter(client, "test-table", Callbacks{
		OnPace: func(rate float64) {
			mu.Lock()
			defer mu.Unlock()
			paces = append(paces, rate)
		},
	}, WithBackoff(&instantBackoff{}), withPaceClock(clock))

	// The first batch is refused; the second is sent after the window it fell in closed.
	for range 2 {
		if err := writeAndWait(context.Background(), w, putOps(10)); err != nil {
			t.Fatalf("Submit failed: %v", err)
		}
		clock.advance(paceWindow)
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

	p.setRateLocked(clock.Now(), 1) // One unit a second.
	clock.advance(paceWindow)
	p.take(1, 1) // Opens the first clean window.
	p.settle(1, 0, 0)
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

// TestPacerCutsFromWhatTheTableWasTaking verifies the first cut steps down from the rate
// the restore was sending at, not from nothing.
//
// A large table that throttles once during a burst would otherwise be held to the floor
// of one unit a second and have to climb all the way back, turning a moment's pressure
// into minutes of a restore running at a thousandth of the table's capacity.
func TestPacerCutsFromWhatTheTableWasTaking(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)

	p.take(1, 25)                   // Opens a window.
	p.settle(1000, 1000, 0)         // A thousand units go through...
	p.refused(1000)                 // ...and as many are refused...
	clock.advance(10 * time.Second) // ...over ten seconds: two hundred a second sent.
	p.take(1, 25)                   // Closes it.

	rate, _ := p.current()
	if !near(rate, 140) {
		t.Errorf("expected the rate cut to seven tenths of the 200/s sent, got %v", rate)
	}
}

// TestSubmitFillsARequestToTheCapacityAvailable verifies a request carries as many
// items as the capacity on hand pays for. The per-item cost is what converts a rate
// into a request size, so an estimate that drifts high sends needlessly small requests
// and one that drifts low sends ones the table refuses.
func TestSubmitFillsARequestToTheCapacityAvailable(t *testing.T) {
	clock := newTestClock()
	client := &capacityClient{limit: 25}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{},
		WithBackoff(&instantBackoff{}), withPaceClock(clock))

	// The table is being held to ten units a second, with a second's worth banked, and
	// the ten items are a unit each.
	w.pacer.setRateLocked(clock.Now(), 10)
	clock.advance(time.Second)

	if err := writeAndWait(context.Background(), w, putOps(10)); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	if sizes := client.requestSizes(); len(sizes) != 1 || sizes[0] != 10 {
		t.Errorf("expected one request carrying all 10 items, got %v", sizes)
	}
}

// TestSubmitAsksWhatEachWriteConsumed verifies every write asks the table what it
// cost. Without it the restore paces on its own estimate of item sizes for the whole
// run, and an export whose lines are a poor guide to the items would be held at a rate
// that has nothing to do with the table.
func TestSubmitAsksWhatEachWriteConsumed(t *testing.T) {
	client := &capacityClient{limit: 25}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{},
		WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	if err := writeAndWait(context.Background(), w, putOps(25)); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if client.reported != len(client.sizes) {
		t.Errorf("%d of %d writes asked what they consumed", client.reported, len(client.sizes))
	}
}

// TestSubmitClimbsBackWhileTheTableKeepsUp verifies a restore held to a rate raises
// it again while the table accepts everything at that rate. A rate that only ever fell
// would hold a restore at the worst moment the table ever had, and capacity added
// during a long restore, by autoscaling or a partition split, would go unused.
func TestSubmitClimbsBackWhileTheTableKeepsUp(t *testing.T) {
	clock := newTestClock()
	client := &capacityClient{limit: 25}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{},
		WithBackoff(&instantBackoff{}), withPaceClock(clock))
	w.pacer.setRateLocked(clock.Now(), 10)

	// Four seconds of writing the whole allowance, none of it refused.
	for i := 0; i < 4; i++ {
		clock.advance(time.Second)
		if err := writeAndWait(context.Background(), w, putOps(10)); err != nil {
			t.Fatalf("Submit failed: %v", err)
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

// TestSubmitChargesTheCapacityTheTableReported verifies the capacity a response
// reports is what the restore is charged, not what it guessed.
//
// The export line an item was decoded from is only a guide to what DynamoDB will
// measure. A restore that kept spending against its own guess would drift away from the
// rate the table actually granted and sit in a permanent refusal.
func TestSubmitChargesTheCapacityTheTableReported(t *testing.T) {
	clock := newTestClock()
	// Each item is estimated at one unit but costs four.
	client := &capacityClient{limit: 25, charge: 4}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{},
		WithBackoff(&instantBackoff{}), withPaceClock(clock))
	w.pacer.setRateLocked(clock.Now(), 10)
	clock.advance(time.Second)

	if err := writeAndWait(context.Background(), w, putOps(10)); err != nil {
		t.Fatalf("Submit failed: %v", err)
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
	p.take(1, 1)               // Opens a window.
	clock.advance(time.Second) // A hundred units accrue.

	// One small request, so most of the hundred stays banked.
	if n, _ := p.take(1, 10); n != 10 {
		t.Fatalf("expected the request granted from the banked capacity, got %d", n)
	}

	p.refused(10) // Which cuts the rate to seventy when the window closes.
	clock.advance(paceWindow)

	n, _ := p.take(1, 100)
	if n > 70 {
		t.Errorf("expected the next request held to the new rate of 70, got %d", n)
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
	p.take(1, 1)         // Opens a window.
	p.settle(40, 20, 20) // Half of forty units go through it...
	clock.advance(4 * time.Second)
	p.take(1, 1) // ...over four seconds: ten a second sent, five taken.

	rate, _ := p.current()
	if !near(rate, 7) {
		t.Errorf("expected the rate cut to seven tenths of the 10/s sent, got %v", rate)
	}
}

// TestPacerLeavesANarrowHandBackAlone verifies a window in which the table handed back
// a hot partition's share of the writes leaves the rate alone. Batches mix items from
// every file being read, so one hot partition hands back a few items out of many; cutting
// the whole table's rate over it would slow every cold partition to the hot one's pace.
func TestPacerLeavesANarrowHandBackAlone(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)

	p.take(1, 25) // Opens a window.
	p.settle(1000, 985, 15)
	clock.advance(paceWindow)
	p.take(1, 25) // Closes it.

	if rate, paced := p.current(); paced {
		t.Errorf("expected 1.5%% handed back to leave writes unlimited, got a rate of %v", rate)
	}
}

// TestPacerHoldsToWhatTheTableAcceptedWhenPartIsHandedBack verifies a window that had a
// tenth of its writes handed back sets the rate to what the table accepted, rather than
// halving it. The table took nine tenths of what was sent, and holding to that loses
// nothing it would have taken.
func TestPacerHoldsToWhatTheTableAcceptedWhenPartIsHandedBack(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)

	p.take(1, 25)
	p.settle(100, 90, 10)
	clock.advance(paceWindow)
	p.take(1, 25)

	if rate, _ := p.current(); !near(rate, 90) {
		t.Errorf("expected the rate held to the 90/s the table accepted, got %v", rate)
	}
}

// TestPacerCutsAHeavyHandBackByNoMoreThanARefusal verifies a window in which most writes
// were handed back cuts the rate by no more than a refusal would. A burst that meets a
// table mid-split can have most of one window handed back, and dropping the rate to what
// that window accepted would throw away capacity the next window has again.
func TestPacerCutsAHeavyHandBackByNoMoreThanARefusal(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)

	p.take(1, 25)
	p.settle(100, 20, 80)
	clock.advance(paceWindow)
	p.take(1, 25)

	if rate, _ := p.current(); !near(rate, 70) {
		t.Errorf("expected the rate cut to seven tenths of the 100/s sent, got %v", rate)
	}
}

// FuzzPacerPartialCutIsBounded checks, for any share handed back, that a window's cut
// keeps at least paceBackoff of what was being sent and never raises the rate above it,
// and that a share within partialFloor cuts nothing at all.
func FuzzPacerPartialCutIsBounded(f *testing.F) {
	f.Add(1000.0, 0.01)
	f.Add(1000.0, 0.1)
	f.Add(40.0, 0.9)
	f.Fuzz(func(t *testing.T, sent, share float64) {
		if !(sent >= 1 && sent <= 1e7) || !(share >= 0 && share < 1) {
			t.Skip()
		}
		clock := newTestClock()
		p := newTestPacer(clock)
		p.take(1, 25)
		p.settle(sent, sent*(1-share), sent*share)
		clock.advance(paceWindow)
		p.take(1, 25)

		rate, paced := p.current()
		// Judged the way the pacer judges it, on units, so rounding cannot split the two.
		if sent*share <= partialFloor*sent {
			if paced {
				t.Fatalf("share %v within the floor cut the rate to %v", share, rate)
			}
			return
		}
		if rate < paceBackoff*sent*(1-1e-9) || rate > sent*(1+1e-9) {
			t.Fatalf("share %v of %v/s cut the rate to %v, outside [%v, %v]", share, sent, rate, paceBackoff*sent, sent)
		}
	})
}

// TestPacerRecoversToWhereItWasCutFromWithinTheRecoveryTime verifies a restore cut by a
// refusal is back at the rate it was cut from after recoverySeconds of writing at its
// allowance. A cut is a response to a moment's pressure; staying below the table's
// capacity long after it has passed is throughput thrown away.
func TestPacerRecoversToWhereItWasCutFromWithinTheRecoveryTime(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 100)
	refuseWindow(p, clock) // 100 -> 70, aiming back at 100.

	for range int(recoverySeconds) + 1 {
		saturateWindow(p, clock)
	}

	if rate, _ := p.current(); rate < 99 || rate > 101 {
		t.Errorf("expected the rate back at the 100/s it was cut from, got %v", rate)
	}
}

// TestPacerHoldsItsRecoveryWhileTheRestoreCannotKeepUp verifies time spent below the
// allowance does not count towards recovery. A restore held back by its readers has not
// shown the table takes more, and a recovery that ran on the clock regardless would
// arrive at the old peak on credit the table never granted.
func TestPacerHoldsItsRecoveryWhileTheRestoreCannotKeepUp(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 100)
	refuseWindow(p, clock) // 100 -> 70, aiming back at 100.

	for range 10 {
		clock.advance(paceWindow)
		p.take(1, 1)
		p.settle(1, 1, 0)
	}
	saturateWindow(p, clock)
	saturateWindow(p, clock)

	// One saturated window since the cut is one step along the curve, not ten.
	if rate, _ := p.current(); rate > 90 {
		t.Errorf("expected recovery to have advanced one window, got a rate of %v", rate)
	}
}

// saturateWindow spends a second writing everything the pacer allows, with the table
// accepting all of it, and closes the previous window on the way in.
func saturateWindow(p *pacer, clock *testClock) {
	clock.advance(paceWindow)
	n, _ := p.take(1, 1<<30)
	p.settle(float64(n), float64(n), 0)
}

// near reports whether two rates agree to within rounding.
func near(got, want float64) bool {
	return math.Abs(got-want) < 1e-6*math.Max(1, math.Abs(want))
}

// refuseWindow spends one window with the table refusing every call, and closes it.
func refuseWindow(p *pacer, clock *testClock) {
	p.take(1, 1)
	p.refused(10)
	clock.advance(paceWindow)
	p.take(1, 1)
}

// TestPacerFollowsTheRecoveryCurveBetweenCutAndPeak verifies the first saturated window
// after a cut climbs most of the way back quickly, along the curve, rather than creeping
// by a fixed step or jumping straight back to where the table last refused.
func TestPacerFollowsTheRecoveryCurveBetweenCutAndPeak(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 100)
	refuseWindow(p, clock) // 100 -> 70, aiming back at 100.

	saturateWindow(p, clock)
	saturateWindow(p, clock) // Closes the first saturated window.

	// One window along a curve that reaches 100 after recoverySeconds: 100 - 0.3·100·(1-4)³/4³.
	if rate, _ := p.current(); !near(rate, 100-30*27.0/64) {
		t.Errorf("rate after one saturated window = %v, want %v", rate, 100-30*27.0/64)
	}
}

// TestPacerGrowsARateItWasNeverCutToByAQuarter verifies a rate set without a cut, and so
// with no peak to aim for, still grows while the table keeps up.
func TestPacerGrowsARateItWasNeverCutToByAQuarter(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 100)

	saturateWindow(p, clock)
	saturateWindow(p, clock)
	if rate, _ := p.current(); !near(rate, 125) {
		t.Errorf("rate = %v, want 125", rate)
	}
}

// TestPacerDoesNotRaiseTheRateInAWindowWithRefusals verifies a window in which the table
// refused a call, beyond a hot partition's share, is cut rather than counted towards
// climbing back, however much of its allowance it used. The refusal is what that window
// showed about the table.
func TestPacerDoesNotRaiseTheRateInAWindowWithRefusals(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 100)
	p.take(1, 1) // Opens a window.
	p.refused(10)
	p.settle(100, 100, 0)
	clock.advance(paceWindow)
	p.take(1, 1) // Closes it.

	if rate, _ := p.current(); !near(rate, 100*100.0/110) {
		t.Errorf("rate = %v after a window with a tenth refused, want %v", rate, 100*100.0/110)
	}
}

// TestPacerJudgesSaturationOverTheWindowsLength verifies a window longer than a second is
// judged saturated by what it used per second. Judged by its total, a quiet stretch
// between writes would read as a table keeping up and raise the rate on no evidence.
func TestPacerJudgesSaturationOverTheWindowsLength(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 10)
	p.take(1, 1)
	p.settle(10, 10, 0) // Ten units over two seconds: half the allowance.
	clock.advance(2 * paceWindow)
	p.take(1, 1)

	if rate, _ := p.current(); rate != 10 {
		t.Errorf("rate = %v after a half-used window, want 10", rate)
	}
}

// TestPacerNeverRaisesTheRateOnAHandBack verifies a window with items handed back never
// ends with a higher rate than it started with, even when the table took more than the
// restore was held to. Items handed back are the table saying less, not more.
func TestPacerNeverRaisesTheRateOnAHandBack(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 50)
	p.take(1, 1)
	p.settle(100, 90, 10)
	clock.advance(paceWindow)
	p.take(1, 1)

	if rate, _ := p.current(); rate > 50 {
		t.Errorf("rate = %v after a hand-back, above the 50 it was held to", rate)
	}
}

// TestPacerLeavesAHandBackAtTheFloorAlone verifies a share handed back exactly at the
// floor is still taken for one hot partition.
func TestPacerLeavesAHandBackAtTheFloorAlone(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.take(1, 1)
	p.settle(100, 98, 100*partialFloor)
	clock.advance(paceWindow)
	p.take(1, 1)

	if _, paced := p.current(); paced {
		t.Error("a hand-back at the floor limited writes")
	}
}

// TestPacerMeasuresAHandBackWithoutReportedCapacity verifies a table that does not report
// what it consumed is still held to what it accepted, counted from what was sent less
// what came back. Counting what came back as accepted would pace the restore above the
// table.
func TestPacerMeasuresAHandBackWithoutReportedCapacity(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.take(1, 1)
	p.settle(100, 0, 10)
	clock.advance(paceWindow)
	p.take(1, 1)

	if rate, _ := p.current(); !near(rate, 90) {
		t.Errorf("rate = %v, want the 90 the table accepted", rate)
	}
}

// TestPacerReportsEachChangeOnce verifies the rate reaches the callback once per change,
// whether the change came from a refusal or from a window closing, so the progress line
// shows the rate settle rather than flicker, and shows it at all when a window changed it.
func TestPacerReportsEachChangeOnce(t *testing.T) {
	clock := newTestClock()
	var reported []float64
	p := newTestPacer(clock)
	p.onPace = func(rate float64) { reported = append(reported, rate) }

	p.admit(context.Background(), 1, 1) // Opens a window.
	p.settle(100, 90, 10)
	clock.advance(paceWindow)
	p.admit(context.Background(), 1, 1) // Closes it: held to 90.
	p.admit(context.Background(), 1, 1) // No change.

	if len(reported) != 1 || !near(reported[0], 90) {
		t.Errorf("reported %v, want [90]", reported)
	}
}

// TestPacerHoldsNoMoreThanASecondsWorth verifies capacity does not bank up while nothing
// is written. A restore that paused for a minute would otherwise send a minute's worth at
// once into a table that takes a second's.
func TestPacerHoldsNoMoreThanASecondsWorth(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 10)
	clock.advance(time.Minute)

	if n, _ := p.take(1, 1000); n != 10 {
		t.Errorf("took %d after a minute idle at 10/s, want 10", n)
	}
}

// TestPacerWaitsInProportionToTheRate verifies the wait for capacity is the shortfall
// divided by the rate, so a faster table means a shorter wait.
func TestPacerWaitsInProportionToTheRate(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.setRateLocked(clock.Now(), 4)
	p.tokens = -3

	if _, wait := p.take(1, 1); wait != time.Second {
		t.Errorf("wait = %s for four units at 4/s, want 1s", wait)
	}
}

// TestPacerCutsInTheUnitsTheTableCharges verifies a window with items handed back holds
// the rate to what the table accepted in the units the table charged, when those differ
// from the restore's estimate. The bucket is charged in the table's units, so a rate set
// in the estimate's would let through twice what the table took, or half.
func TestPacerCutsInTheUnitsTheTableCharges(t *testing.T) {
	clock := newTestClock()
	p := newTestPacer(clock)
	p.take(1, 1)
	// Estimated at 100 units, a tenth handed back, and the ninety taken charged at 45.
	p.settle(100, 45, 10)
	clock.advance(paceWindow)
	p.take(1, 1)

	if rate, _ := p.current(); !near(rate, 45) {
		t.Errorf("rate = %v, want the 45/s the table charged for what it took", rate)
	}
}
