package writer

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/gurre/ddb-pitr/itemimage"
)

// paceWindow is how long the pacer measures consumption over before it decides whether
// the table has room for more. One second matches the unit DynamoDB provisions in.
const paceWindow = time.Second

// minPaceRate is the slowest the pacer will go. A table with no capacity at all still
// gets one unit a second, which is what discovers that capacity has come back.
const minPaceRate = 1.0

// paceIncrease is how much of the current rate a clean, saturated window adds back.
// Growth is additive so the rate creeps up to a new ceiling instead of overshooting it
// the way a doubling would, and the floor of one unit keeps a very slow restore moving.
const paceIncrease = 0.10

// paceSaturation is the share of the allowance a window must have used before the rate
// is raised. Without it a restore held back by its readers would grow the rate without
// limit, and the next throttle would have to climb down from a number the table never
// accepted.
const paceSaturation = 0.9

// bytesPerWriteUnit is the item size one write capacity unit covers.
const bytesPerWriteUnit = 1024

// paceClock is where the pacer reads the time and does its waiting. Sleep reports
// false when the wait was cut short, which callers must treat as "stop writing".
type paceClock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) bool
}

// wallClock is the clock a restore runs on.
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// Sleep waits for d, reporting false if the context ends first.
func (wallClock) Sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// pacer keeps the restore's write rate to what the target table is accepting.
//
// It starts out of the way: until something throttles, nothing is limited, so an
// on-demand table or a generously provisioned one runs exactly as it would without it.
// The first throttle sets a rate from what the table was observed to take, and from
// then on requests are sized to fit that rate. A small provisioned table therefore gets
// requests of one or two items rather than twenty-five it can never accept whole, which
// is the difference between restoring at the table's capacity and restoring at whatever
// survives an exponential backoff.
//
// One pacer is shared by every writer goroutine, because the rate being learned belongs
// to the table rather than to a goroutine.
// Fields are ordered largest-to-smallest for memory alignment.
type pacer struct {
	started  time.Time // When the first write was admitted
	filled   time.Time // When the bucket was last refilled
	window   time.Time // Start of the window consumption is being measured over
	lastCut  time.Time // When the rate was last reduced
	clock    paceClock
	onPace   func(float64)
	rate     float64 // Units per second allowed; zero means nothing is limiting writes
	tokens   float64 // Units available to spend now
	consumed float64 // Units consumed within the current window
	achieved float64 // Units per second the previous window actually consumed
	mu       sync.Mutex
	cut      bool // The rate was reduced within the current window
	pending  bool // The rate changed and the change has not been reported yet
}

// newPacer returns a pacer that limits nothing until the table first refuses a write.
func newPacer(onPace func(float64)) *pacer {
	return &pacer{clock: wallClock{}, onPace: onPace}
}

// writeCost is the write capacity one operation is expected to consume: a unit per
// kilobyte of the item, rounded up. The export line an operation was decoded from is
// longer than the item DynamoDB measures, so this over-estimates rather than under,
// and the consumed capacity the table reports corrects it on the next request.
// A delete is charged by its key alone, which is one unit.
func writeCost(op itemimage.Operation) float64 {
	if op.Type == itemimage.OpDelete {
		return 1
	}
	units := math.Ceil(float64(op.Bytes) / bytesPerWriteUnit)
	if units < 1 {
		return 1
	}
	return units
}

// admit reports how many of the leading requests may be sent as one call, waiting
// until at least one may be. It reports false when the wait was cut short because the
// context ended, which callers must treat as "stop writing".
//
// The answer is the request size. That is what makes a low rate produce small requests
// instead of large ones the table hands most of back, which is the difference between
// restoring at a small table's capacity and restoring at whatever survives a backoff.
func (p *pacer) admit(ctx context.Context, unitCost float64, max int) (int, bool) {
	for {
		n, wait := p.take(unitCost, max)
		if n > 0 {
			p.reportPace()
			return n, true
		}
		if !p.clock.Sleep(ctx, wait) {
			return 0, false
		}
	}
}

// take debits as many requests as the bucket can pay for at the given cost each, up to
// max. When it cannot pay for even one, it reports how long until it can.
func (p *pacer) take(unitCost float64, max int) (int, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Measurement runs from the first write, not from the first limit. What the table
	// was taking before it refused anything is the only evidence of what it can take,
	// and a first refusal with nothing measured has to fall all the way to the floor.
	if p.started.IsZero() {
		p.started = p.clock.Now()
	}
	if p.rate <= 0 {
		return max, 0
	}
	p.refillLocked(unitCost)
	if p.tokens < unitCost {
		// A shortfall smaller than the clock can express rounds to no wait at all, and
		// asking again would spin without the bucket ever refilling. Granting one
		// request instead carries the deficit in the bucket, where the next refill
		// absorbs it, so the rate stays honest.
		if wait := p.waitForLocked(unitCost); wait > 0 {
			return 0, wait
		}
	}

	n := int(p.tokens / unitCost)
	if n > max {
		n = max
	}
	if n < 1 {
		n = 1
	}
	p.tokens -= float64(n) * unitCost
	return n, 0
}

// refillLocked adds the capacity that has accrued since the last refill and rolls the
// measurement window. It runs only once a rate has been set, which is also what starts
// the bucket filling, so there is never a first refill with nowhere to measure from. The bucket holds a second's worth, or one request's worth when
// that is larger, so a rate below the cost of a single item still lets that item
// through rather than deadlocking on an allowance it can never reach.
func (p *pacer) refillLocked(need float64) {
	now := p.clock.Now()
	if elapsed := now.Sub(p.filled).Seconds(); elapsed > 0 {
		p.tokens += elapsed * p.rate
		p.filled = now
	}
	capacity := math.Max(p.rate, need)
	if p.tokens > capacity {
		p.tokens = capacity
	}
	p.rollLocked(now)
}

// waitForLocked is how long until the bucket holds the given capacity.
func (p *pacer) waitForLocked(need float64) time.Duration {
	return time.Duration((need - p.tokens) / p.rate * float64(time.Second))
}

// settle corrects the bucket by what the table actually charged, so a request whose
// items were larger than their export lines suggested is paid for out of the next
// request's allowance rather than accumulating as a rate the table never granted.
//
// A response that reports no capacity at all leaves the estimate standing. Some
// endpoints do not fill the field in, and treating that as no consumption would leave
// every window looking idle, so a rate that had been cut could never climb back.
func (p *pacer) settle(estimated, consumed float64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if consumed <= 0 {
		p.consumed += estimated
		return
	}
	p.consumed += consumed
	if p.rate > 0 {
		p.tokens -= consumed - estimated
	}
}

// throttled reports that the table refused work, and cuts the rate to half of what the
// table was last seen to accept.
//
// At most one cut lands per window. A batch is deliberately spread across partitions,
// so one hot partition hands a couple of items back to several writers in the same
// instant; counting those as separate signals would collapse the rate to the floor over
// a condition that warranted one step down.
func (p *pacer) throttled() {
	p.mu.Lock()
	now := p.clock.Now()
	if p.lastCut.IsZero() || now.Sub(p.lastCut) >= paceWindow {
		p.lastCut = now
		p.cut = true
		p.setRateLocked(now, p.stepDownFromLocked(now)/2)
	}
	p.mu.Unlock()

	p.reportPace()
}

// reportPace hands a rate change to the callback, outside the lock so a slow callback
// cannot hold up the writers, and once per change so an operator reading the progress
// line sees the rate settle rather than flicker.
func (p *pacer) reportPace() {
	p.mu.Lock()
	rate, changed := p.rate, p.pending
	p.pending = false
	p.mu.Unlock()

	if changed && p.onPace != nil {
		p.onPace(rate)
	}
}

// stepDownFromLocked is the rate a cut halves. The observed rate is what lets the first
// cut drop straight to what the table was seen to take rather than inching down from
// no limit at all; once a rate is set, that rate is the one to halve, or a table that
// goes on refusing would be cut to the same number every window and never converge.
func (p *pacer) stepDownFromLocked(now time.Time) float64 {
	observed := p.observedLocked(now)
	if p.rate > 0 && p.rate < observed {
		return p.rate
	}
	return observed
}

// observedLocked is the rate the table was last seen to accept: the previous full
// window when there was one, otherwise what the current partial window has managed.
// A restore that has not written anything yet has nothing to halve, and starts from
// the floor.
func (p *pacer) observedLocked(now time.Time) float64 {
	if p.achieved > 0 {
		return p.achieved
	}
	// Before the first window closes, the measurement runs from the first write.
	since := p.window
	if since.IsZero() {
		since = p.started
	}
	if elapsed := now.Sub(since).Seconds(); elapsed > 0 && p.consumed > 0 && !since.IsZero() {
		return p.consumed / elapsed
	}
	if p.rate > 0 {
		return p.rate
	}
	return minPaceRate
}

// rollLocked closes a finished measurement window. A window that was not cut and that
// used up most of its allowance is evidence the table will take more, which is how the
// restore climbs back after autoscaling or a partition split adds capacity.
func (p *pacer) rollLocked(now time.Time) {
	if p.window.IsZero() {
		// The first window opens now, so anything that happened before it, a cut
		// included, belongs to no window and must not be held against this one.
		p.window = now
		p.consumed = 0
		p.cut = false
		return
	}
	elapsed := now.Sub(p.window)
	if elapsed < paceWindow {
		return
	}

	p.achieved = p.consumed / elapsed.Seconds()
	if p.rate > 0 && !p.cut && p.consumed >= paceSaturation*p.rate*elapsed.Seconds() {
		p.setRateLocked(now, p.rate+math.Max(1, p.rate*paceIncrease))
	}
	p.consumed = 0
	p.cut = false
	p.window = now
}

// setRateLocked moves the allowed rate, keeping it above the floor and the bucket
// within the new ceiling, and notes that the change is worth reporting. The floor is
// applied here and nowhere else, so there is one place for it to be got right.
func (p *pacer) setRateLocked(now time.Time, rate float64) {
	if rate < minPaceRate {
		rate = minPaceRate
	}
	if rate == p.rate {
		return
	}
	// Capacity accrues from the moment there is a rate to accrue it at. Left until the
	// next request, the wait the table was given credit for would be spent twice.
	if p.filled.IsZero() {
		p.filled = now
	}
	p.rate = rate
	p.pending = true
	if p.tokens > rate {
		p.tokens = rate
	}
}

// current reports the rate the table is being held to, and whether anything is holding
// it at all.
func (p *pacer) current() (float64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rate, p.rate > 0
}
