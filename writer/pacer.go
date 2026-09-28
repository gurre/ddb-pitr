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

// paceBackoff is the share of the rate a cut keeps at the least. Halving, the classic
// step, gives up half the table's capacity for every refusal, and a restore spends most
// of its time climbing back; keeping seven tenths is the step CUBIC settled on for the
// same trade between backing off far enough and losing little.
const paceBackoff = 0.7

// partialFloor is the share of a window's writes the table may hand back without the
// rate being cut. A batch mixes items from every file being read, so one hot partition
// shows up as a few items handed back out of many; cutting the rate for the whole table
// over it would slow every cold partition to the hot one's pace. Rejections that narrow
// are left to whoever sends the hot partition's items to slow down.
const partialFloor = 0.02

// recoverySeconds is how many seconds of writing at the full allowance take the rate
// back to where the last full cut fell from. Recovery is quick while far below it and
// slows as it nears it, then probes past it slowly at first and faster the longer the
// table keeps up.
const recoverySeconds = 4.0

// maxGrowth bounds how much one window may raise the rate, so a recovery curve fitted to
// an old peak cannot jump the rate past what the table has shown it takes.
const maxGrowth = 1.25

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
// It starts out of the way: until the table turns something away, nothing is limited,
// so an on-demand table or a generously provisioned one runs exactly as it would without
// it. From then on, requests are sized to fit a rate learned from what the table was
// observed to take, and each waits for a whole call's worth of capacity, or a second's
// worth when that is less. A small provisioned table therefore gets requests of a few
// items rather than twenty-five it can never accept whole.
//
// What the table turns away, whether items handed back from a call it otherwise took or
// a whole call refused, is weighed once a window has closed: the rate falls to the share
// the table took, by no more than paceBackoff, and not at all when the share turned away
// is small enough to be one hot partition. A table that has run dry refuses everything,
// which is a cut in full.
//
// One pacer is shared by every write in flight, because the rate being learned belongs
// to the table rather than to any one of them.
// Fields are ordered largest-to-smallest for memory alignment.
type pacer struct {
	filled   time.Time // When the bucket was last refilled
	window   time.Time // Start of the window consumption is being measured over
	clock    paceClock
	onPace   func(float64)
	rate     float64 // Units per second allowed; zero means nothing is limiting writes
	tokens   float64 // Units available to spend now
	consumed float64 // Units the table accepted within the current window
	achieved float64 // Units per second the table accepted over the previous window
	sent     float64 // Estimated units sent within the current window
	rejected float64 // Estimated units handed back within the current window
	peak     float64 // Rate the last cut fell from, which recovery aims back at first
	floor    float64 // Rate the last cut fell to, where recovery started
	epoch    float64 // Seconds of saturated writing since the last cut
	mu       sync.Mutex
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
// until they may be. It reports ok false
// when the wait was cut short because the context ended, which callers must treat as
// "stop writing".
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
// max. It waits until the bucket holds a whole call's worth, or a second's worth when
// that is less, and reports how long until it does. Handing out capacity as soon as one
// item's worth had accrued would send one-item calls: many more of them, and ones a hot
// partition can only refuse whole, never hand back in part.
func (p *pacer) take(unitCost float64, max int) (int, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.clock.Now()
	// Windows close whether or not anything is limiting writes, since a share of items
	// handed back is weighed at the close and is what sets the first rate as often as a
	// refusal does.
	p.rollLocked(now)
	if p.rate <= 0 {
		return max, 0
	}
	need := math.Min(float64(max)*unitCost, math.Max(p.rate, unitCost))
	p.refillLocked(now, need)
	if p.tokens < need {
		// A shortfall smaller than the clock can express rounds to no wait at all, and
		// asking again would spin without the bucket ever refilling. Granting one
		// request instead carries the deficit in the bucket, where the next refill
		// absorbs it, so the rate stays honest.
		if wait := p.waitForLocked(need); wait > 0 {
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

// refillLocked adds the capacity that has accrued since the last refill. It runs only
// once a rate has been set, which is also what starts the bucket filling, so there is
// never a first refill with nowhere to measure from. The bucket holds a second's worth,
// or one request's worth when that is larger, so a rate below the cost of a single item
// still lets that item through rather than deadlocking on an allowance it can never
// reach.
func (p *pacer) refillLocked(now time.Time, need float64) {
	if elapsed := now.Sub(p.filled).Seconds(); elapsed > 0 {
		p.tokens += elapsed * p.rate
		p.filled = now
	}
	capacity := math.Max(p.rate, need)
	if p.tokens > capacity {
		p.tokens = capacity
	}
}

// waitForLocked is how long until the bucket holds the given capacity.
func (p *pacer) waitForLocked(need float64) time.Duration {
	return time.Duration((need - p.tokens) / p.rate * float64(time.Second))
}

// settle records how a call went: the capacity it was estimated at, what the table
// said it consumed, and the estimated capacity of the items it handed back. The bucket
// is corrected to what the table actually charged, so items larger than their export
// lines suggested are paid for out of the next request's allowance, and items handed
// back, which consumed nothing, are paid back.
//
// A response that reports no capacity at all leaves the estimate standing. Some
// endpoints do not fill the field in, and treating that as no consumption would leave
// every window looking idle, so a rate that had been cut could never climb back.
func (p *pacer) settle(estimated, consumed, handedBack float64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.sent += estimated
	p.rejected += handedBack
	if consumed <= 0 {
		p.consumed += estimated - handedBack
		if p.rate > 0 {
			p.tokens += handedBack
		}
		return
	}
	p.consumed += consumed
	if p.rate > 0 {
		p.tokens -= consumed - estimated
	}
}

// refused records a call the table refused outright, which it does only when it could
// take none of it. It is weighed with the rest of the window when the window closes, the
// same way items handed back are: a call small enough to fall on one partition being
// refused says no more about the table as a whole than items handed back from a larger
// one do, and a table that has run dry shows as a window of nothing but refusals, which
// is cut in full.
func (p *pacer) refused(estimated float64) {
	p.settle(estimated, 0, estimated)
}

// cutLocked lowers the rate to the given share of the rate the restore was running at,
// and starts the recovery back towards that rate. A restore not yet paced is cut from
// what it was sending.
func (p *pacer) cutLocked(now time.Time, from, share float64) {
	if p.rate > 0 {
		from = p.rate
	}
	p.epoch = 0
	p.peak = from
	p.setRateLocked(now, from*math.Max(paceBackoff, share))
	p.floor = p.rate
}

// reportPace hands a rate change to the callback, outside the lock so a slow callback
// cannot hold up the writes, and once per change so an operator reading the progress
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

// rollLocked closes a finished measurement window and acts on what it showed. A window
// in which the table turned away more than a hot partition's share, handed back or
// refused, lowers the rate to the share it took, by no more than paceBackoff. One that
// was not cut and used most of its allowance is evidence the table will take more, which is how the restore climbs back
// after autoscaling or a partition split adds capacity.
func (p *pacer) rollLocked(now time.Time) {
	if p.window.IsZero() {
		// The first window opens now, so anything that happened before it, a cut
		// included, belongs to no window and must not be held against this one.
		p.openWindowLocked(now)
		return
	}
	elapsed := now.Sub(p.window)
	if elapsed < paceWindow {
		return
	}

	seconds := elapsed.Seconds()
	p.achieved = p.consumed / seconds
	switch {
	case p.rejected > partialFloor*p.sent:
		p.cutLocked(now, p.sentRateLocked(seconds), (p.sent-p.rejected)/p.sent)
	case p.rate > 0 && p.consumed >= paceSaturation*p.rate*seconds:
		p.epoch += seconds
		p.setRateLocked(now, p.recoveryRateLocked())
	}
	p.openWindowLocked(now)
}

// openWindowLocked starts measuring a new window from now.
func (p *pacer) openWindowLocked(now time.Time) {
	p.window = now
	p.consumed = 0
	p.sent = 0
	p.rejected = 0
}

// sentRateLocked is the rate the window that just closed was sending at, in the units
// the table charges, since those are what the bucket is paid in: what the table took,
// scaled up by the share it turned away. A window in which the table took next to
// nothing has too little accepted to scale from, and is measured by its estimate.
func (p *pacer) sentRateLocked(seconds float64) float64 {
	accepted := p.sent - p.rejected
	if accepted <= 0.01*p.sent || p.achieved <= 0 {
		return p.sent / seconds
	}
	return p.achieved * p.sent / accepted
}

// recoveryRateLocked is the rate after one more saturated window since the last cut,
// following CUBIC's curve: it climbs quickly while far below the rate the cut fell
// from, levels off as it reaches it, and probes past it slowly at first. Growth in one
// window is bounded above by maxGrowth and below by a unit, so a restore always moves.
func (p *pacer) recoveryRateLocked() float64 {
	if p.peak <= 0 {
		// Paced without ever having been cut, so there is no curve to follow.
		return p.rate + math.Max(1, p.rate*(maxGrowth-1))
	}
	scale := p.peak * (1 - paceBackoff) / (recoverySeconds * recoverySeconds * recoverySeconds)
	reach := math.Cbrt(math.Max(0, p.peak-p.floor) / scale)
	lag := p.epoch - reach
	target := p.peak + scale*lag*lag*lag
	return math.Max(math.Min(target, p.rate*maxGrowth), p.rate+1)
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
