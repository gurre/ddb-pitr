package writer

import (
	"context"
	"sync"
	"time"
)

// DefaultMaxInFlight is the most calls the writer has in flight at once unless told
// otherwise with WithMaxInFlight. How many are in flight follows what the table and the
// restore can use, and settles below this wherever something else is the limit; this
// bounds what a restore nothing else limits holds, in memory and in connections.
const DefaultMaxInFlight = 1024

// HighestMaxInFlight is the most calls in flight WithMaxInFlight accepts. A restore is
// one client among the table's, the account's and the network's others, and this is
// the most it may ever throw at them at once: at a tenth of a second a call it is some
// forty thousand calls a second, past what any one table takes.
const HighestMaxInFlight = 4096

// initialConcurrency is how many calls may be in flight before anything is measured.
const initialConcurrency = 16

// limitWindow is how long the limiter measures over before it moves the limit.
const limitWindow = time.Second

// limitHeadroom is how far above what was in use the limit is set: room for the use to
// double before the limit binds, and a doubling per window while it does.
const limitHeadroom = 2.0

// kneeUse is the share of the limit a window has to have used for its throughput to say
// anything about whether a higher limit paid.
const kneeUse = 0.75

// kneeGain is the share of a raise's proportional gain in throughput below which the
// raise is taken not to have paid: the limit went up by a half, so throughput should
// have gone up by at least a quarter.
const kneeGain = 0.5

// kneeHold is how many windows the rate stays held to a knee once found, before it
// tries above it again, since what the machine and the path can carry changes too.
const kneeHold = 10

// kneeStep is how far above a knee each window after the hold may try. A doubling from
// below a knee lands past it, and falling back to where it started would leave the limit
// short of the knee for good; stepping by a quarter walks up to it instead.
const kneeStep = 1.25

// kneeRelease is the share of a knee's rate below which a window that used its limit
// is taken to show a slower path rather than the knee: at a knee the rate holds.
const kneeRelease = 0.8

// concurrencyLimiter decides how many calls may be in flight. Once a window it sets the
// limit to twice the calls that were on the wire on average, which is the calls answered
// per second times how long each was on the wire. That is what the restore used, whatever
// held it there: the table's rate, reading the export, or the limit itself, in which case
// twice it is a doubling. Time a call spent waiting for the table's capacity before it
// was sent is not time on the wire, so a restore the table's rate holds back keeps only
// the calls that rate pays for.
//
// Latency is not compared with any baseline, so jitter cannot be mistaken for queueing.
// What is compared is throughput: a window at a raised limit that used it and answered
// barely more calls than the window before found a knee, somewhere between the restore
// and the table. The limit goes back as far as the raise did not pay for kneeHold windows,
// then tries a kneeStep above it a window at a time, so it finds the knee rather than stopping
// short of it, and follows it if it moves. A window that used its limit and answered
// well under the knee's rate shows a path that got slower, not a knee, and lets it go:
// a slower path needs more calls in flight for the same rate.
// Fields are ordered largest-to-smallest for memory alignment.
type concurrencyLimiter struct {
	window    time.Time     // Start of the window being measured
	last      time.Time     // When the last call was answered
	kneeUntil time.Time     // Until when the limit stays at or below knee
	wake      chan struct{} // Closed and replaced whenever a slot may have opened
	onLimit   func(int)
	ceiling   float64 // Most calls ever let be in flight
	limit     float64
	previous  float64 // Limit in force over the window before this one
	answered  float64 // Calls per second the window before answered
	knee      float64 // Limit above which more calls did not pay; 0 until one is found
	kneeRate  float64 // Calls per second the knee limit answered when the knee was found
	onWire    float64 // Seconds calls spent on the wire in this window, summed
	calls     int     // Calls answered in this window
	inFlight  int
	reported  int
	mu        sync.Mutex
}

// newConcurrencyLimiter starts at initialConcurrency calls in flight, or the ceiling
// when that is lower, and never lets more than the ceiling be in flight.
func newConcurrencyLimiter(ceiling int, onLimit func(int)) *concurrencyLimiter {
	start := float64(min(initialConcurrency, ceiling))
	return &concurrencyLimiter{
		wake:     make(chan struct{}),
		onLimit:  onLimit,
		ceiling:  float64(ceiling),
		limit:    start,
		previous: start,
		window:   time.Now(),
	}
}

// acquire waits for a slot, returning the context's error if it ends first.
func (l *concurrencyLimiter) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		l.mu.Lock()
		if l.inFlight < int(l.limit) {
			l.inFlight++
			l.mu.Unlock()
			return nil
		}
		wake := l.wake
		l.mu.Unlock()

		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// release gives a slot back.
func (l *concurrencyLimiter) release() {
	l.mu.Lock()
	l.inFlight--
	l.wakeLocked()
	l.mu.Unlock()
}

// wakeLocked lets every waiting acquire look again.
func (l *concurrencyLimiter) wakeLocked() {
	close(l.wake)
	l.wake = make(chan struct{})
}

// sample records a call the table answered, refusals included, and how long it was on
// the wire, closing the window when it is over. A window closes only on a call, so a
// spell in which nothing was answered moves nothing: it is no evidence either way.
func (l *concurrencyLimiter) sample(onWire time.Duration) {
	l.mu.Lock()
	now := time.Now()
	if elapsed := now.Sub(l.window); elapsed >= limitWindow {
		// A window a pause cut short is measured over the span its calls were answered
		// in, since the pause says nothing about how many calls the restore uses, and
		// counting it would spread the calls before it thin and cut the limit for a
		// pause. Under half a window of calls is too little to go on, and moves nothing.
		span := elapsed
		if now.Sub(l.last) >= limitWindow {
			span = l.last.Sub(l.window)
		}
		if span >= limitWindow/2 {
			l.closeWindowLocked(now, span.Seconds())
		} else {
			l.window, l.onWire, l.calls = now, 0, 0
		}
	}
	l.last = now
	l.onWire += onWire.Seconds()
	l.calls++
	limit, changed := int(l.limit), int(l.limit) != l.reported
	l.reported = limit
	l.mu.Unlock()

	if changed && l.onLimit != nil {
		l.onLimit(limit)
	}
}

// closeWindowLocked moves the limit on what the window that just ended showed.
func (l *concurrencyLimiter) closeWindowLocked(now time.Time, seconds float64) {
	answered := float64(l.calls) / seconds
	used := l.onWire / seconds
	next := limitHeadroom * used
	binding := used >= kneeUse*l.limit

	switch {
	case l.knee == 0 || !now.Before(l.kneeUntil):
		// Probing: a raise that was used and answered barely more calls than the
		// window before found a knee. How much more it answered says how much of the
		// raise paid: a doubling that crossed the knee gained some, and the knee lies that
		// share of the way up, not back where the raise started.
		if l.limit > l.previous && binding && answered < l.answered*(1+kneeGain*(l.limit/l.previous-1)) {
			paid := min(max(1, answered/l.answered), l.limit/l.previous)
			l.knee, l.kneeRate, l.kneeUntil = l.previous*paid, answered, now.Add(kneeHold*limitWindow)
		} else if l.knee > 0 {
			l.knee *= kneeStep
		}
	case binding && answered < kneeRelease*l.kneeRate:
		// At a knee the rate holds while the limit does. A rate well below it at the
		// same limit is every call taking longer: the path got slower, which takes more
		// calls in flight to carry, and the knee found on the faster path no longer
		// says where more calls stop paying.
		l.knee = 0
	}
	if l.knee > 0 {
		next = min(next, l.knee)
	}

	l.previous, l.answered = l.limit, answered
	l.limit = max(1, min(l.ceiling, next))
	if l.limit > l.previous {
		l.wakeLocked()
	}
	l.window, l.onWire, l.calls = now, 0, 0
}

// current reports the limit and how many calls are in flight.
func (l *concurrencyLimiter) current() (limit, inFlight int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int(l.limit), l.inFlight
}
