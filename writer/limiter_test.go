package writer

import (
	"math"
	"math/rand/v2"
	"testing"
	"testing/synctest"
	"time"
)

// path models what the table, the network and the machine between them do with a given
// number of calls in flight: how many are answered each second and how long each takes.
// Latency is drawn around a median with lognormal jitter; beyond knee calls a second, or
// supply batches a second, nothing more is answered however many are in flight.
type path struct {
	rng    *rand.Rand
	median time.Duration
	sigma  float64
	knee   float64 // Calls a second the path can carry; 0 for no limit
	supply float64 // Batches a second the restore has to send; 0 for no limit
}

func newPath(median time.Duration, sigma float64) *path {
	return &path{rng: rand.New(rand.NewPCG(1, 2)), median: median, sigma: sigma}
}

// answer runs one second of calls at the given limit through the limiter, and reports
// how many were answered. Calls queue behind a knee, so their latency grows with them.
func (p *path) answer(l *concurrencyLimiter, limit int) int {
	perCall := p.median.Seconds()
	calls := float64(limit) / perCall
	queued := 1.0
	if p.knee > 0 && calls > p.knee {
		queued = calls / p.knee
		calls = p.knee
	}
	if p.supply > 0 && calls > p.supply {
		calls = p.supply
	}
	// Calls are answered through the second, a tenth at a time, the way they arrive.
	n := int(calls)
	for tenth := range 10 {
		time.Sleep(time.Second / 10)
		for range n*(tenth+1)/10 - n*tenth/10 {
			jitter := math.Exp(p.sigma * p.rng.NormFloat64())
			l.sample(time.Duration(float64(p.median) * queued * jitter))
		}
	}
	return n
}

// settle runs the limiter against the path for the given number of seconds and returns
// the limits it held over the last half, where it has settled.
func settle(t *testing.T, p *path, seconds int) []int {
	t.Helper()
	var limits []int
	synctest.Test(t, func(t *testing.T) {
		l := newConcurrencyLimiter(DefaultMaxInFlight, nil)
		for s := range seconds {
			limit, _ := l.current()
			p.answer(l, limit)
			if s >= seconds/2 {
				limits = append(limits, limit)
			}
		}
	})
	return limits
}

// TestLimiterOpensUpWhenNothingHoldsItBack verifies a restore nothing limits reaches the
// most calls it will hold within seconds, however jittery the table's latency. A limiter
// that took jitter for queueing would hold a large table to a few calls in flight.
func TestLimiterOpensUpWhenNothingHoldsItBack(t *testing.T) {
	for _, sigma := range []float64{0, 0.5, 1} {
		limits := settle(t, newPath(20*time.Millisecond, sigma), 30)
		for _, limit := range limits {
			if limit != DefaultMaxInFlight {
				t.Fatalf("with jitter %v the limit fell to %d with nothing holding it back", sigma, limit)
			}
		}
	}
}

// TestLimiterSettlesNearAKnee verifies a path that carries no more than a certain number
// of calls a second holds the limit near what that takes, rather than piling calls up
// behind it. Calls beyond the knee add latency and memory and nothing else.
func TestLimiterSettlesNearAKnee(t *testing.T) {
	for _, knee := range []float64{500, 5000, 20000} {
		p := newPath(20*time.Millisecond, 0.5)
		p.knee = knee
		need := knee * p.median.Seconds()
		for _, limit := range settle(t, p, 60) {
			if float64(limit) < 0.9*need || float64(limit) > 2.2*need {
				t.Errorf("knee at %v calls/s (needing %v in flight) held a limit of %d", knee, need, limit)
			}
		}
	}
}

// TestLimiterFollowsWhatTheRestoreUses verifies a restore reading slower than it could
// write holds a limit sized to what it uses. A limit far above that is one nothing has
// tested, and the first burst from the readers would send that many calls at once.
func TestLimiterFollowsWhatTheRestoreUses(t *testing.T) {
	p := newPath(20*time.Millisecond, 0.5)
	p.supply = 200 // Four calls on the wire on average.
	for _, limit := range settle(t, p, 30) {
		if limit > 16 {
			t.Errorf("limit %d held for four calls in use", limit)
		}
	}
}

// TestLimiterNeverFallsBelowOneCall verifies a restore whose calls spent no measurable
// time on the wire still has a call it may send. A limit of none would stop the restore
// for good.
func TestLimiterNeverFallsBelowOneCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newConcurrencyLimiter(DefaultMaxInFlight, nil)
		for range 100 {
			time.Sleep(time.Second / 10)
			l.sample(0)
		}
		if limit, _ := l.current(); limit < 1 {
			t.Errorf("limit fell to %d", limit)
		}
	})
}

// TestLimiterReportsItsLimitAsItChanges verifies the limit reaches whoever reports it,
// once per change. An operator reading the progress line needs to see how many writes
// the restore settled on, or a slow restore gives no hint which end is holding it.
func TestLimiterReportsItsLimitAsItChanges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var reported []int
		l := newConcurrencyLimiter(DefaultMaxInFlight, func(limit int) { reported = append(reported, limit) })
		p := newPath(20*time.Millisecond, 0)
		for range 5 {
			limit, _ := l.current()
			p.answer(l, limit)
		}
		if len(reported) < 2 || reported[0] != initialConcurrency {
			t.Fatalf("reported %v, want the initial limit then each change", reported)
		}
		for i := 1; i < len(reported); i++ {
			if reported[i] == reported[i-1] {
				t.Errorf("limit %d reported twice in a row", reported[i])
			}
		}
	})
}
