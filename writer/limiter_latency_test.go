package writer

import (
	"testing"
	"testing/synctest"
	"time"
)

// drive runs the limiter against the path for the given number of seconds and returns,
// for each second, the limit in force and the calls answered. It runs inside a synctest
// bubble the caller has already entered.
func drive(l *concurrencyLimiter, p *path, seconds int) (limits, answered []int) {
	for range seconds {
		limit, _ := l.current()
		limits = append(limits, limit)
		answered = append(answered, p.answer(l, limit))
	}
	return limits, answered
}

// TestLimiterKeepsThroughputWhenThePathGetsSlower verifies a restore whose calls start
// taking three times as long is back to answering as many calls a second within a few
// seconds. Each call then holds its slot three times as long, so the same limit would
// carry a third of the calls; the limit has to follow the latency up.
func TestLimiterKeepsThroughputWhenThePathGetsSlower(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newPath(20*time.Millisecond, 0.5)
		p.supply = 2000 // Forty calls on the wire at 20ms.
		l := newConcurrencyLimiter(DefaultMaxInFlight, nil)
		drive(l, p, 20)

		p.median = 60 * time.Millisecond
		_, answered := drive(l, p, 20)
		for s, n := range answered[2:] {
			if float64(n) < 0.95*p.supply {
				t.Fatalf("%ds after the path slowed, %d calls answered of the %v the restore had to send", s+2, n, p.supply)
			}
		}
	})
}

// TestLimiterLetsGoOfAKneeWhenThePathGetsSlower verifies a limiter held at a knee on a
// fast path is back to the knee's rate within seconds of every call taking three times
// as long. The knee was found as a number of calls in flight; on the slower path the same
// rate takes three times as many, and holding the old number would cut the rate to a
// third for as long as the knee was trusted.
func TestLimiterLetsGoOfAKneeWhenThePathGetsSlower(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newPath(20*time.Millisecond, 0.5)
		p.knee = 5000
		l := newConcurrencyLimiter(DefaultMaxInFlight, nil)
		drive(l, p, 30)

		p.median = 60 * time.Millisecond
		limits, answered := drive(l, p, 40)
		for s, n := range answered[3:] {
			if float64(n) < 0.95*p.knee {
				t.Fatalf("%ds after the path slowed, %d calls answered of the %v it carries", s+3, n, p.knee)
			}
		}
		need := p.knee * p.median.Seconds()
		for s, limit := range limits[3:] {
			if float64(limit) > 2.2*need {
				t.Fatalf("%ds after the path slowed, the limit was %d for %v needed", s+3, limit, need)
			}
		}
	})
}

// TestLimiterComesBackDownAfterALatencySpike verifies a few seconds in which every call
// took twenty times as long leave no lasting mark: once latency is back, the limit is back
// to what the restore uses and every call it has to send goes. A spike taken for a knee,
// or for the new normal, would hold the restore at the wrong limit long after it passed.
func TestLimiterComesBackDownAfterALatencySpike(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newPath(20*time.Millisecond, 0.5)
		p.supply = 2000 // Forty calls on the wire at 20ms.
		l := newConcurrencyLimiter(DefaultMaxInFlight, nil)
		drive(l, p, 20)

		p.median = 400 * time.Millisecond
		drive(l, p, 3)
		p.median = 20 * time.Millisecond
		limits, answered := drive(l, p, 20)
		for s := 2; s < len(limits); s++ {
			if float64(answered[s]) < 0.95*p.supply || limits[s] > 2*int(2.2*p.supply*p.median.Seconds()) {
				t.Fatalf("%ds after the spike, limit %d answered %d of %v", s, limits[s], answered[s], p.supply)
			}
		}
	})
}

// TestLimiterStopsAtItsCeiling verifies a path so slow that carrying what the restore has
// to send would take more than DefaultMaxInFlight calls holds the limit at DefaultMaxInFlight and no
// higher. The ceiling is what bounds memory and connections; past it, the progress line
// showing every allowed write in flight is how an operator sees it is the limit.
func TestLimiterStopsAtItsCeiling(t *testing.T) {
	for _, limit := range settle(t, newPath(2*time.Second, 0.5), 30) {
		if limit != DefaultMaxInFlight {
			t.Fatalf("limit %d on a path needing more than %d calls in flight", limit, DefaultMaxInFlight)
		}
	}
}

// TestLimiterKeepsItsLimitThroughAnIdleSpell verifies seconds in which no call was
// answered leave the limit where it was. A restore that stops writing for a while, while
// it verifies the export or waits on a slow read, has shown nothing about how many calls
// the table takes, and dropping the limit to one would cost it seconds of climbing back.
func TestLimiterKeepsItsLimitThroughAnIdleSpell(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newPath(20*time.Millisecond, 0.5)
		p.knee = 5000
		l := newConcurrencyLimiter(DefaultMaxInFlight, nil)
		drive(l, p, 30)
		before, _ := l.current()

		time.Sleep(5 * time.Second)
		l.sample(p.median) // The first call answered after the spell closes the window.
		if after, _ := l.current(); after != before {
			t.Errorf("limit went from %d to %d over a spell with no calls answered", before, after)
		}
	})
}

// TestLimiterFollowsAKneeThatMovesUp verifies a restore held at a knee reaches the higher
// rate a path carries once it gains capacity, such as a machine freed of other work. A
// knee held for good would keep the restore at the rate of its worst moment.
func TestLimiterFollowsAKneeThatMovesUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newPath(20*time.Millisecond, 0.5)
		p.knee = 5000
		l := newConcurrencyLimiter(DefaultMaxInFlight, nil)
		drive(l, p, 30)

		p.knee = 10000
		_, answered := drive(l, p, 30)
		if last := answered[len(answered)-1]; float64(last) < 0.95*p.knee {
			t.Errorf("30s after the path doubled its capacity, %d calls answered of %v", last, p.knee)
		}
	})
}

// TestLimiterHoldsToTheCeilingItWasGiven verifies a ceiling set below the default is the
// most the limiter allows, however slow the path. It is what an operator sets to bound a
// restore's memory and connections, and a ceiling that was exceeded would bound nothing.
func TestLimiterHoldsToTheCeilingItWasGiven(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const ceiling = 48
		l := newConcurrencyLimiter(ceiling, nil)
		limits, _ := drive(l, newPath(2*time.Second, 0.5), 20)
		for s, limit := range limits {
			if limit > ceiling {
				t.Fatalf("limit %d at %ds, above the ceiling of %d", limit, s, ceiling)
			}
		}
		if last := limits[len(limits)-1]; last != ceiling {
			t.Errorf("limit %d on a path needing more, want the ceiling of %d", last, ceiling)
		}
	})
}

// TestLimiterStartsNoHigherThanItsCeiling verifies a ceiling below where the limiter
// starts holds from the first call, not only once the limit has first been moved.
func TestLimiterStartsNoHigherThanItsCeiling(t *testing.T) {
	if limit, _ := newConcurrencyLimiter(4, nil).current(); limit != 4 {
		t.Errorf("limit %d before anything was measured, want the ceiling of 4", limit)
	}
}
