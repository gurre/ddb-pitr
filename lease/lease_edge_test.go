package lease_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gurre/ddb-pitr/lease"
)

// TestClaimNamesTheRestoreHoldingIt verifies the lease records which restore holds the
// table, and that a restore refused names it. An operator told only that the table is
// held cannot tell whether to wait, or where to go and stop the other restore.
func TestClaimNamesTheRestoreHoldingIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		first := mustAcquire(t, store.client(), "host-a")
		defer release(t, first)

		rec := store.record(t)
		if rec.Table != "orders" || rec.TTL != ttl || rec.Owner == "" {
			t.Errorf("lease record = %+v, want it to name the table, the TTL and an owner", rec)
		}
		_, err := lease.Acquire(caller(t), store.client(), uri, holder("host-b"))
		if err == nil || !strings.Contains(err.Error(), "host-a pid 42") || !strings.Contains(err.Error(), "s3://exports/e1") {
			t.Errorf("refusal %q does not name the holder and what it is restoring", err)
		}
	})
}

// TestAcquireTakesOverWhenTheHolderLetsGoWhileWatched verifies a challenger watching a
// held lease takes it as soon as the holder releases it, rather than waiting out the
// TTL. A restore started while another is finishing would otherwise wait for nothing.
func TestAcquireTakesOverWhenTheHolderLetsGoWhileWatched(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		first := mustAcquire(t, store.client(), "host-a")
		c := store.client()

		// The challenger reads the lease while it is held, then the holder lets go.
		var second *lease.Lease
		done := make(chan struct{})
		go func() {
			defer close(done)
			l, err := lease.Acquire(caller(t), c, uri, holder("host-b"))
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			second = l
		}()
		synctest.Wait()
		start := time.Now()
		release(t, first)
		<-done

		if second == nil || time.Since(start) >= ttl {
			t.Fatalf("challenger took %s to take a released lease", time.Since(start))
		}
		release(t, second)
	})
}

// TestDeadlineMovesWithEachRenewal verifies a holder's deadline is counted from its
// latest renewal, not from when it acquired the table. Counted from acquisition, every
// restore would stop twenty seconds in.
func TestDeadlineMovesWithEachRenewal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		c := store.client()
		l := mustAcquire(t, c, "host-a")
		defer func() { c.failPuts.Store(false); release(t, l) }()

		time.Sleep(renewEvery)
		synctest.Wait()
		c.failPuts.Store(true)

		time.Sleep(fenceAfter - time.Nanosecond)
		synctest.Wait()
		if err := l.Check(); err != nil {
			t.Fatalf("stopped before the deadline the renewal set: %v", err)
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if err := l.Check(); !errors.Is(err, lease.ErrLeaseLost) {
			t.Fatalf("Check at the renewal's deadline = %v, want ErrLeaseLost", err)
		}
	})
}

// TestAdoptedRenewalCountsFromItsFirstAttempt verifies a renewal that landed although its
// answer was lost moves the deadline from when that renewal was first sent, not from the
// later attempt that found it had landed. The write took effect when it was first sent;
// counting from later would let the holder write past the moment a challenger could have
// taken over.
func TestAdoptedRenewalCountsFromItsFirstAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		c := store.client()
		l := mustAcquire(t, c, "host-a")
		defer func() { c.failPuts.Store(false); release(t, l) }()

		// The renewal due at renewEvery lands but reports failure; its retry finds it.
		c.landThenFail.Store(1)
		time.Sleep(renewEvery)
		synctest.Wait()
		firstSent := time.Now()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		c.failPuts.Store(true)

		time.Sleep(fenceAfter - time.Since(firstSent))
		synctest.Wait()
		if err := l.Check(); !errors.Is(err, lease.ErrLeaseLost) {
			t.Fatalf("Check %s after the adopted renewal was first sent = %v, want ErrLeaseLost", fenceAfter, err)
		}
	})
}

// TestHungRenewalIsAbandonedInTimeToRetry verifies a renewal whose request never answers
// is given up on and tried again well before the holder's deadline. A connection that
// hangs would otherwise use up the whole margin and stop a healthy restore.
func TestHungRenewalIsAbandonedInTimeToRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		c := store.client()
		l := mustAcquire(t, c, "host-a")
		defer release(t, l)

		c.hangPuts.Store(1)
		time.Sleep(3 * ttl)
		synctest.Wait()
		if err := l.Check(); err != nil {
			t.Fatalf("one hung renewal stopped the restore: %v", err)
		}
	})
}

// TestCheckReportsATakeoverBeforeTheDeadline verifies Check refuses a holder whose lease
// was taken over even though its own deadline has not come. The deadline only bounds a
// holder that cannot hear from S3; one that has heard it lost must stop at once.
func TestCheckReportsATakeoverBeforeTheDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		l := mustAcquire(t, store.client(), "host-a")
		defer release(t, l)

		store.overwrite(t, recordOf("host-b", time.Now()))
		time.Sleep(renewEvery)
		synctest.Wait()
		if err := l.Check(); !errors.Is(err, lease.ErrLeaseLost) || !strings.Contains(err.Error(), "host-b") {
			t.Fatalf("Check after a takeover = %v, want ErrLeaseLost naming host-b", err)
		}
	})
}

// TestAcquireFailsWhenS3CannotBeReached verifies a restore that cannot reach the lease
// stops at the start with the reason, rather than restoring unclaimed. A restore that
// could not claim the table has no way to keep another off it.
func TestAcquireFailsWhenS3CannotBeReached(t *testing.T) {
	c := newFakeS3().client()
	c.failPuts.Store(true)
	if _, err := lease.Acquire(caller(t), c, uri, holder("host-a")); err == nil || errors.Is(err, lease.ErrLeaseHeld) {
		t.Fatalf("Acquire with S3 unreachable = %v, want a failure that is not a held table", err)
	}

	held := newFakeS3()
	held.plant(t, recordOf("host-a", time.Now()))
	c = held.client()
	c.failGets.Store(true)
	if _, err := lease.Acquire(caller(t), c, uri, holder("host-b")); err == nil || errors.Is(err, lease.ErrLeaseHeld) {
		t.Fatalf("Acquire unable to read a held lease = %v, want a failure that is not a held table", err)
	}
}

// TestAcquireRefusesALeaseItCannotRead verifies a lease object that is not a lease stops
// the restore rather than being waited out or overwritten. Either would be a guess about
// a restore this one cannot see.
func TestAcquireRefusesALeaseItCannotRead(t *testing.T) {
	store := newFakeS3()
	store.mu.Lock()
	store.data, store.etag = []byte("not a lease"), digest([]byte("not a lease"))
	store.mu.Unlock()

	if _, err := lease.Acquire(caller(t), store.client(), uri, holder("host-a")); err == nil || errors.Is(err, lease.ErrLeaseHeld) {
		t.Fatalf("Acquire over an unreadable lease = %v, want a failure", err)
	}
}

// TestReleaseReportsALeaseItCouldNotLetGo verifies a release that did not reach S3 says
// so, since the next restore of the table will then wait out the TTL rather than start
// at once, and an operator seeing that wait deserves to know why.
func TestReleaseReportsALeaseItCouldNotLetGo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		c := store.client()
		l := mustAcquire(t, c, "host-a")
		c.failPuts.Store(true)

		if err := l.Release(context.Background()); err == nil {
			t.Error("Release unable to reach S3 reported success")
		}
		if cause := context.Cause(l.Context()); cause == nil {
			t.Error("the lease context outlived its release")
		}
	})
}

// TestAcquireRejectsURIsThatNameNoObject verifies every way a lease URI can fail to name
// one object is caught before any request, since each would otherwise claim nothing, or
// the wrong thing, and let a second restore in.
func TestAcquireRejectsURIsThatNameNoObject(t *testing.T) {
	for _, bad := range []string{"https://bucket/key", "s3:///key", "s3://bucket", "::"} {
		if _, err := lease.Acquire(caller(t), newFakeS3().client(), bad, holder("host-a")); err == nil {
			t.Errorf("Acquire accepted %q", bad)
		}
	}
}

// TestInterruptingTheRestoreEndsTheLeaseContext verifies the context a restore runs under
// ends when the restore is interrupted, while the claim itself is kept until release. The
// restore stops on the context; if the interruption did not reach it, Ctrl-C would not
// stop a restore at all.
func TestInterruptingTheRestoreEndsTheLeaseContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, interrupt := context.WithCancel(caller(t))
		l, err := lease.Acquire(ctx, newFakeS3().client(), uri, holder("host-a"))
		if err != nil {
			t.Fatal(err)
		}
		defer release(t, l)
		interrupt()
		synctest.Wait()

		if !errors.Is(context.Cause(l.Context()), context.Canceled) {
			t.Errorf("lease context cause after an interrupt = %v, want context.Canceled", context.Cause(l.Context()))
		}
	})
}

// TestTakeoverLostToARenewalWaitsAgain verifies a challenger whose takeover is beaten by
// the holder renewing, just as the TTL ran out, does not claim the table: it goes back to
// watching, and claims it only once the new version has itself gone a TTL unrenewed.
func TestTakeoverLostToARenewalWaitsAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		store.plant(t, recordOf("slow", time.Now()))
		c := store.client()
		puts := 0
		c.beforePut = func() {
			// The challenger's first write creates, its second takes over: the holder
			// renews in between.
			if puts++; puts == 2 {
				store.data, store.etag = []byte(`{"owner":"slow","host":"slow","table":"orders","ttl":30000000000,"renewals":1}`), `"renewed"`
			}
		}
		start := time.Now()
		l, err := lease.Acquire(caller(t), c, uri, holder("host-b"))
		if err != nil {
			t.Fatal(err)
		}
		defer release(t, l)
		if waited := time.Since(start); waited < 2*ttl {
			t.Errorf("claimed after %s, before the renewed version had gone a TTL unrenewed", waited)
		}
	})
}

// TestRetriedRenewalCountsFromTheAttemptThatLanded verifies a renewal retried after a
// failure moves the deadline from the retry that succeeded. Counting from the failed
// attempt would stop a restore early for every passing fault.
func TestRetriedRenewalCountsFromTheAttemptThatLanded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		c := store.client()
		l := mustAcquire(t, c, "host-a")
		defer func() { c.failPuts.Store(false); release(t, l) }()

		c.failPuts.Store(true)
		time.Sleep(renewEvery)
		synctest.Wait()
		c.failPuts.Store(false)
		time.Sleep(2 * time.Second) // The retry, which lands.
		synctest.Wait()
		landed := time.Now()
		c.failPuts.Store(true)

		time.Sleep(fenceAfter - time.Since(landed) - time.Nanosecond)
		synctest.Wait()
		if err := l.Check(); err != nil {
			t.Fatalf("stopped before the deadline the retried renewal set: %v", err)
		}
	})
}

// TestTakeoverByALongRunningRestoreIsNotMistakenForOwnRenewal verifies a holder whose
// lease was taken over by a restore that has renewed many times stops, however far that
// restore's count is ahead of its own. Only the holder's own identity says a write it
// cannot account for was its own.
func TestTakeoverByALongRunningRestoreIsNotMistakenForOwnRenewal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		l := mustAcquire(t, store.client(), "host-a")
		defer release(t, l)

		veteran := recordOf("host-b", time.Now())
		veteran.Renewals = 1000
		store.overwrite(t, veteran)
		time.Sleep(renewEvery)
		synctest.Wait()
		if err := l.Check(); !errors.Is(err, lease.ErrLeaseLost) {
			t.Fatalf("Check after a takeover by a long-running restore = %v, want ErrLeaseLost", err)
		}
	})
}

// TestAcquireRidesOutARacingWrite verifies a claim S3 turns away because another write to
// the lease was in progress is tried again rather than failing the restore. S3 answers
// two conditional writes racing with a conflict rather than a failed condition, and it
// says nothing about who, if anyone, holds the table.
func TestAcquireRidesOutARacingWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFakeS3().client()
		c.conflicts.Store(1)
		l, err := lease.Acquire(caller(t), c, uri, holder("host-a"))
		if err != nil {
			t.Fatalf("Acquire after a racing write = %v", err)
		}
		release(t, l)
	})
}

// TestHungRenewalsDoNotDelayTheStop verifies a holder whose renewals hang stops at its
// deadline, not when the hung request finally gives up. The margin between the holder's
// stop and a challenger's takeover is for writes DynamoDB applies late; a stop that
// waited on S3 would spend it.
func TestHungRenewalsDoNotDelayTheStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFakeS3().client()
		l := mustAcquire(t, c, "host-a")
		c.hangPuts.Store(1 << 20)
		defer func() { c.hangPuts.Store(0); release(t, l) }()

		time.Sleep(fenceAfter)
		synctest.Wait()
		if !errors.Is(context.Cause(l.Context()), lease.ErrLeaseLost) {
			t.Fatalf("lease context at the deadline with renewals hanging: cause %v, want ErrLeaseLost", context.Cause(l.Context()))
		}
	})
}

// TestRenewalRacingAnotherWriteIsNotATakeover verifies a renewal S3 turned away because
// another write to the lease was in progress, while the lease is unchanged, is retried
// rather than taken for a takeover. Stopping would end a healthy restore, and name itself
// as the restore that took the table.
func TestRenewalRacingAnotherWriteIsNotATakeover(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFakeS3().client()
		l := mustAcquire(t, c, "host-a")
		defer release(t, l)

		c.conflicts.Store(1)
		time.Sleep(3 * ttl)
		synctest.Wait()
		if err := l.Check(); err != nil {
			t.Fatalf("a renewal that raced another write stopped the restore: %v", err)
		}
	})
}

// TestReleaseAfterAnUnseenRenewalStillLetsGo verifies a restore whose last renewal landed
// with its answer lost still releases the table on its way out. Leaving it claimed would
// make the next restore wait out the TTL for a restore that is already gone.
func TestReleaseAfterAnUnseenRenewalStillLetsGo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		c := store.client()
		l := mustAcquire(t, c, "host-a")

		c.landThenFail.Store(1)
		time.Sleep(renewEvery) // The renewal lands; its answer is lost.
		synctest.Wait()
		release(t, l)

		if rec := store.record(t); !rec.Released {
			t.Errorf("lease after release = %+v, want released", rec)
		}
	})
}

// TestHungClaimGivesUp verifies a claim whose request never answers fails the restore at
// the start rather than holding it there forever.
func TestHungClaimGivesUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFakeS3().client()
		c.hangPuts.Store(1 << 20)
		c.failGets.Store(true)
		if _, err := lease.Acquire(caller(t), c, uri, holder("host-a")); err == nil {
			t.Fatal("a claim that never answered succeeded")
		}
	})
}
