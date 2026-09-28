package lease_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	json "github.com/goccy/go-json"
	"github.com/gurre/ddb-pitr/lease"
)

const (
	uri = "s3://bucket/ddb-pitr/leases/eu-west-1.orders.json"
	// The package's timings, restated so the tests can place themselves either side of
	// each boundary. A change to the package's values fails these tests on purpose.
	ttl        = 30 * time.Second
	renewEvery = 10 * time.Second
	fenceAfter = 20 * time.Second
)

// TestAcquireClaimsAnUnheldTable pins the ordinary case: a table nobody holds is claimed
// at once, and the lease names who holds it so a second restore can say so.
func TestAcquireClaimsAnUnheldTable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		l := mustAcquire(t, store.client(), "host-a")
		defer release(t, l)

		if rec := store.record(t); rec.Host != "host-a" || rec.Released {
			t.Fatalf("lease record = %+v, want held by host-a", rec)
		}
	})
}

// TestAcquireRefusesWhileTheHolderRenews is the guarantee itself: a second restore of a
// table being restored stops without touching it, and the first carries on.
func TestAcquireRefusesWhileTheHolderRenews(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		first := mustAcquire(t, store.client(), "host-a")
		defer release(t, first)

		_, err := lease.Acquire(caller(t), store.client(), uri, holder("host-b"))
		if !errors.Is(err, lease.ErrLeaseHeld) {
			t.Fatalf("second Acquire error = %v, want ErrLeaseHeld", err)
		}
		if err := first.Check(); err != nil {
			t.Fatalf("first holder was disturbed by the refused claim: %v", err)
		}
	})
}

// TestAcquireWaitsOutTheTTLBeforeTakingOver pins the takeover boundary. A lease left by
// a crashed restore is taken over once it has gone a full TTL unchanged, and not a
// moment before, however old the dead holder's own clock says it is: clocks on two
// machines are never compared.
func TestAcquireWaitsOutTheTTLBeforeTakingOver(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		store.plant(t, recordOf("crashed", time.Now().Add(-time.Hour)))
		start := time.Now()

		var claimed atomic.Pointer[lease.Lease]
		go func() {
			l, err := lease.Acquire(caller(t), store.client(), uri, holder("host-b"))
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			claimed.Store(l)
		}()

		time.Sleep(ttl - time.Nanosecond)
		synctest.Wait()
		if claimed.Load() != nil || store.record(t).Host != "crashed" {
			t.Fatalf("took over after %s, before the TTL had passed", time.Since(start))
		}

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		l := claimed.Load()
		if l == nil || store.record(t).Host != "host-b" {
			t.Fatalf("not taken over after a full TTL unrenewed")
		}
		release(t, l)
	})
}

// TestAcquireTakesAReleasedLeaseWithoutWaiting pins what Release is for: the next restore
// of a table whose last restore let go starts at once rather than waiting out the TTL.
func TestAcquireTakesAReleasedLeaseWithoutWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		release(t, mustAcquire(t, store.client(), "host-a"))

		start := time.Now()
		l := mustAcquire(t, store.client(), "host-b")
		defer release(t, l)
		if waited := time.Since(start); waited != 0 {
			t.Fatalf("waited %s for a released lease", waited)
		}
	})
}

// TestTakeoverStopsTheFormerHolder pins that a holder whose lease was taken over stops
// at its next renewal, with a cause that says why, rather than writing on alongside
// the new holder.
func TestTakeoverStopsTheFormerHolder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		l := mustAcquire(t, store.client(), "host-a")
		defer release(t, l)

		store.overwrite(t, recordOf("host-b", time.Now()))
		time.Sleep(renewEvery)
		synctest.Wait()

		if cause := context.Cause(l.Context()); !errors.Is(cause, lease.ErrLeaseLost) {
			t.Fatalf("lease context cause = %v, want ErrLeaseLost", cause)
		}
	})
}

// TestHolderStopsWhenRenewalsKeepFailing pins the half of the guarantee that needs no
// answer from S3: a holder cut off from it stops writing, measured from the last write
// it sent, before a challenger could have waited out the TTL.
func TestHolderStopsWhenRenewalsKeepFailing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		c := store.client()
		l := mustAcquire(t, c, "host-a")
		defer func() { c.failPuts.Store(false); release(t, l) }()
		c.failPuts.Store(true)

		time.Sleep(fenceAfter - time.Nanosecond)
		synctest.Wait()
		if err := l.Check(); err != nil {
			t.Fatalf("stopped before the deadline: %v", err)
		}

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if cause := context.Cause(l.Context()); !errors.Is(cause, lease.ErrLeaseLost) {
			t.Fatalf("lease context cause = %v at the deadline, want ErrLeaseLost", cause)
		}
		if err := l.Check(); !errors.Is(err, lease.ErrLeaseLost) {
			t.Fatalf("Check at the deadline = %v, want ErrLeaseLost", err)
		}
	})
}

// TestTransientRenewalFailureIsSurvived pins that one failed request does not stop a
// restore: the renewal is retried well within the margin and the deadline moves on.
func TestTransientRenewalFailureIsSurvived(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		c := store.client()
		l := mustAcquire(t, c, "host-a")
		defer release(t, l)

		c.failPuts.Store(true)
		time.Sleep(renewEvery)
		synctest.Wait()
		c.failPuts.Store(false)

		time.Sleep(3 * ttl)
		synctest.Wait()
		if err := l.Check(); err != nil {
			t.Fatalf("one failed renewal stopped the restore: %v", err)
		}
	})
}

// TestRenewalThatLandedDespiteAnErrorIsAdopted pins the ambiguous case: a renewal S3
// applied but whose answer was lost looks, on the next attempt, like someone else
// writing. Reading it back as this holder's own keeps the restore running.
func TestRenewalThatLandedDespiteAnErrorIsAdopted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		c := store.client()
		l := mustAcquire(t, c, "host-a")
		defer release(t, l)

		c.landThenFail.Store(1)
		time.Sleep(3 * ttl)
		synctest.Wait()
		if err := l.Check(); err != nil {
			t.Fatalf("a renewal that landed was taken for a takeover: %v", err)
		}
	})
}

// TestHolderStopsBeforeAChallengerTakesOver is the rule that makes the lease exclusive:
// when the holder is cut off from S3 and a challenger waits it out, the holder has
// stopped before the challenger holds the table.
func TestHolderStopsBeforeAChallengerTakesOver(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		cut := store.client()
		first := mustAcquire(t, cut, "host-a")
		defer func() { cut.failPuts.Store(false); cut.failGets.Store(false); release(t, first) }()
		cut.failPuts.Store(true)
		cut.failGets.Store(true)

		var stopped, claimed time.Time
		go func() {
			<-first.Context().Done()
			stopped = time.Now()
		}()
		second := mustAcquire(t, store.client(), "host-b")
		claimed = time.Now()
		defer release(t, second)
		synctest.Wait()

		if stopped.IsZero() || !stopped.Before(claimed) {
			t.Fatalf("holder stopped at %v, challenger claimed at %v; the two overlapped", stopped, claimed)
		}
	})
}

// TestInterruptedRestoreKeepsTheLeaseUntilReleased pins that an interrupted restore
// still holds the table while it saves where it stopped: renewal outlives the signal,
// and the lease context reports the interruption rather than a lost lease.
func TestInterruptedRestoreKeepsTheLeaseUntilReleased(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		ctx, interrupt := context.WithCancel(caller(t))
		l, err := lease.Acquire(ctx, store.client(), uri, holder("host-a"))
		if err != nil {
			t.Fatal(err)
		}
		interrupt()
		time.Sleep(3 * ttl)
		synctest.Wait()

		if cause := context.Cause(l.Context()); errors.Is(cause, lease.ErrLeaseLost) {
			t.Fatalf("interruption reported as a lost lease: %v", cause)
		}
		if err := l.Check(); err != nil {
			t.Fatalf("lease lapsed while the interrupted restore was still saving: %v", err)
		}
		release(t, l)
	})
}

// TestReleaseLeavesATakenOverLeaseAlone pins that a holder that lost its lease does not
// mark the new holder's lease released on its way out.
func TestReleaseLeavesATakenOverLeaseAlone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeS3()
		l := mustAcquire(t, store.client(), "host-a")
		store.overwrite(t, recordOf("host-b", time.Now()))

		release(t, l)
		if rec := store.record(t); rec.Host != "host-b" || rec.Released {
			t.Fatalf("lease after the former holder released = %+v, want host-b still holding", rec)
		}
	})
}

// TestAcquireRejectsAURIWithoutAKey pins that a lease addressed to a bucket alone is a
// wiring mistake caught before any request.
func TestAcquireRejectsAURIWithoutAKey(t *testing.T) {
	if _, err := lease.Acquire(caller(t), newFakeS3().client(), "s3://bucket/", holder("host-a")); err == nil {
		t.Fatal("Acquire accepted a URI naming no key")
	}
}

// mustAcquire claims the table for host through c, failing the test if it cannot.
func mustAcquire(t *testing.T, c *fakeClient, host string) *lease.Lease {
	t.Helper()
	l, err := lease.Acquire(caller(t), c, uri, holder(host))
	if err != nil {
		t.Fatalf("Acquire(%s): %v", host, err)
	}
	return l
}

// release lets the lease go, failing the test if that does not reach the store.
func release(t *testing.T, l *lease.Lease) {
	t.Helper()
	if err := l.Release(caller(t)); err != nil {
		t.Errorf("Release: %v", err)
	}
}

// holder is a restore of the test table from the test export, running on host.
func holder(host string) lease.Holder {
	return lease.Holder{Host: host, Table: "orders", Export: "s3://exports/e1", Version: "test", PID: 42}
}

// storedRecord mirrors the lease object's fields the tests look at.
type storedRecord struct {
	Renewed  time.Time     `json:"renewed"`
	Acquired time.Time     `json:"acquired"`
	Owner    string        `json:"owner"`
	Host     string        `json:"host"`
	Table    string        `json:"table"`
	TTL      time.Duration `json:"ttl"`
	Renewals int64         `json:"renewals"`
	Released bool          `json:"released"`
}

// recordOf is a lease record held by host, as written at the given time by its clock.
func recordOf(host string, at time.Time) storedRecord {
	return storedRecord{Owner: host, Host: host, Table: "orders", TTL: ttl, Acquired: at, Renewed: at}
}

// fakeS3 holds one object and answers conditional writes the way S3 does, with the
// ETag a digest of the body so a write of unchanged bytes is indistinguishable from no
// write, as it is on S3.
type fakeS3 struct {
	data []byte
	etag string
	mu   sync.Mutex
}

func newFakeS3() *fakeS3 { return &fakeS3{} }

// client is one process's view of the store, whose connection can fail on its own.
func (s *fakeS3) client() *fakeClient { return &fakeClient{store: s} }

func (s *fakeS3) plant(t *testing.T, rec storedRecord) {
	t.Helper()
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data, s.etag = data, digest(data)
}

// overwrite stands in for another restore taking the lease over.
func (s *fakeS3) overwrite(t *testing.T, rec storedRecord) { s.plant(t, rec) }

func (s *fakeS3) record(t *testing.T) storedRecord {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var rec storedRecord
	if err := json.Unmarshal(s.data, &rec); err != nil {
		t.Fatalf("lease object unreadable: %v", err)
	}
	return rec
}

type fakeClient struct {
	store        *fakeS3
	landThenFail atomic.Int32 // Writes to apply and then report as failed
	hangPuts     atomic.Int32 // Writes to hold until their context ends
	conflicts    atomic.Int32 // Writes to refuse as racing another write
	beforePut    func()       // Runs before each write is applied, for a test to race it
	failPuts     atomic.Bool
	failGets     atomic.Bool
}

// check stands in for S3 refusing a request addressed anywhere but the lease object, and
// fails one made without a deadline: a request the lease cannot bound could outlast the
// margin it has before it must stop writing.
func (c *fakeClient) check(ctx context.Context, bucket, key *string) error {
	if bucket == nil || *bucket != "bucket" || key == nil || *key != "ddb-pitr/leases/eu-west-1.orders.json" {
		return errors.New("request addressed to the wrong object")
	}
	if _, ok := ctx.Deadline(); !ok && ctx.Value(callerKey{}) == nil {
		return errors.New("request made outside the caller's context")
	}
	return nil
}

// callerKey marks the context a test hands to Acquire, so the fake can tell a request
// made under it from one made under a context the lease substituted.
type callerKey struct{}

var errNetwork = errors.New("connection reset")

// GetObject returns the lease object and its ETag, or NoSuchKey when there is none.
func (c *fakeClient) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if err := c.check(ctx, params.Bucket, params.Key); err != nil {
		return nil, err
	}
	if c.failGets.Load() {
		return nil, errNetwork
	}
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		return nil, &types.NoSuchKey{}
	}
	etag := s.etag
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(s.data)), ETag: &etag}, nil
}

// PutObject writes the lease object if its conditions hold, the way S3 decides them.
func (c *fakeClient) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if err := c.check(ctx, params.Bucket, params.Key); err != nil {
		return nil, err
	}
	if c.conflicts.Load() > 0 {
		c.conflicts.Add(-1)
		return nil, &smithy.GenericAPIError{Code: "ConditionalRequestConflict"}
	}
	if c.beforePut != nil {
		c.beforePut()
	}
	if c.hangPuts.Load() > 0 {
		c.hangPuts.Add(-1)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if c.failPuts.Load() {
		return nil, errNetwork
	}
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if params.IfNoneMatch != nil && s.data != nil {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed"}
	}
	if params.IfMatch != nil && (s.data == nil || *params.IfMatch != s.etag) {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed"}
	}
	s.data, s.etag = data, digest(data)
	if c.landThenFail.Load() > 0 {
		c.landThenFail.Add(-1)
		return nil, errNetwork
	}
	etag := s.etag
	return &s3.PutObjectOutput{ETag: &etag}, nil
}

func digest(data []byte) string {
	sum := md5.Sum(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// caller is the context a test hands the lease, marked so the fake can tell it apart.
func caller(t *testing.T) context.Context {
	return context.WithValue(t.Context(), callerKey{}, true)
}
