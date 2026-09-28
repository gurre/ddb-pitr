// Package lease claims a target table for one restore at a time, through a single
// object in S3 that the holder keeps renewing.
//
// Two restores writing one table interleave in ways neither can see: an older export
// applied alongside a newer one can put back data the newer one replaced. The lease
// stops the second restore before it touches the table, and a holder that cannot prove
// it still holds the lease stops writing before anyone else could take it over.
//
// No decision here compares clocks on two machines. A challenger decides a lease is
// abandoned by watching the object stay unchanged for the holder's TTL on its own
// monotonic clock, and the holder stops writing a margin before that TTL has passed
// since it last renewed, on its own monotonic clock. Only the rate of each clock
// matters, never where it is set.
package lease

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	json "github.com/goccy/go-json"
)

// ErrLeaseHeld is returned by Acquire when another restore is holding the table and
// renewed its claim while this one watched. Nothing has been written to the table.
var ErrLeaseHeld = errors.New("lease: another restore holds this table")

// ErrLeaseLost is the cause the lease's context is cancelled with when this restore can
// no longer show that it holds the table, either because another restore took it over
// or because renewals stopped succeeding for long enough that one could.
var ErrLeaseLost = errors.New("lease: this restore no longer holds the table")

const (
	// ttl is how long a holder promises to renew within. A challenger takes over a lease
	// only after watching it go this long without a renewal.
	ttl = 30 * time.Second
	// renewEvery gives a holder three chances to renew within one TTL.
	renewEvery = ttl / 3
	// retryEvery is how soon a failed renewal is tried again.
	retryEvery = 2 * time.Second
	// callTimeout bounds one request to S3, so a hung connection cannot use up the
	// margin the holder has left.
	callTimeout = 5 * time.Second
	// margin is how long before the TTL runs out the holder stops writing. It covers a
	// write DynamoDB applies after the client abandoned it, and the drift between the
	// two machines' clock rates.
	margin = 10 * time.Second
)

// objectStore is the part of S3 the lease needs: conditional reads and writes of one
// object.
type objectStore interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// Holder is who is claiming the table, written into the lease so a restore that finds
// it held can name who holds it.
// Fields are ordered largest-to-smallest for memory alignment.
type Holder struct {
	Host    string // Machine the restore runs on
	Table   string // Target table being restored
	Export  string // Export being restored from
	Version string // Build of the restore
	PID     int    // Process id on Host
}

// record is what the lease object holds. It records what happened; whether the lease
// is held is decided by whoever reads it, from how the object changes over time.
// Fields are ordered largest-to-smallest for memory alignment.
type record struct {
	Acquired time.Time     `json:"acquired"` // Holder's wall clock at acquisition, for operators only
	Renewed  time.Time     `json:"renewed"`  // Holder's wall clock at the last write, for operators only
	Owner    string        `json:"owner"`    // Random identity of the holding process
	Host     string        `json:"host"`
	Table    string        `json:"table"`
	Export   string        `json:"export"`
	Version  string        `json:"version"`
	TTL      time.Duration `json:"ttl"` // How long the holder promised to renew within
	// Renewals counts the holder's writes. It makes every body differ from the one
	// before: S3's ETag is a digest of the body, and a renewal that wrote the same bytes
	// would look to a watcher like no renewal at all.
	Renewals int64 `json:"renewals"`
	PID      int   `json:"pid"`
	Released bool  `json:"released"` // The holder let go deliberately, so nobody need wait
}

// Option adjusts optional Lease behaviour.
type Option func(*Lease)

// WithNotices sends what Acquire has to say while it waits on another holder, such as
// who holds the table and how long it will watch, to w. Left out, nothing is said.
// Example:
//
//	l, err := lease.Acquire(ctx, client, uri, holder, lease.WithNotices(os.Stderr))
func WithNotices(w io.Writer) Option {
	return func(l *Lease) {
		l.notices = w
	}
}

// Lease is a held claim on a table. While it is held, a heartbeat renews it; the
// context from Context is cancelled with ErrLeaseLost the moment the claim can no
// longer be shown to be this restore's.
// Fields are ordered largest-to-smallest for memory alignment.
type Lease struct {
	deadline time.Time // After this the holder must not write; guarded by mu
	rec      record    // What was last written; owned by the heartbeat until it stops
	ctx      context.Context
	client   objectStore
	notices  io.Writer
	cancel   context.CancelCauseFunc
	fence    *time.Timer // Cancels the context at the deadline; moved under mu
	stop     chan struct{}
	done     chan struct{}
	bucket   string
	key      string
	etag     string // ETag of the last write; owned by the heartbeat until it stops
	stopOnce sync.Once
	mu       sync.Mutex
}

// Acquire claims the table for this restore, waiting up to one TTL to learn whether a
// lease already there is still being renewed. It returns ErrLeaseHeld when it is. The
// returned lease is renewed in the background until Release; its Context is derived
// from ctx and is what the restore must run under. Example:
//
//	l, err := lease.Acquire(ctx, client, "s3://bucket/ddb-pitr/leases/eu-west-1.orders.json", lease.Holder{
//	    Host: host, Table: "orders", Export: exportURI, Version: version, PID: os.Getpid(),
//	})
//	if err != nil {
//	    return err
//	}
//	defer func() { _ = l.Release(context.Background()) }()
//	err = restore(l.Context())
func Acquire(ctx context.Context, client objectStore, uri string, h Holder, opts ...Option) (*Lease, error) {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "s3" || u.Host == "" || strings.Trim(u.Path, "/") == "" {
		return nil, fmt.Errorf("lease URI must be s3://bucket/key: %q", uri)
	}
	owner, err := newOwner()
	if err != nil {
		return nil, err
	}
	l := &Lease{
		client:  client,
		notices: io.Discard,
		bucket:  u.Host,
		key:     strings.TrimPrefix(u.Path, "/"),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	for _, opt := range opts {
		opt(l)
	}
	l.rec = record{
		Owner:   owner,
		Host:    h.Host,
		Table:   h.Table,
		Export:  h.Export,
		Version: h.Version,
		TTL:     ttl,
		PID:     h.PID,
	}

	for {
		// The deadline counts from when a write was sent, not when it was answered: the
		// write may have taken effect the moment it arrived.
		sent := time.Now()
		l.rec.Acquired, l.rec.Renewed = time.Now(), time.Now()
		etag, err := l.put(ctx, l.rec, "")
		if err == nil {
			return l.start(ctx, etag, sent), nil
		}
		if !isPreconditionFailure(err) {
			// The claim may have landed with only its answer lost; one that did is ours,
			// and walking away from it would make the next restore wait out the TTL.
			if cur, curETag, found, readErr := l.read(ctx); readErr == nil && found && l.owns(cur) {
				l.rec = cur
				return l.start(ctx, curETag, sent), nil
			}
			return nil, fmt.Errorf("failed to claim table %s at s3://%s/%s: %w", h.Table, l.bucket, l.key, err)
		}

		held, heldETag, found, err := l.read(ctx)
		if err != nil {
			return nil, err
		}
		if !found {
			// Released and removed between the two requests; claim it afresh.
			continue
		}
		if !held.Released {
			if watchErr := l.watch(ctx, held, heldETag); watchErr != nil {
				if errors.Is(watchErr, errChanged) {
					continue
				}
				return nil, watchErr
			}
		}

		sent = time.Now()
		l.rec.Acquired, l.rec.Renewed = time.Now(), time.Now()
		etag, err = l.put(ctx, l.rec, heldETag)
		if err == nil {
			return l.start(ctx, etag, sent), nil
		}
		if !isPreconditionFailure(err) {
			return nil, fmt.Errorf("failed to take over table %s at s3://%s/%s: %w", h.Table, l.bucket, l.key, err)
		}
		// Another restore got there first. Going round again finds it renewing.
	}
}

// errChanged is watch's report that the lease was released or removed while it
// watched, so the claim is worth trying again from the start.
var errChanged = errors.New("lease changed while watching")

// watch waits for the holder's TTL, measured from when the held version was first
// seen, and returns nil when that version outlived it unchanged. A renewal in the
// meantime means the holder is alive and is ErrLeaseHeld.
func (l *Lease) watch(ctx context.Context, held record, heldETag string) error {
	// The clock starts when the version was seen, which is after it was written, so the
	// holder's own count towards its deadline started earlier than this one.
	seen := time.Now()
	// A record that promises less than this build's TTL, or nothing at all, is not
	// trusted to have meant it.
	wait := max(held.TTL, ttl)
	_, _ = fmt.Fprintf(l.notices, "table %s is claimed by %s pid %d (since %s by its clock, renewed %d times); waiting up to %s to see whether it is still running\n",
		held.Table, held.Host, held.PID, held.Acquired.Format(time.TimeOnly), held.Renewals, wait)

	for {
		remaining := wait - time.Since(seen)
		if remaining <= 0 {
			return nil
		}
		if !sleep(ctx, min(remaining, renewEvery)) {
			return fmt.Errorf("stopped waiting for the claim on table %s: %w", held.Table, ctx.Err())
		}
		cur, curETag, found, err := l.read(ctx)
		if err != nil {
			return err
		}
		if !found || cur.Released {
			return errChanged
		}
		if curETag != heldETag {
			return fmt.Errorf("%w: %s pid %d is restoring %s from %s (running since %s by its clock) and renewed its claim while this restore watched. Stop it or wait for it to finish; if it has died, running again takes over once it has gone %s without renewing",
				ErrLeaseHeld, cur.Host, cur.PID, cur.Table, cur.Export, cur.Acquired.Format(time.TimeOnly), wait)
		}
	}
}

// start begins renewing a lease just written, with its deadline counted from when that
// write was sent.
func (l *Lease) start(parent context.Context, etag string, sent time.Time) *Lease {
	l.etag = etag
	l.deadline = sent.Add(ttl - margin)
	l.ctx, l.cancel = context.WithCancelCause(parent)
	// The fence runs on a timer of its own rather than on the heartbeat's, so a renewal
	// that is slow to answer cannot hold it past the deadline.
	l.fence = time.AfterFunc(time.Until(l.deadline), func() {
		l.cancel(fmt.Errorf("%w: not renewed within %s", ErrLeaseLost, ttl-margin))
	})
	// Renewal outlives the parent: an interrupted restore still has its final checkpoint
	// to save, and it must still hold the table while it does.
	go l.heartbeat(context.WithoutCancel(parent), sent)
	return l
}

// Context is what the restore must run under. It is cancelled with ErrLeaseLost as
// its cause once this restore can no longer show it holds the table, and follows the
// context Acquire was given otherwise.
func (l *Lease) Context() context.Context {
	return l.ctx
}

// Check reports ErrLeaseLost once the lease can no longer be relied on. It reads the
// clock itself rather than waiting on the fence, so a process that was paused past its
// deadline is stopped by its next write rather than by a timer yet to run.
// Example:
//
//	if err := l.Check(); err != nil {
//	    return err // do not write
//	}
func (l *Lease) Check() error {
	l.mu.Lock()
	deadline := l.deadline
	l.mu.Unlock()
	if !time.Now().Before(deadline) {
		return fmt.Errorf("%w: not renewed within %s", ErrLeaseLost, ttl-margin)
	}
	if cause := context.Cause(l.ctx); errors.Is(cause, ErrLeaseLost) {
		return cause
	}
	return nil
}

// lost reports whether the lease has been given up for lost. It is final: a renewal
// that lands afterwards does not bring it back, since the restore has already stopped.
func (l *Lease) lost() bool {
	return errors.Is(context.Cause(l.ctx), ErrLeaseLost)
}

// heartbeat renews the lease until Release or until it is lost, starting a renewal
// interval after the claim was sent.
func (l *Lease) heartbeat(ctx context.Context, claimed time.Time) {
	defer close(l.done)

	next := time.NewTimer(time.Until(claimed.Add(renewEvery)))
	defer next.Stop()
	// firstSent is when the first attempt at the current renewal was sent. An attempt
	// that failed on this side may still have landed, and if a later attempt finds it
	// did, the deadline has to count from the earliest send, not the latest.
	var firstSent time.Time

	for {
		select {
		case <-l.stop:
			return
		case <-next.C:
		}
		if l.lost() {
			return
		}
		sent := time.Now()
		if firstSent.IsZero() {
			firstSent = sent
		}
		adopted, err := l.renew(ctx)
		switch {
		case err == nil:
			if adopted {
				sent = firstSent
			}
			firstSent = time.Time{}
			l.extend(sent.Add(ttl - margin))
			next.Reset(renewEvery)
		case errors.Is(err, ErrLeaseLost):
			l.cancel(err)
			return
		default:
			// A failed request says nothing about who holds the table. The fence keeps
			// running, so a failure that lasts stops the restore on its own.
			next.Reset(retryEvery)
		}
	}
}

// extend moves the deadline and the fence to it, unless the lease was lost meanwhile.
func (l *Lease) extend(deadline time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost() || !l.fence.Stop() {
		return
	}
	l.deadline = deadline
	l.fence.Reset(time.Until(deadline))
}

// renew writes the next version of the record over the one this restore last wrote.
// adopted reports that the write was refused because an earlier attempt, reported as
// failed, had in fact landed. Its requests end at the deadline, so a renewal cannot
// still be in flight when the restore has had to stop.
func (l *Lease) renew(ctx context.Context) (adopted bool, err error) {
	l.mu.Lock()
	deadline := l.deadline
	l.mu.Unlock()
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	next := l.rec
	next.Renewals++
	next.Renewed = time.Now()
	etag, err := l.put(ctx, next, l.etag)
	if err == nil {
		l.rec, l.etag = next, etag
		return false, nil
	}
	if !isPreconditionFailure(err) {
		return false, err
	}

	cur, curETag, found, err := l.read(ctx)
	switch {
	case err != nil:
		return false, err
	case !found:
		return false, fmt.Errorf("%w: the lease at s3://%s/%s was removed", ErrLeaseLost, l.bucket, l.key)
	case curETag == l.etag:
		// Nothing changed: S3 turned the write away because another write to the
		// object was in progress. That says nothing about who holds it; try again.
		return false, fmt.Errorf("renewal raced another write to the lease: %w", err)
	case l.owns(cur) && cur.Renewals >= next.Renewals:
		l.rec, l.etag = cur, curETag
		return true, nil
	}
	return false, fmt.Errorf("%w: taken over by %s pid %d", ErrLeaseLost, cur.Host, cur.PID)
}

// owns reports whether a record read back is this restore's own, still held.
func (l *Lease) owns(cur record) bool {
	return cur.Owner == l.rec.Owner && !cur.Released
}

// Release stops renewing and marks the lease released, so the next restore of the table
// can start without waiting out the TTL. A lease another restore has since taken over is
// left alone. A release that fails leaves a lease that lapses within the TTL on its own.
// The lease's context is cancelled either way. Example:
//
//	defer func() {
//	    if err := l.Release(context.Background()); err != nil {
//	        fmt.Fprintln(os.Stderr, err)
//	    }
//	}()
func (l *Lease) Release(ctx context.Context) error {
	l.stopOnce.Do(func() { close(l.stop) })
	<-l.done
	l.fence.Stop()
	defer l.cancel(context.Canceled)

	ctx = context.WithoutCancel(ctx)
	etag := l.etag
	// Twice at most: a renewal that landed with its answer lost leaves this restore
	// holding a version it has not seen, and the lease is still its own to let go.
	for range 2 {
		next := l.rec
		next.Renewals++
		next.Renewed = time.Now()
		next.Released = true
		_, err := l.put(ctx, next, etag)
		if err == nil {
			return nil
		}
		if !isPreconditionFailure(err) {
			return fmt.Errorf("failed to release table %s; it will be claimable within %s: %w", l.rec.Table, ttl, err)
		}
		cur, curETag, found, readErr := l.read(ctx)
		if readErr != nil {
			return fmt.Errorf("failed to release table %s; it will be claimable within %s: %w", l.rec.Table, ttl, readErr)
		}
		if !found || !l.owns(cur) {
			// Another restore holds it now, or it is already let go.
			return nil
		}
		l.rec, etag = cur, curETag
	}
	return fmt.Errorf("failed to release table %s; it will be claimable within %s: the lease kept changing", l.rec.Table, ttl)
}

// put writes rec, over the version with the given ETag, or only where there is no
// object at all when etag is empty. It returns the ETag of what it wrote.
func (l *Lease) put(ctx context.Context, rec record, etag string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	data, err := json.Marshal(rec)
	if err != nil {
		return "", fmt.Errorf("failed to encode lease: %w", err)
	}
	input := &s3.PutObjectInput{
		Bucket: &l.bucket,
		Key:    &l.key,
		Body:   bytes.NewReader(data),
	}
	if etag == "" {
		input.IfNoneMatch = awssdk.String("*")
	} else {
		input.IfMatch = awssdk.String(etag)
	}
	out, err := l.client.PutObject(ctx, input)
	if err != nil {
		return "", err
	}
	// Without the ETag the next renewal could not be made conditional on this write.
	if out.ETag == nil {
		return "", fmt.Errorf("lease at s3://%s/%s was written without an ETag, so it cannot be renewed safely", l.bucket, l.key)
	}
	return *out.ETag, nil
}

// read returns the lease as it stands and its ETag, and whether there is one at all.
func (l *Lease) read(ctx context.Context) (rec record, etag string, found bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	out, err := l.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &l.bucket, Key: &l.key})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		var notFound *types.NotFound
		if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
			return record{}, "", false, nil
		}
		return record{}, "", false, fmt.Errorf("failed to read lease at s3://%s/%s: %w", l.bucket, l.key, err)
	}
	defer func() { _ = out.Body.Close() }()
	if out.ETag == nil {
		return record{}, "", false, fmt.Errorf("lease at s3://%s/%s came back without an ETag", l.bucket, l.key)
	}
	// A lease that cannot be read cannot be judged abandoned; waiting it out or
	// overwriting it would both be guesses about someone else's restore.
	if err := json.NewDecoder(out.Body).Decode(&rec); err != nil {
		return record{}, "", false, fmt.Errorf("lease at s3://%s/%s is not one this restore can read: %w", l.bucket, l.key, err)
	}
	return rec, *out.ETag, true, nil
}

// newOwner returns a random identity for this process's claims.
func newOwner() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("failed to draw a lease owner: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// sleep waits for d, reporting false if the context ends first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// isPreconditionFailure reports whether S3 refused a conditional write. S3 answers a
// failed IfMatch or IfNoneMatch with 412 PreconditionFailed, and two conditional writes
// racing with 409 ConditionalRequestConflict; neither has a typed error in the SDK.
func isPreconditionFailure(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "PreconditionFailed", "ConditionalRequestConflict":
		return true
	}
	return false
}
