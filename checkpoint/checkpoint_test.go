package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	json "github.com/goccy/go-json"
)

// testState is the progress a partly-finished restore would have recorded: one file
// done and another part-way through.
func testState() State {
	return State{
		ExportID:  "arn:aws:dynamodb:eu-north-1:123456789012:table/orders/export/01768385930622-efd1a093",
		Completed: []string{"data-001.json.gz"},
		Offsets:   map[string]int64{"data-002.json.gz": 1024},
	}
}

// assertState fails unless the loaded state carries the same progress that was saved.
func assertState(t *testing.T, got, want State) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("state = %+v, want %+v", got, want)
	}
}

// TestMemoryStore_SaveLoad verifies progress written to the in-memory store comes back
// intact, including which files finished and how far the unfinished ones got.
func TestMemoryStore_SaveLoad(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	state := testState()
	if err := store.Save(ctx, state); err != nil {
		t.Fatalf("failed to save state: %v", err)
	}

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	assertState(t, loaded, state)
}

// TestMemoryStore_EmptyState verifies a store nothing has been saved to reads as no
// progress, so the first run of a restore starts from the top rather than failing.
func TestMemoryStore_EmptyState(t *testing.T) {
	store := NewMemoryStore()

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("failed to load empty state: %v", err)
	}

	if state.ExportID != "" || len(state.Completed) != 0 || len(state.Offsets) != 0 {
		t.Errorf("expected no recorded progress, got %+v", state)
	}
}

// TestMemoryStore_ConcurrentAccess verifies the in-memory store is safe to read and
// write from several workers at once. The restore checkpoints from whichever worker
// reaches an interval first, so unsynchronised access here is a live data race.
func TestMemoryStore_ConcurrentAccess(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			_ = store.Save(ctx, State{Completed: []string{fmt.Sprintf("data-%03d.json.gz", n)}})
		}(i)
		go func() {
			defer wg.Done()
			if _, err := store.Load(ctx); err != nil {
				t.Errorf("concurrent load failed: %v", err)
			}
		}()
	}
	wg.Wait()
}

func TestS3Store_NewValidURI(t *testing.T) {
	// We can only test URI parsing without a real S3 client
	store, err := NewS3Store(nil, "s3://my-bucket/path/to/checkpoint.json")
	if err != nil {
		t.Fatalf("failed to create S3 store: %v", err)
	}

	if store.bucket != "my-bucket" {
		t.Errorf("bucket mismatch: got %s, want my-bucket", store.bucket)
	}
	if store.key != "path/to/checkpoint.json" {
		t.Errorf("key mismatch: got %s, want path/to/checkpoint.json", store.key)
	}
}

func TestS3Store_InvalidURI(t *testing.T) {
	testCases := []string{
		"http://bucket/key",
		"https://bucket/key",
		"file:///path/to/file",
		"bucket/key",
	}

	for _, uri := range testCases {
		t.Run(uri, func(t *testing.T) {
			_, err := NewS3Store(nil, uri)
			if err == nil {
				t.Errorf("expected error for invalid S3 URI: %s", uri)
			}
		})
	}
}

// TestMemoryStore_Overwrite verifies a later save replaces the earlier one wholesale.
// The coordinator always saves a complete snapshot of its progress, so a store that
// merged instead of replacing would keep resurrecting stale offsets.
func TestMemoryStore_Overwrite(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	first := State{ExportID: "first", Completed: []string{"file1"}, Offsets: map[string]int64{"file2": 100}}
	if err := store.Save(ctx, first); err != nil {
		t.Fatalf("failed to save first state: %v", err)
	}

	second := State{ExportID: "second", Completed: []string{"file2"}, Offsets: map[string]int64{"file3": 200}}
	if err := store.Save(ctx, second); err != nil {
		t.Fatalf("failed to save second state: %v", err)
	}

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	assertState(t, loaded, second)
}

// TestS3Store_RoundTrip verifies state written to S3 comes back intact, which is what
// makes a restore resumable across process restarts.
func TestS3Store_RoundTrip(t *testing.T) {
	client := &stubS3Client{objects: map[string][]byte{}}
	store, err := NewS3Store(client, "s3://my-bucket/checkpoints/restore-001.json")
	if err != nil {
		t.Fatalf("failed to create S3 store: %v", err)
	}

	want := testState()
	if err := store.Save(context.Background(), want); err != nil {
		t.Fatalf("failed to save state: %v", err)
	}

	got, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	assertState(t, got, want)
}

// TestS3Store_MissingObjectStartsFresh verifies a checkpoint that does not exist yet reads
// as empty state rather than an error, so the first run of a restore is not a failure.
func TestS3Store_MissingObjectStartsFresh(t *testing.T) {
	store, err := NewS3Store(&stubS3Client{objects: map[string][]byte{}}, "s3://my-bucket/checkpoint.json")
	if err != nil {
		t.Fatalf("failed to create S3 store: %v", err)
	}

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("expected a missing checkpoint to read as empty state, got %v", err)
	}
	if state.ExportID != "" || len(state.Completed) != 0 || len(state.Offsets) != 0 {
		t.Errorf("expected no recorded progress, got %+v", state)
	}
}

// TestS3Store_ReportsReadFailure verifies a checkpoint that exists but cannot be read is
// surfaced. Treating it as empty state would silently restart a restore from the top.
func TestS3Store_ReportsReadFailure(t *testing.T) {
	store, err := NewS3Store(&stubS3Client{getErr: errors.New("access denied")}, "s3://my-bucket/checkpoint.json")
	if err != nil {
		t.Fatalf("failed to create S3 store: %v", err)
	}

	if _, err := store.Load(context.Background()); err == nil {
		t.Error("expected an unreadable checkpoint to be reported")
	}
}

// TestS3Store_ReportsUnreadableContent verifies a checkpoint whose contents are not valid
// state is reported rather than silently treated as no progress.
func TestS3Store_ReportsUnreadableContent(t *testing.T) {
	client := &stubS3Client{objects: map[string][]byte{"checkpoint.json": []byte("not json")}}
	store, err := NewS3Store(client, "s3://my-bucket/checkpoint.json")
	if err != nil {
		t.Fatalf("failed to create S3 store: %v", err)
	}

	if _, err := store.Load(context.Background()); err == nil {
		t.Error("expected a corrupt checkpoint to be reported")
	}
}

// TestS3Store_ReportsWriteFailure verifies a checkpoint that cannot be written is reported,
// since progress the operator believes is saved but is not makes a resume lose work.
func TestS3Store_ReportsWriteFailure(t *testing.T) {
	store, err := NewS3Store(&stubS3Client{putErr: errors.New("access denied")}, "s3://my-bucket/checkpoint.json")
	if err != nil {
		t.Fatalf("failed to create S3 store: %v", err)
	}

	if err := store.Save(context.Background(), testState()); err == nil {
		t.Error("expected a failed checkpoint write to be reported")
	}
}

// TestS3Store_SendsTheCallersContext verifies checkpoint reads and writes are made under
// the context the caller passed. Detaching from it would leave a shutting-down restore
// blocked on S3 with no deadline and no cancellation.
func TestS3Store_SendsTheCallersContext(t *testing.T) {
	client := &stubS3Client{objects: map[string][]byte{}}
	store, err := NewS3Store(client, "s3://my-bucket/checkpoint.json")
	if err != nil {
		t.Fatalf("failed to create S3 store: %v", err)
	}

	ctx := context.WithValue(context.Background(), callerContextKey{}, true)
	if err := store.Save(ctx, testState()); err != nil {
		t.Fatalf("failed to save state: %v", err)
	}
	if _, err := store.Load(ctx); err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if client.detached > 0 {
		t.Errorf("%d S3 calls were made outside the caller's context", client.detached)
	}
}

// TestS3Store_ReadsTheBodyBeforeClosingIt verifies the checkpoint is decoded while its
// response body is still open. Closing it first loses the recorded progress, which the
// restore would then read as a fresh start and rewrite the whole export.
func TestS3Store_ReadsTheBodyBeforeClosingIt(t *testing.T) {
	want := testState()
	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("failed to encode the checkpoint: %v", err)
	}
	client := &stubS3Client{objects: map[string][]byte{"checkpoint.json": encoded}}
	store, err := NewS3Store(client, "s3://my-bucket/checkpoint.json")
	if err != nil {
		t.Fatalf("failed to create S3 store: %v", err)
	}

	got, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	assertState(t, got, want)
	if !client.bodyClosed {
		t.Error("the response body was never closed")
	}
}

// TestS3Store_MissingObjectVariants verifies both of the errors S3 implementations use
// for an absent object read as a fresh start. Treating either as a failure would stop
// the very first run of a restore, before there is any checkpoint to find.
func TestS3Store_MissingObjectVariants(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "NoSuchKey", err: &types.NoSuchKey{}},
		{name: "NotFound", err: &types.NotFound{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := NewS3Store(&stubS3Client{getErr: tt.err}, "s3://my-bucket/checkpoint.json")
			if err != nil {
				t.Fatalf("failed to create S3 store: %v", err)
			}

			state, err := store.Load(context.Background())
			if err != nil {
				t.Fatalf("expected a missing checkpoint to read as empty state, got %v", err)
			}
			if len(state.Completed) != 0 || len(state.Offsets) != 0 {
				t.Errorf("expected no recorded progress, got %+v", state)
			}
		})
	}
}

// TestS3Store_CreatesTheCheckpointOnlyWhenAbsent verifies a store that loaded no
// checkpoint insists on creating one, so two runs started against the same fresh URI
// cannot both believe they own it: the second to write is refused.
func TestS3Store_CreatesTheCheckpointOnlyWhenAbsent(t *testing.T) {
	client := &stubS3Client{objects: map[string][]byte{}}
	first, err := NewS3Store(client, "s3://my-bucket/checkpoint.json")
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
	}
	second, err := NewS3Store(client, "s3://my-bucket/checkpoint.json")
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
	}
	ctx := context.Background()
	if _, err := first.Load(ctx); err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if _, err := second.Load(ctx); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if err := first.Save(ctx, testState()); err != nil {
		t.Fatalf("expected the first run to create the checkpoint, got %v", err)
	}
	if err := second.Save(ctx, testState()); !errors.Is(err, ErrCheckpointContended) {
		t.Errorf("expected the second run refused as contended, got %v", err)
	}
}

// TestS3Store_OverwritesOnlyTheVersionItLastWrote verifies successive saves by one run
// succeed, each replacing the version the run itself wrote, while a save by a run
// holding an older version is refused. This is what stops a run that fell behind
// another from erasing the other's progress.
func TestS3Store_OverwritesOnlyTheVersionItLastWrote(t *testing.T) {
	client := &stubS3Client{objects: map[string][]byte{"checkpoint.json": []byte(`{}`)}}
	owner, err := NewS3Store(client, "s3://my-bucket/checkpoint.json")
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
	}
	straggler, err := NewS3Store(client, "s3://my-bucket/checkpoint.json")
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
	}
	ctx := context.Background()
	if _, err := owner.Load(ctx); err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if _, err := straggler.Load(ctx); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := owner.Save(ctx, testState()); err != nil {
			t.Fatalf("expected save %d by the owner to succeed, got %v", i+1, err)
		}
	}
	if err := straggler.Save(ctx, testState()); !errors.Is(err, ErrCheckpointContended) {
		t.Errorf("expected a save over another run's progress refused as contended, got %v", err)
	}
}

// TestS3Store_ContentionIsNamedWithoutInvitingDeletion verifies the refusal tells the
// operator not to delete the checkpoint. Deleting it is the instinctive response to a
// precondition failure and the one that makes both runs write the export again.
func TestS3Store_ContentionIsNamedWithoutInvitingDeletion(t *testing.T) {
	client := &stubS3Client{objects: map[string][]byte{"checkpoint.json": []byte(`{}`)}}
	store, err := NewS3Store(client, "s3://my-bucket/checkpoint.json")
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
	}

	err = store.Save(context.Background(), testState())
	if err == nil || !strings.Contains(err.Error(), "do not delete") {
		t.Errorf("expected the refusal to warn against deleting the checkpoint, got %v", err)
	}
}

// TestMemoryStore_LoadDoesNotAliasItsState verifies editing what Load returned leaves
// the store unchanged, so a caller cannot corrupt the checkpoint through a shared map.
func TestMemoryStore_LoadDoesNotAliasItsState(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if err := store.Save(ctx, testState()); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	loaded.Offsets["data-002.json.gz"] = 0
	loaded.Completed[0] = "tampered"

	again, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	assertState(t, again, testState())
}

// callerContextKey marks the context a test passed in, so a client stub can tell the
// caller's context from one the code under test substituted for it.
type callerContextKey struct{}

// stubS3Client serves checkpoint objects from memory and can fail either operation.
// It counts calls that arrived without the marker a test put on its context, which is
// how a call made under a substituted context is told from one made under the caller's.
type stubS3Client struct {
	objects    map[string][]byte
	etags      map[string]string // ETag per object; every write gets a new one
	getErr     error
	putErr     error
	writes     int
	detached   int
	bodyClosed bool
}

// etag reports the current ETag of an object, minting one for objects seeded without.
func (s *stubS3Client) etag(key string) string {
	if s.etags == nil {
		s.etags = map[string]string{}
	}
	if _, ok := s.etags[key]; !ok {
		s.etags[key] = "seeded"
	}
	return s.etags[key]
}

// preconditionFailed is what S3 answers a conditional write whose condition did not hold.
func preconditionFailed() error {
	return &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "At least one of the pre-conditions you specified did not hold"}
}

// trackedBody fails a read that arrives after Close, the way a real S3 response body
// does, so a store that closed one before finishing with it is caught here.
type trackedBody struct {
	reader *bytes.Reader
	store  *stubS3Client
}

func (b *trackedBody) Read(p []byte) (int, error) {
	if b.store.bodyClosed {
		return 0, fmt.Errorf("read after the response body was closed")
	}
	return b.reader.Read(p)
}

func (b *trackedBody) Close() error {
	b.store.bodyClosed = true
	return nil
}

func (s *stubS3Client) noteContext(ctx context.Context) {
	if ctx.Value(callerContextKey{}) == nil {
		s.detached++
	}
}

// requireBucket stands in for S3 rejecting a request that names no bucket, so a
// checkpoint addressed to nowhere fails here rather than appearing to succeed.
func requireBucket(bucket *string) error {
	if bucket == nil || *bucket == "" {
		return fmt.Errorf("no bucket in request")
	}
	return nil
}

func (s *stubS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	s.noteContext(ctx)
	if err := requireBucket(params.Bucket); err != nil {
		return nil, err
	}
	if s.getErr != nil {
		return nil, s.getErr
	}
	data, ok := s.objects[*params.Key]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	etag := s.etag(*params.Key)
	s.bodyClosed = false
	return &s3.GetObjectOutput{Body: &trackedBody{reader: bytes.NewReader(data), store: s}, ETag: &etag}, nil
}

func (s *stubS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	s.noteContext(ctx)
	if err := requireBucket(params.Bucket); err != nil {
		return nil, err
	}
	if s.putErr != nil {
		return nil, s.putErr
	}
	// The conditions are enforced the way S3 enforces them, or the guard against two
	// runs sharing a checkpoint would ship untested.
	_, exists := s.objects[*params.Key]
	if params.IfNoneMatch != nil && *params.IfNoneMatch == "*" && exists {
		return nil, preconditionFailed()
	}
	if params.IfMatch != nil && (!exists || *params.IfMatch != s.etag(*params.Key)) {
		return nil, preconditionFailed()
	}
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	if s.objects == nil {
		s.objects = map[string][]byte{}
	}
	s.objects[*params.Key] = data
	s.writes++
	if s.etags == nil {
		s.etags = map[string]string{}
	}
	etag := fmt.Sprintf("v%d", s.writes)
	s.etags[*params.Key] = etag
	return &s3.PutObjectOutput{ETag: &etag}, nil
}

func (s *stubS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{}, nil
}
