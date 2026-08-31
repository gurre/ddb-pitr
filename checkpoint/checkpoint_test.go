package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

func TestFileStore_SaveLoad(t *testing.T) {
	// Create a temporary directory for the test
	tmpDir := t.TempDir()
	uri := "file://" + filepath.Join(tmpDir, "checkpoint.json")

	store, err := NewFileStore(uri)
	if err != nil {
		t.Fatalf("failed to create file store: %v", err)
	}

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

func TestFileStore_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	uri := "file://" + filepath.Join(tmpDir, "nonexistent.json")

	store, err := NewFileStore(uri)
	if err != nil {
		t.Fatalf("failed to create file store: %v", err)
	}

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("failed to load non-existent state: %v", err)
	}

	// Should return empty state for non-existent file
	if state.ExportID != "" || len(state.Completed) != 0 || len(state.Offsets) != 0 {
		t.Errorf("expected empty state for non-existent file, got: %+v", state)
	}
}

func TestFileStore_InvalidURI(t *testing.T) {
	testCases := []string{
		"s3://bucket/key",
		"http://example.com/file",
		"/path/without/scheme",
	}

	for _, uri := range testCases {
		t.Run(uri, func(t *testing.T) {
			_, err := NewFileStore(uri)
			if err == nil {
				t.Errorf("expected error for invalid file URI: %s", uri)
			}
		})
	}
}

func TestFileStore_CreatesDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	nestedDir := filepath.Join(tmpDir, "nested", "dir")
	uri := "file://" + filepath.Join(nestedDir, "checkpoint.json")

	store, err := NewFileStore(uri)
	if err != nil {
		t.Fatalf("failed to create file store: %v", err)
	}

	// Verify directory was created
	if _, err := os.Stat(nestedDir); os.IsNotExist(err) {
		t.Error("expected nested directory to be created")
	}

	// Verify we can save to the store
	ctx := context.Background()
	state := State{ExportID: "test"}
	if err := store.Save(ctx, state); err != nil {
		t.Fatalf("failed to save state: %v", err)
	}
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

// TestFileStore_RoundTripSurvivesEncoding verifies progress written to disk comes back
// intact. It is the only checkpoint that outlives the process without S3, so a resume
// after a crash depends on the whole record surviving the round trip.
func TestFileStore_RoundTripSurvivesEncoding(t *testing.T) {
	store, err := NewFileStore("file://" + filepath.Join(t.TempDir(), "checkpoint.json"))
	if err != nil {
		t.Fatalf("failed to create file store: %v", err)
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

// TestFileStore_ReportsUnreadableContent verifies a checkpoint file that is not valid
// state is reported rather than read as no progress, which would silently restart the
// restore from the top and rewrite everything.
func TestFileStore_ReportsUnreadableContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	if err := os.WriteFile(path, []byte("not json"), 0600); err != nil {
		t.Fatalf("failed to write the corrupt checkpoint: %v", err)
	}

	store, err := NewFileStore("file://" + path)
	if err != nil {
		t.Fatalf("failed to create file store: %v", err)
	}

	if _, err := store.Load(context.Background()); err == nil {
		t.Error("expected a corrupt checkpoint file to be reported")
	}
}

// TestFileStore_ReportsWriteFailure verifies a checkpoint that cannot be written is
// reported. Progress an operator believes is saved but is not makes a resume lose work.
func TestFileStore_ReportsWriteFailure(t *testing.T) {
	// A directory occupies the checkpoint's path, so the write cannot succeed.
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	if err := os.Mkdir(path, 0755); err != nil {
		t.Fatalf("failed to occupy the checkpoint path: %v", err)
	}

	store, err := NewFileStore("file://" + path)
	if err != nil {
		t.Fatalf("failed to create file store: %v", err)
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

// TestNewFileStore_RejectsRelativePaths verifies a checkpoint path that is not absolute
// is refused. Resolving it against whatever directory the process happens to run in puts
// the record of a restore somewhere unpredictable, and a resume would not find it.
func TestNewFileStore_RejectsRelativePaths(t *testing.T) {
	for _, uri := range []string{"file:checkpoints/restore.json", "file:./restore.json"} {
		t.Run(uri, func(t *testing.T) {
			if _, err := NewFileStore(uri); err == nil {
				t.Errorf("expected a relative checkpoint path to be refused: %s", uri)
			}
		})
	}
}

// TestNewFileStore_ReportsUncreatableDirectory verifies a checkpoint directory that
// cannot be created is reported at construction, rather than at the first save halfway
// through a restore.
func TestNewFileStore_ReportsUncreatableDirectory(t *testing.T) {
	// A regular file occupies the directory's path, so it cannot be created.
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0600); err != nil {
		t.Fatalf("failed to occupy the directory path: %v", err)
	}

	if _, err := NewFileStore("file://" + filepath.Join(blocker, "checkpoint.json")); err == nil {
		t.Error("expected an uncreatable checkpoint directory to be reported")
	}
}

// callerContextKey marks the context a test passed in, so a client stub can tell the
// caller's context from one the code under test substituted for it.
type callerContextKey struct{}

// stubS3Client serves checkpoint objects from memory and can fail either operation.
// It counts calls that arrived without the marker a test put on its context, which is
// how a call made under a substituted context is told from one made under the caller's.
type stubS3Client struct {
	objects    map[string][]byte
	getErr     error
	putErr     error
	detached   int
	bodyClosed bool
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
	return &s3.GetObjectOutput{Body: &trackedBody{reader: bytes.NewReader(data), store: s}}, nil
}

func (s *stubS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	s.noteContext(ctx)
	if err := requireBucket(params.Bucket); err != nil {
		return nil, err
	}
	if s.putErr != nil {
		return nil, s.putErr
	}
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	s.objects[*params.Key] = data
	return &s3.PutObjectOutput{}, nil
}

func (s *stubS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{}, nil
}
