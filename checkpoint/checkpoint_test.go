package checkpoint

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestMemoryStore_SaveLoad(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	state := State{
		ExportID:       "export-123",
		LastFile:       "data-001.json",
		LastByteOffset: 1024,
	}

	if err := store.Save(ctx, state); err != nil {
		t.Fatalf("failed to save state: %v", err)
	}

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if loaded.ExportID != state.ExportID {
		t.Errorf("ExportID mismatch: got %s, want %s", loaded.ExportID, state.ExportID)
	}
	if loaded.LastFile != state.LastFile {
		t.Errorf("LastFile mismatch: got %s, want %s", loaded.LastFile, state.LastFile)
	}
	if loaded.LastByteOffset != state.LastByteOffset {
		t.Errorf("LastByteOffset mismatch: got %d, want %d", loaded.LastByteOffset, state.LastByteOffset)
	}
}

func TestMemoryStore_EmptyState(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("failed to load empty state: %v", err)
	}

	// Empty state should have zero values
	if state.ExportID != "" {
		t.Errorf("expected empty ExportID, got %s", state.ExportID)
	}
	if state.LastFile != "" {
		t.Errorf("expected empty LastFile, got %s", state.LastFile)
	}
	if state.LastByteOffset != 0 {
		t.Errorf("expected zero LastByteOffset, got %d", state.LastByteOffset)
	}
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
	state := State{
		ExportID:       "export-456",
		LastFile:       "data-002.json",
		LastByteOffset: 2048,
	}

	if err := store.Save(ctx, state); err != nil {
		t.Fatalf("failed to save state: %v", err)
	}

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if loaded.ExportID != state.ExportID {
		t.Errorf("ExportID mismatch: got %s, want %s", loaded.ExportID, state.ExportID)
	}
	if loaded.LastFile != state.LastFile {
		t.Errorf("LastFile mismatch: got %s, want %s", loaded.LastFile, state.LastFile)
	}
	if loaded.LastByteOffset != state.LastByteOffset {
		t.Errorf("LastByteOffset mismatch: got %d, want %d", loaded.LastByteOffset, state.LastByteOffset)
	}
}

func TestFileStore_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	uri := "file://" + filepath.Join(tmpDir, "nonexistent.json")

	store, err := NewFileStore(uri)
	if err != nil {
		t.Fatalf("failed to create file store: %v", err)
	}

	ctx := context.Background()
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("failed to load non-existent state: %v", err)
	}

	// Should return empty state for non-existent file
	if state.ExportID != "" || state.LastFile != "" || state.LastByteOffset != 0 {
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

func TestMemoryStore_Overwrite(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Save first state
	state1 := State{ExportID: "first", LastFile: "file1", LastByteOffset: 100}
	if err := store.Save(ctx, state1); err != nil {
		t.Fatalf("failed to save first state: %v", err)
	}

	// Save second state (should overwrite)
	state2 := State{ExportID: "second", LastFile: "file2", LastByteOffset: 200}
	if err := store.Save(ctx, state2); err != nil {
		t.Fatalf("failed to save second state: %v", err)
	}

	// Load and verify it's the second state
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if loaded.ExportID != "second" {
		t.Errorf("expected ExportID 'second', got %s", loaded.ExportID)
	}
}
