// Package checkpoint implements the checkpoint functionality as specified in section 4.7
// of the design specification. It handles saving and loading progress for resumable operations.
package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	json "github.com/goccy/go-json"
	"github.com/gurre/ddb-pitr/aws"
)

// State is the durable record of what a restore has finished, from which what remains
// to do is computed.
//
// Progress is recorded per file rather than as a single high-water mark because a pool
// of workers processes different files at the same time. A worker finishing the ninth
// file says nothing about the third through eighth, which other workers may still be
// part-way through; a single mark would let a resume skip them.
//
// Example:
//
//	store := checkpoint.NewS3Store(client, "s3://my-bucket/checkpoints/restore-123.json")
//	state, err := store.Load(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("%d files already done\n", len(state.Completed))
type State struct {
	Completed []string         `json:"completed"` // Keys of data files processed to the end
	ExportID  string           `json:"exportId"`  // Identity of the export this progress belongs to
	Offsets   map[string]int64 `json:"offsets"`   // Bytes already consumed, per file still in progress
}

// Store interface defines the contract for saving and loading checkpoint state.
// Example:
//
//	var store checkpoint.Store
//	state, err := store.Load(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	state.Completed = append(state.Completed, "new-file.json")
//	err = store.Save(ctx, state)
type Store interface {
	Load(ctx context.Context) (State, error)
	Save(ctx context.Context, s State) error
}

// S3Store implements the Store interface using AWS S3.
// Example:
//
//	client := s3.NewFromConfig(cfg)
//	store := checkpoint.NewS3Store(client, "s3://my-bucket/checkpoints/restore-123.json")
//	state, err := store.Load(ctx)
type S3Store struct {
	client aws.S3Client
	bucket string
	key    string
}

// NewS3Store creates a new S3Store instance from an S3 URI.
// Example:
//
//	client := s3.NewFromConfig(cfg)
//	store, err := checkpoint.NewS3Store(client, "s3://my-bucket/checkpoints/restore-123.json")
//	if err != nil {
//	    log.Fatal(err)
//	}
func NewS3Store(client aws.S3Client, uri string) (*S3Store, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid S3 URI: %w", err)
	}
	if u.Scheme != "s3" {
		return nil, fmt.Errorf("invalid S3 URI scheme: %s", u.Scheme)
	}

	return &S3Store{
		client: client,
		bucket: u.Host,
		key:    strings.TrimPrefix(u.Path, "/"),
	}, nil
}

// Load implements the checkpoint loading requirements from section 4.7.
// Example:
//
//	store := checkpoint.NewS3Store(client, "s3://my-bucket/checkpoints/restore-123.json")
//	state, err := store.Load(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Resuming: %d files done, %d part-way\n", len(state.Completed), len(state.Offsets))
func (s *S3Store) Load(ctx context.Context) (State, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &s.key,
	})
	if err != nil {
		// If the object doesn't exist, return empty state
		// Use proper error type assertion instead of string matching
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return State{}, nil
		}
		// Also check for NotFound which some S3-compatible stores return
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return State{}, nil
		}
		return State{}, fmt.Errorf("failed to get checkpoint: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var state State
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return State{}, fmt.Errorf("failed to decode checkpoint: %w", err)
	}

	return state, nil
}

// Save implements the checkpoint saving requirements from section 4.7.
// Example:
//
//	store := checkpoint.NewS3Store(client, "s3://my-bucket/checkpoints/restore-123.json")
//	state := checkpoint.State{
//	    ExportID:  "arn:aws:dynamodb:eu-north-1:123456789012:table/orders/export/01768385930622-efd1a093",
//	    Completed: []string{"data-001.json.gz"},
//	    Offsets:   map[string]int64{"data-002.json.gz": 1024},
//	}
//	err := store.Save(ctx, state)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (s *S3Store) Save(ctx context.Context, state State) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to encode checkpoint: %w", err)
	}

	// Use bytes.NewReader to avoid extra allocation from string conversion
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &s.key,
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	return nil
}

// FileStore implements the Store interface using the local filesystem.
// Example:
//
//	store := checkpoint.NewFileStore("file:///tmp/checkpoints/restore-123.json")
//	state, err := store.Load(ctx)
type FileStore struct {
	path string
}

// NewFileStore creates a new FileStore instance from a file URI.
// The path must be absolute and is cleaned to prevent path traversal attacks.
// Example:
//
//	store, err := checkpoint.NewFileStore("file:///tmp/checkpoints/restore-123.json")
//	if err != nil {
//	    log.Fatal(err)
//	}
func NewFileStore(uri string) (*FileStore, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid file URI: %w", err)
	}
	if u.Scheme != "file" {
		return nil, fmt.Errorf("invalid file URI scheme: %s", u.Scheme)
	}

	// Clean the path to resolve any .. or . components
	cleanPath := filepath.Clean(u.Path)

	// Ensure path is absolute to prevent relative path attacks
	if !filepath.IsAbs(cleanPath) {
		return nil, fmt.Errorf("checkpoint path must be absolute: %s", cleanPath)
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(cleanPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	return &FileStore{
		path: cleanPath,
	}, nil
}

// Load implements the checkpoint loading requirements from section 4.7.
// Example:
//
//	store := checkpoint.NewFileStore("file:///tmp/checkpoints/restore-123.json")
//	state, err := store.Load(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Resuming: %d files done, %d part-way\n", len(state.Completed), len(state.Offsets))
func (f *FileStore) Load(ctx context.Context) (State, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return State{}, nil
		}
		return State{}, fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return State{}, fmt.Errorf("failed to decode checkpoint: %w", err)
	}

	return state, nil
}

// Save implements the checkpoint saving requirements from section 4.7.
// Example:
//
//	store := checkpoint.NewFileStore("file:///tmp/checkpoints/restore-123.json")
//	state := checkpoint.State{
//	    ExportID:  "arn:aws:dynamodb:eu-north-1:123456789012:table/orders/export/01768385930622-efd1a093",
//	    Completed: []string{"data-001.json.gz"},
//	    Offsets:   map[string]int64{"data-002.json.gz": 1024},
//	}
//	err := store.Save(ctx, state)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (f *FileStore) Save(ctx context.Context, state State) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to encode checkpoint: %w", err)
	}

	if err := os.WriteFile(f.path, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint file: %w", err)
	}

	return nil
}
