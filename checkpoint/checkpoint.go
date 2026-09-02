// Package checkpoint records what a restore has finished, so an interrupted run can be
// resumed rather than restarted.
package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
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
	Offsets   map[string]int64 `json:"offsets"`   // Offset of the last line written, per file still in progress
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

// ErrCheckpointContended is returned when the checkpoint object changed underneath a
// run, which means another restore is writing to the same checkpoint URI. Two runs
// sharing one would each overwrite the other's progress, and the survivor would skip
// files the other never finished.
var ErrCheckpointContended = errors.New("checkpoint: another restore is writing this checkpoint")

// S3Store implements the Store interface using AWS S3. Every save is conditional on
// the object being the version this store last read or wrote, so two runs pointed at
// the same checkpoint URI cannot silently interleave their progress; the second to
// write fails with ErrCheckpointContended. A store is used by one run at a time,
// serialised by the coordinator; it is not safe for concurrent Save calls.
// Example:
//
//	client := s3.NewFromConfig(cfg)
//	store := checkpoint.NewS3Store(client, "s3://my-bucket/checkpoints/restore-123.json")
//	state, err := store.Load(ctx)
type S3Store struct {
	client aws.S3Client
	bucket string
	key    string
	etag   string // ETag of the object as last read or written; empty when there is none
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

// Load reads the checkpoint, remembering which version it read so the next Save can
// insist on replacing that version and no other. A checkpoint that does not exist yet
// reads as no progress.
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
		var noSuchKey *types.NoSuchKey
		var notFound *types.NotFound
		if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
			s.etag = ""
			return State{}, nil
		}
		return State{}, fmt.Errorf("failed to get checkpoint: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var state State
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return State{}, fmt.Errorf("failed to decode checkpoint: %w", err)
	}
	if resp.ETag != nil {
		s.etag = *resp.ETag
	}

	return state, nil
}

// Save writes the checkpoint on the condition that nothing else has written it since
// this store last read or wrote it: it must not exist if none was loaded, and must
// still carry the last known ETag otherwise. A failed condition means another run is
// using the same checkpoint and is reported as ErrCheckpointContended.
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

	input := &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &s.key,
		Body:   bytes.NewReader(data),
	}
	if s.etag == "" {
		input.IfNoneMatch = awssdk.String("*")
	} else {
		input.IfMatch = awssdk.String(s.etag)
	}

	resp, err := s.client.PutObject(ctx, input)
	if err != nil {
		if isPreconditionFailure(err) {
			return fmt.Errorf("%w at %s; do not delete it, that would make both restores write everything again",
				ErrCheckpointContended, s.key)
		}
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}
	if resp.ETag != nil {
		s.etag = *resp.ETag
	}

	return nil
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
