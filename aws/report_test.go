package aws

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	json "github.com/goccy/go-json"
	"github.com/gurre/ddb-pitr/metrics"
)

// TestUploadReportSplitsTheURI verifies the report is written to the bucket and key the
// URI names. Transposing them puts the record of the restore somewhere nobody will look
// for it, and the upload would still report success.
func TestUploadReportSplitsTheURI(t *testing.T) {
	client := &stubS3Client{}

	err := NewS3ReportUploader(client).UploadReport(
		context.Background(), "s3://my-reports/restores/2026-01-16.json", metrics.Report{})
	if err != nil {
		t.Fatalf("UploadReport failed: %v", err)
	}

	if client.bucket != "my-reports" {
		t.Errorf("bucket = %q, want my-reports", client.bucket)
	}
	if client.key != "restores/2026-01-16.json" {
		t.Errorf("key = %q, want restores/2026-01-16.json", client.key)
	}
	if client.contentType != "application/json" {
		t.Errorf("content type = %q, want application/json", client.contentType)
	}
}

// TestUploadReportWritesReadableJSON verifies the uploaded body is the report, encoded so
// it can be read back. It is the durable record of the restore, so a body that arrives
// empty or malformed loses the whole account of what happened.
func TestUploadReportWritesReadableJSON(t *testing.T) {
	client := &stubS3Client{}
	report := metrics.Report{
		Duration:   90 * time.Second,
		TotalItems: 4200,
		Throttles:  7,
		LostItems:  2,
	}

	if err := NewS3ReportUploader(client).UploadReport(
		context.Background(), "s3://my-reports/restore.json", report); err != nil {
		t.Fatalf("UploadReport failed: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(client.body, &got); err != nil {
		t.Fatalf("uploaded body is not readable JSON: %v", err)
	}
	if got["totalItems"] != float64(4200) {
		t.Errorf("totalItems = %v, want 4200", got["totalItems"])
	}
	if got["throttles"] != float64(7) {
		t.Errorf("throttles = %v, want 7", got["throttles"])
	}
	if got["duration"] != "1m30s" {
		t.Errorf("duration = %v, want 1m30s", got["duration"])
	}
}

// TestUploadReportRejectsUnusableURIs verifies a destination that names nowhere is
// reported rather than attempted. An upload aimed at an empty bucket or key would either
// fail far from its cause or quietly write the report where it cannot be found.
func TestUploadReportRejectsUnusableURIs(t *testing.T) {
	for _, uri := range []string{
		"https://my-reports/restore.json",
		"my-reports/restore.json",
		"s3://",
		"s3://my-reports",
		"s3:///restore.json",
	} {
		t.Run(uri, func(t *testing.T) {
			client := &stubS3Client{}
			if err := NewS3ReportUploader(client).UploadReport(
				context.Background(), uri, metrics.Report{}); err == nil {
				t.Errorf("expected %q to be rejected", uri)
			}
			if client.calls != 0 {
				t.Errorf("expected no upload attempted, got %d", client.calls)
			}
		})
	}
}

// TestUploadReportSurfacesFailure verifies an upload that does not land is reported. An
// operator told the report was uploaded when it was not has no record of the restore and
// no reason to look for one.
func TestUploadReportSurfacesFailure(t *testing.T) {
	client := &stubS3Client{err: errors.New("access denied")}

	err := NewS3ReportUploader(client).UploadReport(
		context.Background(), "s3://my-reports/restore.json", metrics.Report{})
	if err == nil {
		t.Fatal("expected a failed upload to be reported")
	}
	if !errors.Is(err, client.err) {
		t.Errorf("expected the underlying failure wrapped, got %v", err)
	}
}

// TestUploadReportRejectsUnparseableURI verifies a destination that cannot be parsed at
// all is reported rather than attempted, so the failure names the URI instead of
// surfacing as an S3 error about an empty bucket.
func TestUploadReportRejectsUnparseableURI(t *testing.T) {
	client := &stubS3Client{}

	err := NewS3ReportUploader(client).UploadReport(
		context.Background(), "s3://my-reports/%zz", metrics.Report{})
	if err == nil {
		t.Fatal("expected an unparseable URI to be rejected")
	}
	if client.calls != 0 {
		t.Errorf("expected no upload attempted, got %d", client.calls)
	}
}

// TestUploadReportSendsTheCallersContext verifies the upload is made under the caller's
// context, so a shutting-down restore is not left waiting on S3 with no deadline.
func TestUploadReportSendsTheCallersContext(t *testing.T) {
	client := &stubS3Client{}

	ctx := context.WithValue(context.Background(), callerContextKey{}, true)
	if err := NewS3ReportUploader(client).UploadReport(
		ctx, "s3://my-reports/restore.json", metrics.Report{}); err != nil {
		t.Fatalf("UploadReport failed: %v", err)
	}

	if client.detached > 0 {
		t.Errorf("%d uploads were made outside the caller's context", client.detached)
	}
}

// callerContextKey marks the context a test passed in, so the stub can tell the caller's
// context from one the code under test substituted for it.
type callerContextKey struct{}

// stubS3Client records the one PutObject the report uploader makes.
type stubS3Client struct {
	err         error
	body        []byte
	bucket      string
	key         string
	contentType string
	calls       int
	detached    int
}

func (s *stubS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if ctx.Value(callerContextKey{}) == nil {
		s.detached++
	}
	s.calls++
	if params.Bucket != nil {
		s.bucket = *params.Bucket
	}
	if params.Key != nil {
		s.key = *params.Key
	}
	if params.ContentType != nil {
		s.contentType = *params.ContentType
	}
	if params.Body != nil {
		data, err := io.ReadAll(params.Body)
		if err != nil {
			return nil, err
		}
		s.body = data
	}
	if s.err != nil {
		return nil, s.err
	}
	return &s3.PutObjectOutput{}, nil
}

func (s *stubS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{}, nil
}

func (s *stubS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{}, nil
}
