// Package main is the ddb-pitr command: it parses the flags, wires the AWS clients to
// the restore and maps the outcome to an exit status.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gurre/ddb-pitr/aws"
	"github.com/gurre/ddb-pitr/checkpoint"
	"github.com/gurre/ddb-pitr/config"
	"github.com/gurre/ddb-pitr/coordinator"
	"github.com/gurre/ddb-pitr/itemimage"
	"github.com/gurre/ddb-pitr/lease"
	"github.com/gurre/ddb-pitr/manifest"
	"github.com/gurre/ddb-pitr/metrics"
	"github.com/gurre/ddb-pitr/writer"
	"github.com/gurre/s3streamer"
)

// Build identity, stamped in at link time by the release build. The defaults are what a
// binary built straight from a working tree reports, so an operator can always tell a
// released build from a local one.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// exitSkipped is the status of a restore that ran to the end but skipped lines it could
// not decode. It is told apart from exitFailed because the remedy differs: the table
// holds everything else, and running again changes nothing.
const (
	exitFailed  = 1
	exitSkipped = 3
	// exitHeld is the status of a restore that did not start because another restore
	// holds the table. Nothing was written; the remedy is to wait for the other or stop it.
	exitHeld = 4
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(exitStatus(err))
	}
}

// exitStatus is the status a failed run exits with, which tells a script what to do
// next: nothing will change a restore that skipped records, a table another restore
// holds needs that restore to finish or be stopped, and anything else is worth running
// again.
func exitStatus(err error) int {
	switch {
	case errors.Is(err, coordinator.ErrRecordsSkipped):
		return exitSkipped
	case errors.Is(err, lease.ErrLeaseHeld):
		return exitHeld
	}
	return exitFailed
}

// errNothingToDo is returned by parseArgs when the command line asked only for the
// version or for help, and that has been answered.
var errNothingToDo = errors.New("nothing to do")

// parseArgs turns the command line into a validated configuration. The command takes
// flags only: an operator who types a word before them, such as the subcommand an
// older README showed, gets told which word rather than a complaint about a flag the
// word stopped from being read.
func parseArgs(args []string, out io.Writer) (*config.Config, error) {
	fs := flag.NewFlagSet("ddb-pitr", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.Usage = func() {
		// Usage goes to whatever the caller gave for output; a write failing there
		// has nowhere better to be reported.
		_, _ = fmt.Fprintln(out, "Usage: ddb-pitr --table TABLE --export s3://BUCKET/PREFIX/AWSDynamoDB/EXPORT-ID/ [flags]")
		_, _ = fmt.Fprintln(out)
		fs.PrintDefaults()
	}

	tableName := fs.String("table", "", "DynamoDB table name to restore to")
	exportS3URI := fs.String("export", "", "S3 URI of the export's manifest-summary.json, or of the directory holding it")
	region := fs.String("region", "", "AWS region; resolved from your AWS environment when omitted")
	resumeKey := fs.String("resume", "", "S3 URI to record progress in; defaults to a key in the export's own bucket")
	noResume := fs.Bool("no-resume", false, "Record no progress, so an interrupted restore starts over")
	readers := fs.Int("readers", 50, "Data files read at once, which is how widely writes are spread over the table's partitions")
	maxInFlight := fs.Int("max-in-flight", writer.DefaultMaxInFlight, "Most writes to the table in flight at once, 1 to 4096; the restore settles below this unless latency to the table holds it here")
	reportS3URI := fs.String("report", "", "S3 URI for the final report")
	dryRun := fs.Bool("dry-run", false, "Read and measure the whole export without writing to the table")
	shutdownTimeout := fs.Duration("shutdown-timeout", 5*time.Minute, "How long an interrupted restore has to record where it stopped")
	showVersion := fs.Bool("version", false, "Print the build identity and exit")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, errNothingToDo
		}
		return nil, fmt.Errorf("failed to parse flags: %w", err)
	}
	if fs.NArg() > 0 {
		fs.Usage()
		return nil, fmt.Errorf("unexpected argument %q: ddb-pitr takes flags only (see --help)", fs.Arg(0))
	}

	// Asked before anything is validated, so the version is readable without a
	// complete configuration.
	if *showVersion {
		_, _ = fmt.Fprintf(out, "ddb-pitr %s (commit %s, built %s)\n", version, commit, date)
		return nil, errNothingToDo
	}

	cfg := &config.Config{
		TableName:       *tableName,
		ExportS3URI:     *exportS3URI,
		Region:          *region,
		ResumeKey:       *resumeKey,
		Readers:         *readers,
		MaxInFlight:     *maxInFlight,
		ReportS3URI:     *reportS3URI,
		DryRun:          *dryRun,
		NoResume:        *noResume,
		ShutdownTimeout: *shutdownTimeout,
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

// newCheckpointStore returns where the restore will record its progress. A configuration
// that names a checkpoint gets one in S3, which outlives the process and is what a resume
// reads; anything else gets memory, which does not, and says so. A dry run is always the
// latter, whatever was asked for, so a later restore cannot resume past work that was
// only ever measured.
func newCheckpointStore(cfg *config.Config, client aws.S3Client) (checkpoint.Store, error) {
	uri := cfg.CheckpointURI()
	if uri == "" {
		if !cfg.DryRun {
			fmt.Fprintln(os.Stderr, "no-resume: progress is not recorded, so an interruption starts this restore over")
		}
		return checkpoint.NewMemoryStore(), nil
	}
	store, err := checkpoint.NewS3Store(client, uri)
	if err != nil {
		return nil, fmt.Errorf("failed to create checkpoint store: %w", err)
	}
	fmt.Printf("Recording progress at %s\n", uri)
	return store, nil
}

// run parses the command line and carries out the restore it describes.
func run() error {
	cfg, err := parseArgs(os.Args[1:], os.Stdout)
	if errors.Is(err, errNothingToDo) {
		return nil
	}
	if err != nil {
		return err
	}

	// The region is taken from the flag when given and otherwise resolved the way the
	// AWS CLI resolves it: environment, profile, instance metadata. Nothing resolving
	// is reported here, naming the flag, rather than by the first request.
	var loadOpts []func(*awsconfig.LoadOptions) error
	if cfg.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}
	if awsCfg.Region == "" {
		return fmt.Errorf("no AWS region: pass --region or set one in your AWS environment")
	}
	// Each pool keeps as many idle connections as it may have requests in flight. The SDK
	// keeps ten idle connections per host by default, so beyond that every request would
	// open a fresh one and pay for a TLS handshake it then throws away, which caps a
	// restore well below what it could write. The writer settles its own concurrency far
	// below its ceiling, and idle connections it never opened cost nothing.
	dynamoHTTP := awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
		t.MaxIdleConnsPerHost = cfg.MaxInFlight
		t.MaxIdleConns = cfg.MaxInFlight
	})
	s3HTTP := awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
		t.MaxIdleConnsPerHost = cfg.Readers
		t.MaxIdleConns = cfg.Readers
	})

	// The writer owns the retry policy for DynamoDB: it holds throttled work until the
	// table has capacity for it and bounds everything else. Left at the SDK's default of
	// three attempts, every one of the writer's attempts would be up to three requests,
	// and the throttle count the operator watches would be a third of the truth.
	dynamoClient := aws.NewDynamoDBClient(dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		o.RetryMaxAttempts = 1
		o.HTTPClient = dynamoHTTP
	}))
	rawS3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.HTTPClient = s3HTTP
	})
	s3Client := aws.NewS3Client(rawS3Client)

	// Ctrl-C or a container runtime's SIGTERM asks the restore to stop: writes in flight
	// are abandoned and the checkpoint is saved, which costs the resume those batches and
	// nothing else, since their offsets were never recorded. The handler is removed as
	// soon as it fires, so a second signal takes the default disposition and kills the
	// process, for the operator who cannot wait for a clean stop.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop()
	}()

	// Create metrics for tracking progress
	m := metrics.NewMetrics()

	// Create writer callbacks that wire to metrics
	writerCallbacks := writer.Callbacks{
		OnThrottle:    m.RecordThrottle,
		OnRetry:       m.RecordRetry,
		OnLost:        func(kind itemimage.OperationType, n int) { m.RecordLost(kind, int64(n)) },
		OnPace:        m.RecordPace,
		OnConcurrency: m.RecordConcurrency,
	}

	// Create and initialize required components for the coordinator
	manifestLoader := manifest.NewS3Loader(s3Client)
	streamer := s3streamer.NewS3Streamer(rawS3Client)
	jsonDecoder := itemimage.NewJSONDecoder()

	// The table is claimed before anything else reads or writes on its behalf, so a
	// second restore of it stops here with the table untouched, and the checkpoint the
	// coordinator loads is the one the previous holder finished saving. From here the
	// restore runs under the claim's context, which ends the moment the claim can no
	// longer be shown to be this restore's, and every write to the table or to the
	// checkpoint checks the claim just before it is sent. A dry run writes neither and
	// claims nothing; any other restore without somewhere to claim the table is a wiring
	// mistake, since it would run unguarded.
	var claim claimChecker = unclaimed{}
	if !cfg.DryRun {
		uri := cfg.LeaseURI(awsCfg.Region)
		if uri == "" {
			return fmt.Errorf("no bucket to claim table %s in; a restore that writes must claim its table", cfg.TableName)
		}
		host, hostErr := os.Hostname()
		if hostErr != nil {
			// Only the operator reading a refusal sees the host; the claim does not
			// depend on it.
			host = "unknown host"
		}
		// The lease bounds and retries its own requests, within how long it may go
		// unrenewed; SDK retries inside that would spend the margin unseen.
		leaseClient := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.RetryMaxAttempts = 1
		})
		held, acquireErr := lease.Acquire(ctx, leaseClient, uri, lease.Holder{
			Host:    host,
			Table:   cfg.TableName,
			Export:  cfg.ExportS3URI,
			Version: version,
			PID:     os.Getpid(),
		}, lease.WithNotices(os.Stderr))
		if acquireErr != nil {
			return acquireErr
		}
		defer func() {
			if releaseErr := held.Release(context.Background()); releaseErr != nil {
				fmt.Fprintf(os.Stderr, "Warning: %v\n", releaseErr)
			}
		}()
		ctx = held.Context()
		claim = held
	}

	// A dry run reads, decodes and measures the whole export but writes nothing. Which
	// writer is wired in is the only thing that decides that, so there is no path by
	// which a dry run reaches the table.
	var ddbWriter coordinator.Submitter
	if cfg.DryRun {
		ddbWriter = writer.NewDiscard()
	} else {
		ddbWriter = writer.NewDynamoDBWriter(&fencedTable{table: dynamoClient, claim: claim}, cfg.TableName, writerCallbacks,
			writer.WithMaxInFlight(cfg.MaxInFlight))
	}

	if cfg.ResumeKey != "" && cfg.DryRun {
		fmt.Fprintln(os.Stderr, "dry run: --resume is not used, since a dry run records no progress")
	}
	store, err := newCheckpointStore(cfg, s3Client)
	if err != nil {
		return err
	}
	checkpointStore := &fencedStore{store: store, claim: claim}

	// Create report uploader if report URI is provided. The variable is declared as the
	// interface the coordinator expects: a nil *S3ReportUploader would otherwise arrive
	// there as a non-nil interface holding a nil pointer, and the coordinator's nil check
	// would not catch it.
	var reportUploader coordinator.ReportUploader
	if cfg.ReportS3URI != "" {
		reportUploader = aws.NewS3ReportUploader(s3Client)
	}

	// Create the coordinator with all dependencies
	coord := coordinator.NewCoordinator(
		cfg,
		manifestLoader,
		streamer,
		jsonDecoder,
		ddbWriter,
		checkpointStore,
		reportUploader,
		m,
	)

	// Run the coordinator
	if cfg.DryRun {
		fmt.Printf("Dry run: reading %s and measuring what a restore of table %s would write\n",
			cfg.ExportS3URI, cfg.TableName)
	} else {
		fmt.Printf("Starting restore of table %s from %s\n", cfg.TableName, cfg.ExportS3URI)
	}
	if err := coord.Run(ctx); err != nil {
		// A restore that lost its claim stopped because another may now hold the table,
		// which an operator must hear as that rather than as an interruption.
		if cause := context.Cause(ctx); errors.Is(cause, lease.ErrLeaseLost) {
			return fmt.Errorf("restore stopped: %w; another restore may be writing the table, so make sure none is before running this one again: %w", cause, err)
		}
		// A run that skipped records did finish, and one that was interrupted was
		// stopped rather than broken. Wrapping either as a failure would contradict
		// what the message and the exit status say about what to do next.
		if errors.Is(err, coordinator.ErrRecordsSkipped) {
			return err
		}
		if errors.Is(err, coordinator.ErrInterrupted) {
			return fmt.Errorf("%w; run the same command again to carry on", err)
		}
		return fmt.Errorf("restore operation failed: %w", err)
	}

	if cfg.DryRun {
		fmt.Println("Dry run completed successfully; nothing was written")
	} else {
		fmt.Println("Restore operation completed successfully")
	}
	return nil
}

// claimChecker says whether this restore may still write, which is whether it can still
// show that it holds the table.
type claimChecker interface {
	Check() error
}

// unclaimed stands for a dry run, which writes neither the table nor a checkpoint that
// a claim would guard.
type unclaimed struct{}

func (unclaimed) Check() error { return nil }

// fencedTable refuses to send a write once the restore's claim on the table has lapsed.
// It checks just before every request, including the ones the writer sends again after
// a wait or a partial acceptance, so a restore stops at the next write however long its
// batches were in flight when the claim lapsed.
type fencedTable struct {
	table aws.DynamoDBClient
	claim claimChecker
}

// BatchWriteItem sends params unless the claim can no longer be relied on.
func (f *fencedTable) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if err := f.claim.Check(); err != nil {
		return nil, err
	}
	return f.table.BatchWriteItem(ctx, params, optFns...)
}

// fencedStore refuses to save progress once the restore's claim on the table has lapsed.
// A restore that lost its claim may share its checkpoint with the one that took over,
// and a late save would overwrite progress that is no longer its to record.
type fencedStore struct {
	store checkpoint.Store
	claim claimChecker
}

// Load reads the checkpoint; reading needs no claim.
func (f *fencedStore) Load(ctx context.Context) (checkpoint.State, error) {
	return f.store.Load(ctx)
}

// Save records state unless the claim can no longer be relied on.
func (f *fencedStore) Save(ctx context.Context, state checkpoint.State) error {
	if err := f.claim.Check(); err != nil {
		return err
	}
	return f.store.Save(ctx, state)
}
