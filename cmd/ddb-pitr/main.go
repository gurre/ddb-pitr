// Package main is the ddb-pitr command: it parses the flags, wires the AWS clients to
// the restore and maps the outcome to an exit status.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gurre/ddb-pitr/aws"
	"github.com/gurre/ddb-pitr/checkpoint"
	"github.com/gurre/ddb-pitr/config"
	"github.com/gurre/ddb-pitr/coordinator"
	"github.com/gurre/ddb-pitr/itemimage"
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
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		if errors.Is(err, coordinator.ErrRecordsSkipped) {
			os.Exit(exitSkipped)
		}
		os.Exit(exitFailed)
	}
}

// errVersionShown is returned by parseArgs when --version was asked for and answered;
// there is nothing left to do.
var errVersionShown = errors.New("version shown")

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
	resumeKey := fs.String("resume", "", "S3 URI for checkpoint file")
	maxWorkers := fs.Int("workers", 10, "Maximum number of concurrent workers")
	batchSize := fs.Int("batch", 25, "Batch size for DynamoDB writes (max 25)")
	reportS3URI := fs.String("report", "", "S3 URI for the final report")
	dryRun := fs.Bool("dry-run", false, "Read and measure the whole export without writing to the table")
	shutdownTimeout := fs.Duration("shutdown-timeout", 5*time.Minute, "How long an interrupted restore has to finish in-flight writes and save its checkpoint")
	showVersion := fs.Bool("version", false, "Print the build identity and exit")

	if err := fs.Parse(args); err != nil {
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
		return nil, errVersionShown
	}

	cfg := &config.Config{
		TableName:       *tableName,
		ExportS3URI:     *exportS3URI,
		Region:          *region,
		ResumeKey:       *resumeKey,
		MaxWorkers:      *maxWorkers,
		BatchSize:       *batchSize,
		ReportS3URI:     *reportS3URI,
		DryRun:          *dryRun,
		ShutdownTimeout: *shutdownTimeout,
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

// run parses the command line and carries out the restore it describes.
func run() error {
	cfg, err := parseArgs(os.Args[1:], os.Stdout)
	if errors.Is(err, errVersionShown) {
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
	// The writer owns the retry policy for DynamoDB: it paces throttling for as long
	// as the run lives and bounds everything else. Left at the SDK's default of three
	// attempts, every one of the writer's attempts would be up to three requests, and
	// the throttle count the operator watches would be a third of the truth.
	dynamoClient := aws.NewDynamoDBClient(dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		o.RetryMaxAttempts = 1
	}))
	rawS3Client := s3.NewFromConfig(awsCfg)
	s3Client := aws.NewS3Client(rawS3Client)

	// Ctrl-C or a container runtime's SIGTERM asks the restore to stop: workers finish
	// their current batch and the checkpoint is saved. The handler is removed as soon
	// as it fires, so a second signal takes the default disposition and kills the
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
		OnThrottle: m.RecordThrottle,
		OnRetry:    m.RecordRetry,
		OnLost:     func(count int) { m.RecordLost(int64(count)) },
		OnWrite:    func(items, bytes int) { m.RecordBytes(int64(bytes)) },
	}

	// Create and initialize required components for the coordinator
	manifestLoader := manifest.NewS3Loader(s3Client)
	streamer := s3streamer.NewS3Streamer(rawS3Client)
	jsonDecoder := itemimage.NewJSONDecoder()

	// A dry run reads, decodes and measures the whole export but writes nothing. Which
	// writer is wired in is the only thing that decides that, so there is no path by
	// which a dry run reaches the table.
	var ddbWriter writer.Writer
	if cfg.DryRun {
		ddbWriter = writer.NewDiscard(writerCallbacks)
	} else {
		ddbWriter = writer.NewDynamoDBWriter(dynamoClient, cfg.TableName, cfg.BatchSize, writerCallbacks)
	}

	// Set up the checkpoint store based on ResumeKey. A dry run keeps its progress in
	// memory whatever was asked for: a later restore must not resume past work that
	// was only ever measured.
	var checkpointStore checkpoint.Store
	if cfg.ResumeKey != "" && !cfg.DryRun {
		// Use S3Store if a resume key is provided
		s3Store, err := checkpoint.NewS3Store(s3Client, cfg.ResumeKey)
		if err != nil {
			return fmt.Errorf("failed to create checkpoint store: %w", err)
		}
		checkpointStore = s3Store
	} else {
		// Use in-memory store if no resume key provided
		checkpointStore = checkpoint.NewMemoryStore()
	}

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
		// A run that skipped records did finish; wrapping it as a failure would
		// contradict the exit status that says so.
		if errors.Is(err, coordinator.ErrRecordsSkipped) {
			return err
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
