// Package main implements the command-line interface as specified in section 7
// of the design specification. It handles parsing flags and initializing the
// restore operation.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
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

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// run implements the main restore command as specified in section 7.
// It parses flags, validates configuration, and initializes the restore operation.
func run() error {
	// Create a new flag set for the restore command as specified in section 7
	fs := flag.NewFlagSet("restore", flag.ExitOnError)

	// Required flags as specified in section 4.1
	tableName := fs.String("table", "", "DynamoDB table name to restore to")
	exportS3URI := fs.String("export", "", "S3 URI of the PITR export (s3://bucket/prefix)")

	// Optional flags as specified in section 4.1
	exportType := fs.String("type", "FULL", "Export type (FULL|INCREMENTAL)")
	viewType := fs.String("view", "NEW", "View type (NEW|NEW_AND_OLD)")
	region := fs.String("region", "", "AWS region (defaults to AWS_REGION env)")
	resumeKey := fs.String("resume", "", "S3 URI for checkpoint file")
	maxWorkers := fs.Int("workers", 10, "Maximum number of concurrent workers")
	batchSize := fs.Int("batch", 25, "Batch size for DynamoDB writes (max 25)")
	reportS3URI := fs.String("report", "", "S3 URI for the final report")
	dryRun := fs.Bool("dry-run", false, "Read and measure the whole export without writing to the table")
	shutdownTimeout := fs.Duration("shutdown-timeout", 5*time.Minute, "Graceful shutdown timeout")
	showVersion := fs.Bool("version", false, "Print the build identity and exit")

	// Parse flags as specified in section 7
	if err := fs.Parse(os.Args[1:]); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	// Asked before anything is validated, so the version is readable without a
	// complete configuration.
	if *showVersion {
		fmt.Printf("ddb-pitr %s (commit %s, built %s)\n", version, commit, date)
		return nil
	}

	// Create and validate configuration as specified in section 4.1
	cfg := &config.Config{
		TableName:       *tableName,
		ExportS3URI:     *exportS3URI,
		ExportType:      *exportType,
		ViewType:        *viewType,
		Region:          *region,
		ResumeKey:       *resumeKey,
		MaxWorkers:      *maxWorkers,
		BatchSize:       *batchSize,
		ReportS3URI:     *reportS3URI,
		DryRun:          *dryRun,
		ShutdownTimeout: *shutdownTimeout,
	}

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Load AWS configuration as specified in section 3
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.Region),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
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

	// Create context with graceful shutdown handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
		return fmt.Errorf("restore operation failed: %w", err)
	}

	if cfg.DryRun {
		fmt.Println("Dry run completed successfully; nothing was written")
	} else {
		fmt.Println("Restore operation completed successfully")
	}
	return nil
}
