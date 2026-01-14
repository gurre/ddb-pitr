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
	"github.com/gurre/ddb-pitr/writer"
	"github.com/gurre/s3streamer"
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
	readAheadParts := fs.Int("read-ahead", 5, "Number of S3 parts to read ahead")
	batchSize := fs.Int("batch", 25, "Batch size for DynamoDB writes (max 25)")
	reportS3URI := fs.String("report", "", "S3 URI for the final report")
	dryRun := fs.Bool("dry-run", false, "Validate configuration without restoring")
	manageCapacity := fs.Bool("manage-capacity", false, "Automatically manage table capacity")
	shutdownTimeout := fs.Duration("shutdown-timeout", 5*time.Minute, "Graceful shutdown timeout")

	// Parse flags as specified in section 7
	if err := fs.Parse(os.Args[1:]); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
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
		ReadAheadParts:  *readAheadParts,
		BatchSize:       *batchSize,
		ReportS3URI:     *reportS3URI,
		DryRun:          *dryRun,
		ManageCapacity:  *manageCapacity,
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

	// Initialize AWS clients as specified in section 3
	dynamoClient := aws.NewDynamoDBClient(dynamodb.NewFromConfig(awsCfg))
	rawS3Client := s3.NewFromConfig(awsCfg)
	s3Client := aws.NewS3Client(rawS3Client)

	// Create context with graceful shutdown handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create and initialize required components for the coordinator
	manifestLoader := manifest.NewS3Loader(s3Client)
	streamer := s3streamer.NewS3Streamer(rawS3Client)
	jsonDecoder := itemimage.NewJSONDecoder()
	ddbWriter := writer.NewDynamoDBWriter(dynamoClient, cfg.TableName, cfg.BatchSize)

	// Set up the checkpoint store based on ResumeKey
	var checkpointStore checkpoint.Store
	if cfg.ResumeKey != "" {
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

	// Create the coordinator with all dependencies
	coord := coordinator.NewCoordinator(
		cfg,
		manifestLoader,
		streamer,
		jsonDecoder,
		ddbWriter,
		checkpointStore,
	)

	// Run the coordinator
	fmt.Printf("Starting restore of table %s from %s\n", cfg.TableName, cfg.ExportS3URI)
	if err := coord.Run(ctx); err != nil {
		return fmt.Errorf("restore operation failed: %w", err)
	}

	fmt.Println("Restore operation completed successfully")
	return nil
}
