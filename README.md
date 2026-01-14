# ddb-pitr

Restore DynamoDB tables from PITR exports stored on S3.

AWS DynamoDB Point-in-Time Recovery can export table data to S3, but provides no native way to restore that export to a different table. This tool fills that gap by reading PITR exports from S3 and writing items to any DynamoDB table, enabling cross-account restores, cross-region migrations, or selective data recovery.

## Features

- Stream multi-terabyte exports without loading into memory
- Parallel workers with configurable concurrency
- Checkpoint to S3 for safe resume after interruption
- Dry-run mode for validation before restore

## Supported Operations

- **FULL exports**: All items restored via PUT (replaces entire item)
- **INCREMENTAL exports**:
  - PUT: Supported (new items or full item replacement)
  - DELETE: Supported (removes items by key)
  - UPDATE: Not currently supported (partial modifications)

For INCREMENTAL exports containing UPDATE operations, consider using FULL export followed by INCREMENTAL exports where the incremental only contains PUT/DELETE operations.

## Installation

```bash
go install github.com/gurre/ddb-pitr@latest
```

## Usage

```bash
# Basic restore
ddb-pitr restore \
  --table my-table \
  --export s3://my-bucket/AWSDynamoDB/01234567890-abcdef/ \
  --region us-west-2

# Validate export before restoring (no writes)
ddb-pitr restore \
  --table my-table \
  --export s3://my-bucket/AWSDynamoDB/01234567890-abcdef/ \
  --region us-west-2 \
  --dry-run

# Resumable restore with S3 checkpoint (safe to interrupt and restart)
ddb-pitr restore \
  --table my-table \
  --export s3://my-bucket/AWSDynamoDB/01234567890-abcdef/ \
  --region us-west-2 \
  --resume s3://my-bucket/checkpoints/restore-001.json

# High-throughput restore for large exports
ddb-pitr restore \
  --table my-table \
  --export s3://my-bucket/AWSDynamoDB/01234567890-abcdef/ \
  --region us-west-2 \
  --workers 50 \
  --batch 25 \
  --manage-capacity

# Restore incremental export (applies PUT and DELETE operations)
ddb-pitr restore \
  --table my-table \
  --export s3://my-bucket/AWSDynamoDB/01234567890-incr/ \
  --region us-west-2 \
  --type INCREMENTAL

# Cross-region restore with report
ddb-pitr restore \
  --table my-table-replica \
  --export s3://source-bucket/AWSDynamoDB/01234567890-abcdef/ \
  --region eu-west-1 \
  --report s3://dest-bucket/reports/restore-001.json
```

## Configuration

### Required Flags

- `--table`: DynamoDB table name to restore to
- `--export`: S3 URI of the PITR export (s3://bucket/prefix)

### Optional Flags

- `--type`: Export type (FULL|INCREMENTAL, default: FULL)
- `--view`: View type (NEW|NEW_AND_OLD, default: NEW)
- `--region`: AWS region (defaults to AWS_REGION env)
- `--resume`: S3 URI for checkpoint file
- `--workers`: Maximum number of concurrent workers (default: 10)
- `--read-ahead`: Number of S3 parts to read ahead (default: 5)
- `--batch`: Batch size for DynamoDB writes (max 25, default: 25)
- `--report`: S3 URI for the final report
- `--dry-run`: Validate configuration without restoring
- `--manage-capacity`: Automatically manage table capacity
- `--shutdown-timeout`: Graceful shutdown timeout (default: 5m)

## Architecture

The tool is organized into several packages:

- `cmd`: Command-line interface
- `config`: Configuration parsing and validation
- `manifest`: Loading and verifying manifest files
- `itemimage`: Decoding JSON into DynamoDB operations
- `writer`: Writing operations to DynamoDB
- `checkpoint`: Saving and loading progress
- `metrics`: Collecting counters and histograms
- `coordinator`: Worker pool orchestration
- `aws`: AWS service abstractions

External dependencies:
- `github.com/gurre/s3streamer`: Streaming gzipped JSON lines from S3

## Development

### Prerequisites

- Go 1.24 or later
- AWS credentials configured

### Building

```bash
go build
```

### Testing

```bash
go test ./...
```

### Linting

```bash
golangci-lint run ./...
```