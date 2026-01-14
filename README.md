# ddb-pitr

A tool for restoring DynamoDB tables from Point-in-Time Recovery (PITR) exports in S3.

## Features

- Restore DynamoDB tables from PITR exports in S3 (JSON format)
- Handle multi-terabyte exports efficiently
- Run locally (EC2, developer PC)
- Resume safely on interruption; avoid duplicates or data loss
- Emit JSON report to stdout and upload to S3
- Generate least-privilege IAM policies and verify permissions

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
# Restore a table from a PITR export
ddb-pitr restore \
  --table my-table \
  --export s3://my-bucket/export/ \
  --region us-west-2 \
  --workers 10 \
  --batch-size 25

# Generate IAM policy
ddb-pitr policy \
  --table my-table \
  --bucket my-bucket

# Validate configuration
ddb-pitr validate \
  --table my-table \
  --export s3://my-bucket/export/
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
- `--batch-size`: Batch size for DynamoDB writes (max 25, default: 25)
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
- `iam`: Generating policies and checking permissions

External dependencies:
- `github.com/gurre/s3streamer`: Streaming gzipped JSON lines from S3

## Development

### Prerequisites

- Go 1.21 or later
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
go vet ./...
```