# ddb-pitr

Restore DynamoDB tables from PITR exports stored on S3.

AWS DynamoDB Point-in-Time Recovery can export table data to S3, but provides no native way to restore that export to a different table. This tool fills that gap by reading PITR exports from S3 and writing items to any DynamoDB table, enabling cross-account restores, cross-region migrations, or selective data recovery.

## Features

- Stream multi-terabyte exports without loading into memory
- Parallel workers with configurable concurrency
- Checkpoint to S3 so an interrupted restore resumes where it stopped, at any worker count
- Every data file checked against the manifest before the first write, including exports that were copied to another bucket
- Automatic throttling handling with exponential backoff
- Dry-run mode that reads and measures the whole export without writing

## Supported Operations

- **FULL exports**: every item is written, replacing whatever the table held under that key
- **INCREMENTAL exports**, taken with either view type:
  - Inserted items are written
  - Changed items are written as their new state, so the table ends up matching the export
  - Deleted items are removed

Full and incremental exports are recognised from the export itself; there is nothing to
tell the tool which kind it is.

## Installation

Download a build for your platform from the [releases page](https://github.com/gurre/ddb-pitr/releases),
verify it against the release's `checksums.txt`, and put `ddb-pitr` on your PATH.
Builds are published for Linux, macOS and Windows on x86-64, and for Linux and macOS on
ARM64. `ddb-pitr --version` reports which build you are running.

On macOS, Gatekeeper quarantines downloaded binaries; clear it with
`xattr -d com.apple.quarantine ddb-pitr`.

Or build from source:

```bash
go install github.com/gurre/ddb-pitr/cmd/ddb-pitr@latest
```

## Usage

`ddb-pitr` takes flags only; there is no subcommand. Point `--export` at the export's
`manifest-summary.json`, or at the directory containing it. Either is accepted.

```bash
# Basic restore
ddb-pitr --table my-table --export s3://my-bucket/AWSDynamoDB/01234567890-abcdef/

# Read and measure the whole export without writing anything to the table
ddb-pitr --table my-table --export s3://my-bucket/AWSDynamoDB/01234567890-abcdef/ --dry-run

# Resumable restore with an S3 checkpoint (safe to interrupt and restart)
ddb-pitr --table my-table --export s3://my-bucket/AWSDynamoDB/01234567890-abcdef/ --resume s3://my-bucket/checkpoints/restore-001.json

# High-throughput restore for a large export
ddb-pitr --table my-table --export s3://my-bucket/AWSDynamoDB/01234567890-abcdef/ --workers 50 --batch 25

# Cross-region restore, keeping the report
ddb-pitr --table my-table-replica --export s3://source-bucket/AWSDynamoDB/01234567890-abcdef/manifest-summary.json --region eu-west-1 --report s3://dest-bucket/reports/restore-001.json
```

Exit status is 0 when every record in the export was applied, 3 when the restore
finished but skipped records it could not read (the first few are named on stderr with
their file and offset, and the report records the total), and 1 when the restore did
not finish. Running a restore that exited 3 again does not change its outcome.

## Configuration

### Required Flags

- `--table`: the DynamoDB table to write into. It must already exist with the same key schema as the exported table.
- `--export`: where the export is. The S3 URI of its `manifest-summary.json`, or of the directory holding it.

### Optional Flags

- `--region`: AWS region. Left out, the region comes from your AWS environment or profile the same way the AWS CLI resolves it, and the restore fails early if nothing resolves.
- `--resume`: S3 URI where progress is recorded, which is what makes an interrupted restore resumable. Without it, progress is kept in memory and an interrupted restore starts over.
- `--workers`: how many files are read and written in parallel (default 10).
- `--batch`: how many items go in one DynamoDB write (max and default 25). Together with `--workers` this sets how hard the restore pushes the target table; lowering them is the remedy for sustained throttling.
- `--report`: S3 URI for the final report. It is printed to stdout either way.
- `--dry-run`: read, decode and measure the whole export without writing to the table and without recording a checkpoint, so a later real restore cannot skip work that was only measured.
- `--shutdown-timeout`: how long an interrupted restore has to finish its in-flight writes and record where it stopped (default 5m).
- `--version`: print the build identity and exit.

## Resuming an interrupted restore

A restore that is interrupted, whether by Ctrl-C, a container runtime's SIGTERM, a lost
network or the machine going away, can be restarted with the same `--resume` URI and
will finish the export. Nothing in the export is dropped by the interruption: work that
was done is not repeated, and work that was not done is picked up, whatever `--workers`
is set to on either run. A few items may be written a second time. That is safe, because
every write replaces or removes a whole item, so applying it twice leaves the table
exactly as applying it once does.

A checkpoint belongs to one export and to one running restore. Pointing a different
export at it is refused rather than resumed, since file names repeat across exports.
Starting a second restore against a checkpoint one is already using is refused as well;
do not delete the checkpoint in response, since both restores would then start over.

Without `--resume` progress is kept in memory only, and an interrupted restore starts
over. A `--dry-run` never writes a checkpoint.

## Verification

Before the first write, every data file the export's manifest lists is checked against
the copy the restore is going to read. This catches an export that is not what its
manifest describes: a copy that is still syncing, one that is missing files, or an object
that has been replaced since the export was taken. A file that does not match fails the
restore while the target table is still untouched.

An export copied to another bucket or account is verified by content, so copying does
not stand in the way of restoring.

Two things verification does not do. It does not check the target table afterwards;
compare item counts yourself if you need that. And it says nothing about ordering: the
restore applies what the export contains without regard to when each change was made.
Restore into a table nothing else is writing to, and apply incremental exports oldest
first. Applying one to a live table, or out of order, can replace newer data with older.

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
go build -o bin/ddb-pitr ./cmd/ddb-pitr
```

### Testing

```bash
go test -race ./...
```

`scripts/verify-pitr.sh` is the end-to-end check: it creates a table, exports it, restores
the export into a second table with `ddb-pitr`, and compares the two. It runs against a
real AWS account and costs real money and time; read its header first.

Test strength is measured with [mutest](https://github.com/gurre/mutest), which introduces one
small defect at a time and reports the ones no test notices. Measured at the commit that
introduced the figure, the tests reached 97% of the code and noticed 89% of the defects
introduced into what they reach. The survivors that remain are mostly defects no test can
observe: preallocation hints, equivalent boundary rewrites, and buffering a channel
nothing depends on the synchronisation of. The `cmd` packages are only lightly covered:
they wire the others together.

```bash
mutest -packages writer            # one package
mutest -results mutants.json       # every package
```

### Linting

```bash
golangci-lint run --config ./.golangci.yml ./...
golangci-lint config verify
```

CI runs `config verify` before `run` on every push and pull request. It is stricter than
`run`: it rejects configuration keys `run` quietly ignores.

`gocyclo` is set to its default threshold of 30. The stricter 15 the config used to name
was never in effect, and a few functions sit above it: `Coordinator.Run`,
`Coordinator.worker`, `DynamoDBWriter.writeRequests` and `generateRandomItem`.
Tightening it means splitting those first.

## Licence

ddb-pitr is licensed under the GNU Affero General Public License, version 3 or later,
with a commercial licence available. See `LICENSE.md`.
