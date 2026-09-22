# ddb-pitr

Restore DynamoDB tables from PITR exports stored on S3.

AWS DynamoDB Point-in-Time Recovery can export table data to S3, but provides no native way to restore that export to a different table. This tool fills that gap by reading PITR exports from S3 and writing items to any DynamoDB table, enabling cross-account restores, cross-region migrations, or selective data recovery.

## Features

- Stream multi-terabyte exports without loading into memory
- Writes spread across the whole target table rather than the few partitions the files
  in flight happen to belong to
- Write rate that follows the table's capacity, so a small table restores at its
  capacity and a large one is not held back
- Resumable by default: an interrupted restore picks up where it stopped, at any worker count, without having been asked to
- Every data file checked against the manifest before the first write, including exports that were copied to another bucket
- Automatic throttling handling
- Live progress reporting what has been restored, with failures printed as they happen
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

# Record progress somewhere other than the export's bucket, for an export you may only read
ddb-pitr --table my-table --export s3://source-bucket/AWSDynamoDB/01234567890-abcdef/ --resume s3://my-bucket/checkpoints/restore-001.json

# High-throughput restore for a large export
ddb-pitr --table my-table --export s3://my-bucket/AWSDynamoDB/01234567890-abcdef/ --readers 200 --workers 50

# Cross-region restore, keeping the report
ddb-pitr --table my-table-replica --export s3://source-bucket/AWSDynamoDB/01234567890-abcdef/manifest-summary.json --region eu-west-1 --report s3://dest-bucket/reports/restore-001.json
```

Exit status is 0 when every record in the export was applied, 3 when the restore
finished but skipped records it could not read (the first few are named on stderr with
their file and offset, and the report records the total), and 1 when the restore did
not finish. Running a restore that exited 3 again does not change its outcome, and with
`--resume` it exits 3 again: those records are gone for good, and the count follows the
restore rather than the run that found them.

## Configuration

### Required Flags

- `--table`: the DynamoDB table to write into. It must already exist with the same key schema as the exported table.
- `--export`: where the export is. The S3 URI of its `manifest-summary.json`, or of the directory holding it.

### Optional Flags

- `--region`: AWS region. Left out, the region comes from your AWS environment or profile the same way the AWS CLI resolves it, and the restore fails early if nothing resolves.
- `--resume`: S3 URI of the object progress is recorded in, naming a bucket and a key. Left out, progress goes to a key in the export's own bucket. See [Checkpoint and resume](#checkpoint-and-resume).
- `--no-resume`: record no progress at all, so an interrupted restore starts over. For an export in a bucket you may read but not write.
- `--readers`: how many of the export's data files are read at once (default 50). This is how widely the restore's writes are spread over the target table. See [Using the whole table](#using-the-whole-table).
- `--workers`: how many writes to the target table are in flight at once (default 10).
- `--batch`: the largest number of items the restore will put in one DynamoDB write (max and default 25). It is an upper bound, not a fixed size: a table that cannot accept that many gets smaller writes without being asked.
- `--report`: S3 URI for the final report, naming a bucket and a key. It is printed to stdout either way.
- `--dry-run`: read, decode and measure the whole export without writing to the table and without recording a checkpoint, so a later real restore cannot skip work that was only measured.
- `--shutdown-timeout`: how long an interrupted restore has to record where it stopped (default 5m). Writes already in flight when the interruption arrives are abandoned rather than finished; a resume redoes them.
- `--version`: print the build identity and exit.

## Checkpoint and resume

### What a resume guarantees

A restore that is interrupted, whether by Ctrl-C, a container runtime's SIGTERM, a lost
network or the machine going away, can be run again with the same flags and will finish
the export. Nothing in the export is dropped by the interruption: work that was done is
not repeated, and work that was not done is picked up.

Three things hold across the interruption.

- **No record is lost.** Every record the export contains ends up applied, whichever run
  applied it.
- **A few records are applied twice.** The batches in flight when the interruption
  arrived are abandoned and redone. That is safe: every write replaces or removes a whole
  item, so applying it twice leaves the table exactly as applying it once does.
- **The shape of the second run is yours to choose.** `--readers`, `--workers`, `--batch`
  and even the machine can all differ from the first run. Progress is recorded per data
  file, not per worker, so nothing about how the first run was spread out is baked into it.

### Where progress is recorded

Progress goes to one small JSON object in S3. Left to itself the restore puts it in the
export's own bucket, so a restore is resumable without anyone having thought about it:

```
s3://<export bucket>/ddb-pitr/checkpoints/<export id>.<table>.json
```

It sits outside the export's own directory, which stays exactly as DynamoDB wrote it. The
key names both the export and the target table, because one export restored into two
tables is two restores; sharing a checkpoint between them would let the second skip what
the first finished and call a half-filled table done.

Two flags change this.

- `--resume s3://bucket/key` puts the checkpoint where you say instead. Use it when the
  export is in a bucket you may read but not write, which is the usual shape of a
  cross-account restore.
- `--no-resume` records nothing. An interrupted restore then starts over from the
  beginning of the export.

A `--dry-run` never records progress, whatever else was asked for, so a later real
restore cannot skip work that was only measured.

> **Upgrading.** Earlier versions wrote nothing unless `--resume` said where, so an
> invocation that worked before now needs `s3:PutObject` on the export's bucket and will
> stop at the start without it. If the credentials you restore with may only read that
> bucket, add `--resume` pointing somewhere writable, or `--no-resume` to keep the old
> behaviour of starting over after an interruption.

### Starting a restore

Having read the export's manifest and any checkpoint already there, the restore writes
the checkpoint once before it reads a single data file or writes a single item. That
settles two questions while the target table is still untouched: whether the checkpoint
can be written at all, and whether another restore already owns it. A restore that cannot
record its progress stops there rather than discovering it hours in, with the table
part-way filled.

So a restore into a read-only export bucket fails immediately, and the fix is to name a
writable location with `--resume` or to accept `--no-resume`.

### One checkpoint, one restore

A checkpoint belongs to one export and to one running restore.

Pointing a different export at an existing checkpoint is refused rather than resumed,
since file names repeat across exports and a resume would skip files by name collision.

Two restores sharing one checkpoint is caught too: the second is stopped as soon as it
tries to record progress. **Do not delete the checkpoint in response.** Deleting it makes
both restores start over. Let the stopped one stay stopped, or give it its own `--resume`
URI.

### Records that cannot be read

A line the restore cannot decode is skipped rather than failing the run, and the restore
exits 3. Those records are gone for good: a resume does not read the files they were in
again, so running the restore a second time cannot recover them.

The count follows the restore rather than the run that found it. A resumed restore of an
export that lost records still exits 3 and still names the count in its report, even
though this run read past nothing. Re-running to be sure will not turn a 3 into a 0.

The first twenty skipped lines are named on stderr with their file and offset, which is
what you need to go and look at them in the export.

### What a resume does not do

It does not make a restore safe to run twice against a table something else is writing
to, and it does not order anything. See [Verification](#verification).

## Using the whole table

A restore of a large export is usually limited by the target table, and the limit is
rarely the table's total capacity. DynamoDB divides a table into partitions and gives
each its own share; an export is written one file per source partition, so everything in
one file belongs to one narrow slice of the key space. A restore that writes a file at a
time is a restore aimed at one partition at a time, and it throttles there while the
rest of the table sits idle.

So the restore reads many files at once and builds each write from all of them. Writing
throughput then follows the size of the table rather than the number of files in flight,
and adding capacity to the table speeds the restore up. `--readers` is the setting that
decides how wide the spread is; `--workers` decides how many writes are in flight.

Reading a file costs memory while it is open, so `--readers` is bounded by the machine
rather than by the table. Around 7 MB per file is a safe estimate.

> **Upgrading.** `--workers` used to set how many files were read *and* written at once;
> it now sets writes alone, and `--readers` sets reading. A restore therefore holds more
> memory than it used to: roughly 350 MB at the default of 50 readers, against about
> 70 MB before. Lower `--readers` where that matters, such as a container with a small
> memory limit. An invocation that passed `--workers 50` for throughput no longer needs
> to: the default spread is what makes a restore fast, and `--workers` is now only how
> many writes are in flight. The progress line has also changed shape, so anything
> reading it needs updating.

Two consequences worth knowing. Items are written in no particular order, which is
already true of any export: an export holds one record per key, so nothing within it
depends on order. And a restore now holds decoded items in memory between reading and
writing, bounded by `--workers` and `--batch`; an interruption abandons them and a
resume rewrites them, which is safe for the reasons in
[What a resume guarantees](#what-a-resume-guarantees).

## What a restore reports

While it runs, a restore rewrites one line in place:

```
Progress: 12.5% | 1234 items in 81 batches | 5/12 files | 345/s, 6.8 MB/s | 9 readers | 7 writers | pace 55 WCU/s | 11 throttles | 22 retries | 33 lost | 44 errors
```

What has gone right comes first. Items written and files finished are what say the
restore is working, and both are absolute rather than a share of an estimate, so they
can be checked against the table and against the export. The file count covers the whole
restore, so a resumed run reports the files an earlier run finished as done.

The two pool counts are the readers holding a data file and the writers holding a batch,
which is what says which end is the limit. Writers idle means the export is not being
read fast enough; raise `--readers`. Readers idle means the table is not accepting writes
fast enough; raise the table's capacity. The reader count cannot exceed the number of
data files the export holds, so a small export reads narrowly however many readers it
was given.

Anything that goes wrong is printed as it happens, above the progress line, naming the
reader or writer that hit it. A restore that retries past a failure therefore still says
what it survived, rather than finishing with a count and nothing to explain it. Lines
that could not be decoded are named the same way, with the file and offset needed to go
and look at them; the first twenty are named and the rest are counted. An interruption
is not printed as a failure, since every reader and writer reports it at once and the
reason for the stop is reported on its own. An interrupted restore says how many data
files it finished and that running the same command again carries on from there.

## Keeping pace with the table

Until the table refuses something, the restore writes as fast as it can. When the table
does refuse, the restore learns the rate it will accept and sizes its writes to fit,
rather than resending a batch the table has already said is too big.

This matters most on a table with little provisioned capacity, where a write of
twenty-five items can never be accepted whole. Restoring into one of those used to mean
finding a `--batch` and `--workers` small enough by hand, and a restore that spent its
time collecting refusals until you did. It now runs at the table's capacity as it
stands.

The rate is not treated as fixed. A restore climbs back up when capacity returns, so
autoscaling or a partition split during a long restore is picked up within seconds
rather than needing the restore to be started again. The progress line reports the rate
the restore has settled on, and reports `pace -` while nothing is limiting it.

Sustained throttling is therefore not something to tune away. If a restore is slower
than you need, raise the table's capacity; the restore will use it.

## Verification

Before the first write, every data file the export's manifest lists is checked against
the copy the restore is going to read. This catches an export that is not what its
manifest describes: a copy that is still syncing, one that is missing files, or an object
that has been replaced since the export was taken. A file that does not match fails the
restore while the target table is still untouched.

An export copied to another bucket or account is verified by content, so copying does
not stand in the way of restoring. Reading a large copied file to verify it costs as
much as reading it to restore it. A resumed restore verifies only the files it has
left to do.

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
- `writer`: Writing operations to DynamoDB at the rate the table accepts
- `checkpoint`: Saving and loading progress
- `metrics`: Collecting counters and histograms
- `coordinator`: Reader and writer pools, and the progress between them
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
introduced the figure, the library packages cover 97% of statements, a defect can be placed
somewhere a test reaches 98% of the time, and the tests notice 83% of the defects placed
there. The survivors are mostly defects no test can observe: a lock released by the
function returning rather than by the deferred call that was removed, preallocation hints,
buffering a channel nothing depends on the synchronisation of, tuning constants such as how
often progress is saved, and the wording of messages. The `cmd` packages are only lightly
covered: they wire the others together.

```bash
mutest -packages writer            # one package
mutest -results mutants.json       # every package
```

### Linting

```bash
golangci-lint run --config ./.golangci.yml ./...
golangci-lint config verify
```

CI verifies the configuration against its schema before linting, on every push and pull
request. That is stricter than `run` alone, which quietly ignores configuration keys it
does not know.

`gocyclo` is set to its default threshold of 30. The stricter 15 the config used to name
was never in effect, and six functions sit above it: `Coordinator.Run`,
`Coordinator.readFiles`, `Coordinator.writeItems`, `Config.Validate`,
`JSONDecoder.Decode` and `generateRandomItem`. Tightening it means splitting those first.

## Licence

ddb-pitr is licensed under the GNU Affero General Public License, version 3 or later,
with a commercial licence available. See `LICENSE.md`.
