# ddb-pitr

Restore DynamoDB tables from PITR exports stored on S3.

AWS DynamoDB Point-in-Time Recovery can export table data to S3, but provides no native way to restore that export to a different table. This tool fills that gap by reading PITR exports from S3 and writing items to any DynamoDB table, enabling cross-account restores, cross-region migrations, or selective data recovery.

## Features

- Stream multi-terabyte exports without loading into memory
- Writes spread across the whole target table rather than the few partitions the files
  in flight happen to belong to
- Write rate that follows the table's capacity, so a small table restores at its
  capacity and a large one is not held back, with nothing to tune
- A partition the table cannot keep up with slows only the items bound for it, not the
  rest of the table
- One restore per table at a time: a second restore of a table, recording its progress
  in the same bucket, stops before it writes anything
- Resumable by default: an interrupted restore picks up where it stopped, however the second run is shaped, without having been asked to
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

# High-throughput restore for a large export. 200 files open at once is around 1.4 GB;
# see "Using the whole table" for what reading widely costs.
ddb-pitr --table my-table --export s3://my-bucket/AWSDynamoDB/01234567890-abcdef/ --readers 200

# Cross-region restore, keeping the report
ddb-pitr --table my-table-replica --export s3://source-bucket/AWSDynamoDB/01234567890-abcdef/manifest-summary.json --region eu-west-1 --report s3://dest-bucket/reports/restore-001.json
```

Exit status is 0 when every record in the export was applied, 3 when the restore
finished but skipped records it could not read (the first few are named on stderr with
their file and offset, and the report records the total), 4 when it did not start
because another restore is writing the same table (see
[One restore per table](#one-restore-per-table)), and 1 when the restore did not
finish. Running a restore that exited 3 again does not change its outcome, and with
`--resume` it exits 3 again: those records are gone for good, and the count follows the
restore rather than the run that found them.

## Configuration

### Required Flags

- `--table`: the DynamoDB table to write into. It must already exist with the same key schema as the exported table.
- `--export`: where the export is. The S3 URI of its `manifest-summary.json`, or of the directory holding it.

### Optional Flags

- `--region`: AWS region. Left out, the region comes from your AWS environment or profile the same way the AWS CLI resolves it, and the restore fails early if nothing resolves.
- `--resume`: S3 URI of the object progress is recorded in, naming a bucket and a key. Left out, progress goes to a key in the export's own bucket. See [Checkpoint and resume](#checkpoint-and-resume).
- `--no-resume`: record no progress at all, so an interrupted restore starts over. The restore still claims the table in the export's bucket, so this needs that bucket to be writable; see [One restore per table](#one-restore-per-table).
- `--readers`: how many of the export's data files are read at once (default 50). This is how widely the restore's writes are spread over the target table. See [Using the whole table](#using-the-whole-table).
- `--max-in-flight`: the most writes to the table in flight at once (default 1024, at most 4096). The restore settles below this by itself; it holds here only when latency to the table is high enough to need more. Lower it to bound the memory and connections a restore may use, such as in a container with a small memory limit, at the cost of throughput when latency is high.
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
- **The shape of the second run is yours to choose.** `--readers` and even the machine
  can differ from the first run. Progress is recorded per data file, not per reader, so
  nothing about how the first run was spread out is baked into it.

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
- `--no-resume` records no progress. An interrupted restore then starts over from the
  beginning of the export. The table is still claimed, in the export's bucket; see
  [One restore per table](#one-restore-per-table).

A `--dry-run` never records progress, whatever else was asked for, so a later real
restore cannot skip work that was only measured.

> **Upgrading.** Earlier versions wrote nothing unless `--resume` said where, so an
> invocation that worked before now needs `s3:PutObject` on the export's bucket and will
> stop at the start without it. If the credentials you restore with may only read that
> bucket, add `--resume` pointing somewhere writable. `--no-resume` still starts over
> after an interruption, but no longer lets a restore run against a bucket it may only
> read, since the claim on the table needs somewhere to be written.

### Starting a restore

Before anything else, the restore claims the target table; see
[One restore per table](#one-restore-per-table). Having then read the export's manifest
and any checkpoint already there, the restore writes the checkpoint once before it reads
a single data file or writes a single item. That
settles two questions while the target table is still untouched: whether the checkpoint
can be written at all, and whether another restore already owns it. A restore that cannot
record its progress stops there rather than discovering it hours in, with the table
part-way filled.

So a restore into a read-only export bucket fails immediately, and the fix is to name a
writable location with `--resume`.

### One checkpoint, one restore

A checkpoint belongs to one export and to one running restore.

Pointing a different export at an existing checkpoint is refused rather than resumed,
since file names repeat across exports and a resume would skip files by name collision.

Two restores sharing one checkpoint is caught too, as a second line of defence behind the
claim on the table: the second is stopped as soon as it tries to record progress. **Do
not delete the checkpoint in response.** Deleting it makes both restores start over. Let
the stopped one stay stopped, or give it its own `--resume` URI.

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

It does not make a restore safe to run against a table something other than a restore
is writing to, and it does not order anything. See [Verification](#verification).

## One restore per table

Only one restore writes into a table at a time. Two restores of one table, whether the
same command started twice or two exports applied at once, would interleave their writes
in ways neither could see, and an older export could put back data a newer one replaced.

- **A second restore stops before it writes anything.** It names the host and process
  of the restore that holds the table, and exits 4. Wait for that restore to finish, or
  stop it.
- **A restore that dies does not hold the table for long.** If the restore holding a
  table crashes or its machine goes away, the next restore of the table notices within
  30 seconds and carries on. One that stopped cleanly, finished or interrupted, lets go
  as it exits, and the next one starts at once.
- **A restore that loses contact stops writing first.** A restore that cannot confirm
  its claim for 20 seconds stops writing, to the table and to its checkpoint, well
  before another could take the table over. It says so rather than calling itself
  interrupted. Once no other restore holds the table, running it again carries on from
  its last checkpoint, which is at most a few seconds behind.

The claim is a small object next to the checkpoints, at
`ddb-pitr/leases/<region>.<table>.json` in the bucket `--resume` names, or else in the
export's bucket. Restores that record progress in different buckets do not see each
other's claims, so point every restore of one table at the same bucket. A claim is
taken under `--no-resume` too, since not recording progress does not make two restores
of one table any safer; a restore therefore always needs one bucket it may write to. A
`--dry-run` writes nothing to the table and takes no claim.

The claim does not depend on the clocks of the machines involved agreeing. Machines that
are suspended outright, such as a laptop put to sleep mid-restore, stop counting time
while they sleep; do not run restores anywhere that happens.

> **Upgrading.** A restore now needs `s3:GetObject` and `s3:PutObject` on
> `ddb-pitr/leases/*` in the bucket it records progress in, and under `--no-resume` in
> the export's bucket. An invocation that used `--no-resume` against a read-only export
> bucket now stops at the start; drop `--no-resume` and point `--resume` at a bucket it
> may write to instead.

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
decides how wide the spread is. How many writes are in flight, and how large each is, the
restore works out for itself from how the table responds.

Reading a file costs memory while it is open, so `--readers` is bounded by the machine
rather than by the table. Around 7 MB per file is a safe estimate.

When the table cannot keep up with part of the key space, the files that slice belongs
to are slowed down while the rest carry on at the pace the table takes them. The items
it could not take are sent again later among other files' items, rather than all
together and straight back at the partition that refused them.

> **Upgrading.** `--workers` and `--batch` are gone, and a command line that passes
> either now stops with `flag provided but not defined`; drop them. How many writes are
> in flight and how many items each carries now follow the table, up to
> `--max-in-flight`: a table with capacity
> to spare gets as many writes as the machine can keep busy, and one that is short gets
> smaller and fewer. The progress line has changed shape too, so anything reading it
> needs updating.

Two consequences worth knowing. Items are written in no particular order, which is
already true of any export: an export holds one record per key, so nothing within it
depends on order. And a restore holds decoded items in memory between reading and
writing, as many as it has writes in flight and a little more; an interruption abandons
them and a resume rewrites them, which is safe for the reasons in
[What a resume guarantees](#what-a-resume-guarantees).

## What a restore reports

While it runs, a restore rewrites one line in place:

```
Progress: 12.5% | 1234 items (put 1200 update 30 delete 4) in 81 batches | 5/12 files | 345/s, 6.8 MB/s | 9 readers | 7/40 writes in flight | 12 held back | pace 55 WCU/s | 11 throttles | 22 retries | 33 lost | 44 errors
```

What has gone right comes first. Items written and files finished are what say the
restore is working, and both are absolute rather than a share of an estimate, so they
can be checked against the table and against the export. The items are broken down by
kind, inserts, changes and deletes, so an incremental export can be checked against what
it holds kind by kind; a full export shows only puts. The file count covers the whole
restore, so a resumed run reports the files an earlier run finished as done.

The readers holding a data file and the writes in flight, against how many the restore
is allowing itself, are what say which end is the limit.

How many writes are allowed follows what the restore has been using, once a second: twice
what was in flight, less where more stopped paying. When every write starts taking longer,
more are allowed, so the rate holds. A jump in latency lasting only a second or two costs
throughput while it lasts, since it is over before the restore has caught up with it, and
leaves nothing behind. The restore never allows more than `--max-in-flight`, 1024 unless
set; a restore showing every allowed write in flight, such as `1024/1024`, with the table
accepting everything is held back by the latency between it and the table. It runs
faster from a machine in the table's region, or with a higher `--max-in-flight` where
the machine has the memory and connections to spare. It goes no higher than 4096, so one
restore cannot flood a table, or the network, that others use too.

Writes in flight well below what is allowed means the writes are waiting for items, so
reading is the limit. Raising `--readers` helps only while the export still has files
nobody has started: the count can never exceed the number of data files the export
holds, so a small export reads narrowly however many readers it was given.

Fewer readers than the export has files left to read means they are blocked handing items
over, so the table is the limit; raise its capacity. A reader blocked that way is counted
as working for ten seconds before it drops out of the count, so a brief stall does not
show here at all.

Items held back are ones the table refused or could not take yet, waiting to be sent
again. A number that stays high points at part of the table that cannot keep up; they
are not lost, and the uploaded report counts, by kind, how often the table pushed back.

Anything that goes wrong is printed as it happens, above the progress line, naming the
reader, or the writer, that hit it. A restore that retries past a failure therefore still
says what it survived, rather than finishing with a count and nothing to explain it.
Lines that could not be decoded are named the same way, with the file and offset needed
to go and look at them; the first twenty are named and the rest are counted. An
interruption is not printed as a failure, since every reader reports it at once and the
reason for the stop is reported on its own. An interrupted restore says how many data
files it finished and that running the same command again carries on from there; one
that stopped because it lost its claim on the table says that instead, since another
restore may now be writing it. A data file whose items the table keeps turning away is
named once, so a part of the table that never keeps up shows where it is.

The report at the end names how many of each kind the table took. The one `--report`
uploads carries the whole breakdown: for each of put, update and delete, how many the
table took, how many times it pushed one back to be sent again, and how many were lost.

## Keeping pace with the table

Until the table turns something away, the restore writes as fast as it can. When it
does, the restore learns the rate the table will accept and sizes its writes to fit,
rather than resending a batch the table has already said is too big. Each write then
waits until the rate pays for a full one, or for a second's worth on a table too small
for that, rather than going out with whatever one or two items it could pay for.

This matters most on a table with little provisioned capacity, where a write of
twenty-five items can never be accepted whole. Such a table restores at its capacity as
it stands, with nothing to tune.

What the table turns away is weighed a second at a time, whether it refused whole
writes or handed back part of them: the rate falls to the share the table took, and by
no more than three tenths in any second. A small share turned away, as one busy
partition among many produces, does not lower it at all: that is the partition's
shortage, not the table's, and slowing every other partition to its pace would throw
most of the table's capacity away. The data files that partition's items come from are
slowed instead.

The rate is not treated as fixed. A restore climbs back to where it was within seconds
of the table taking everything again, then keeps probing above it, so autoscaling or a
partition split during a long restore is picked up rather than needing the restore to be
started again. The progress line reports the rate the restore has settled on, and
reports `pace -` while nothing is limiting it.

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

- `cmd/ddb-pitr`: Command-line interface. `cmd/ddb-datagen` is not released; it fills
  tables for the end-to-end check
- `config`: Configuration parsing and validation
- `manifest`: Loading and verifying manifest files
- `itemimage`: Decoding JSON into DynamoDB operations
- `writer`: Writing operations to DynamoDB at the rate and concurrency the table rewards
- `checkpoint`: Saving and loading progress
- `lease`: Claiming the target table for one restore at a time
- `metrics`: Collecting counters and rendering the report
- `coordinator`: The readers, the batches between them and the table, and the progress of both
- `aws`: AWS service abstractions

External dependencies:
- `github.com/gurre/s3streamer`: Streaming gzipped JSON lines from S3

## Development

### Prerequisites

- Go 1.25 or later
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
was never in effect, and eight functions sit above it: `Coordinator.Run`,
`Coordinator.readFiles`, `Coordinator.writeItems`, `lease.Acquire`, the writer's
`sameValue`, `JSONDecoder.Decode`, the command's own `run` and `generateRandomItem`. Tightening it
means splitting those first.

## Licence

ddb-pitr is licensed under the GNU Affero General Public License, version 3 or later,
with a commercial licence available. See `LICENSE.md`.
