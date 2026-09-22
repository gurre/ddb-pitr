# Integration Tests

These tests drive the real streamer, decoder, writer and coordinator over the export
fixtures under `../s3exportdata`, against in-memory stand-ins for S3 and DynamoDB. They
are where behaviour that only appears when the real components meet is checked: gzip
offsets a checkpoint can be resumed from, an interrupted restore finishing on a second
run, and an export read many files at a time landing the same table as one read a file
at a time.

## Running

```bash
go test -race ./integration/
```

## Stand-ins

- `mock.S3Client` serves the export fixtures from disk, including the gzipped data files.
- `mock.DynamoDBClient` applies batch writes to an in-memory table and records every call.

Neither simulates throttling or capacity. Those belong to the writer's own tests, which
can drive a clock; see `../writer`.

## Adding tests

A test belongs here when it needs two or more real components together. One that needs
only the coordinator's own logic belongs in `../coordinator`, where the doubles make the
scenario explicit. State the behaviour in the test name and say in a comment what would
break without it.
