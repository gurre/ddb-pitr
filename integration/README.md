# Integration Tests

This directory contains integration tests for the DynamoDB PITR application. These tests use mock AWS clients to verify that all components of the application work together correctly.

## Test Structure

The integration tests cover the following workflows:

1. **Full Integration Flow** (`TestFullIntegrationFlow`): Tests the complete process of:
   - Loading a manifest from S3
   - Streaming data files 
   - Parsing JSON records
   - Writing records to DynamoDB

2. **Error Handling Tests**:
   - `TestS3ErrorHandling`: Tests error handling for S3 operations
   - `TestDynamoDBErrorHandling`: Tests error handling for DynamoDB operations

## Mock Clients

The tests use mock implementations of the AWS clients:

- `mock.S3Client`: Simulates S3 operations using files from the testdata directory
- `mock.DynamoDBClient`: Simulates DynamoDB operations with in-memory storage

## Running the Tests

To run the integration tests:

```bash
go test -v ./integration
```

For running with test coverage:

```bash
go test -v -coverprofile=integration-coverage.out ./integration
go tool cover -html=integration-coverage.out
```

## Test Data

The tests use the sample data in the `testdata` directory, which contains real DynamoDB PITR export files. 

## Adding New Tests

When adding new tests:

1. Use the existing mock clients or extend them as needed
2. Consider testing failure scenarios as well as success paths
3. Verify all assertions after operations complete 