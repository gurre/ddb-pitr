// Package aws implements the AWS service abstractions as specified in section 3
// of the design specification. It provides interfaces and implementations for
// all required AWS services.
package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// DynamoDBClient defines the interface for DynamoDB operations as required by section 4.6.
// It provides methods for batch writing and updating items.
type DynamoDBClient interface {
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

// S3Client defines the interface for S3 operations as required by sections 4.3 and 4.4.
// It provides methods for reading manifest files and data files.
type S3Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// IAMClient defines the interface for IAM operations as required by section 4.2.
// It provides methods for simulating permissions.
type IAMClient interface {
	SimulatePrincipalPolicy(ctx context.Context, params *iam.SimulatePrincipalPolicyInput, optFns ...func(*iam.Options)) (*iam.SimulatePrincipalPolicyOutput, error)
}

// Compile-time interface checks to ensure implementations satisfy interfaces
var (
	_ DynamoDBClient = (*DynamoDBClientImpl)(nil)
	_ S3Client       = (*S3ClientImpl)(nil)
	_ IAMClient      = (*IAMClientImpl)(nil)

	// AWS SDK interface checks to ensure SDK clients satisfy interfaces
	_ DynamoDBClient = (*dynamodb.Client)(nil)
	_ S3Client       = (*s3.Client)(nil)
	_ IAMClient      = (*iam.Client)(nil)
)
