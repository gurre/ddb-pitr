// Package aws implements the AWS service abstractions as specified in section 3
// of the design specification. This file contains the concrete implementations
// of the service interfaces.
package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// DynamoDBClientImpl implements DynamoDBClient using the AWS SDK as specified in section 4.6.
// It provides concrete implementations for batch writing and updating items.
type DynamoDBClientImpl struct {
	client *dynamodb.Client
}

// NewDynamoDBClient creates a new DynamoDBClientImpl instance
func NewDynamoDBClient(client *dynamodb.Client) *DynamoDBClientImpl {
	return &DynamoDBClientImpl{client: client}
}

// BatchWriteItem implements the DynamoDBClient interface for batch writing items
func (c *DynamoDBClientImpl) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	return c.client.BatchWriteItem(ctx, params, optFns...)
}

// UpdateItem implements the DynamoDBClient interface for updating individual items
func (c *DynamoDBClientImpl) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return c.client.UpdateItem(ctx, params, optFns...)
}

// S3ClientImpl implements S3Client using the AWS SDK as specified in sections 4.3 and 4.4.
// It provides concrete implementations for reading manifest files and data files.
type S3ClientImpl struct {
	client *s3.Client
}

// NewS3Client creates a new S3ClientImpl instance
func NewS3Client(client *s3.Client) *S3ClientImpl {
	return &S3ClientImpl{client: client}
}

// GetObject implements the S3Client interface for reading objects
func (c *S3ClientImpl) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return c.client.GetObject(ctx, params, optFns...)
}

// PutObject implements the S3Client interface for writing objects
func (c *S3ClientImpl) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return c.client.PutObject(ctx, params, optFns...)
}

// HeadObject implements the S3Client interface for retrieving object metadata
func (c *S3ClientImpl) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return c.client.HeadObject(ctx, params, optFns...)
}

// IAMClientImpl implements IAMClient using the AWS SDK as specified in section 4.2.
// It provides concrete implementations for simulating permissions.
type IAMClientImpl struct {
	client *iam.Client
}

// NewIAMClient creates a new IAMClientImpl instance
func NewIAMClient(client *iam.Client) *IAMClientImpl {
	return &IAMClientImpl{client: client}
}

// SimulatePrincipalPolicy implements the IAMClient interface for permission simulation
func (c *IAMClientImpl) SimulatePrincipalPolicy(ctx context.Context, params *iam.SimulatePrincipalPolicyInput, optFns ...func(*iam.Options)) (*iam.SimulatePrincipalPolicyOutput, error) {
	return c.client.SimulatePrincipalPolicy(ctx, params, optFns...)
}
