// Package writer implements the DynamoDB writing functionality as specified in section 4.6
// of the design specification. It handles writing batches of operations to DynamoDB.
package writer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gurre/ddb-pitr/aws"
	"github.com/gurre/ddb-pitr/itemimage"
)

// Writer interface as defined in section 4.6 of the spec.
// Implementations must handle writing batches of operations to DynamoDB.
type Writer interface {
	WriteBatch(ctx context.Context, ops []itemimage.Operation) error
	Flush(ctx context.Context) error
}

// DynamoDBWriter implements the Writer interface using AWS DynamoDB as specified in section 4.6.
// It handles batching operations and retrying with exponential backoff.
type DynamoDBWriter struct {
	client    aws.DynamoDBClient
	tableName string
	batchSize int // Maximum number of operations per batch (≤25)
}

// NewDynamoDBWriter creates a new DynamoDBWriter instance with the specified batch size
func NewDynamoDBWriter(client aws.DynamoDBClient, tableName string, batchSize int) *DynamoDBWriter {
	return &DynamoDBWriter{
		client:    client,
		tableName: tableName,
		batchSize: batchSize,
	}
}

// WriteBatch implements the batch writing requirements from section 4.6.
// It splits operations into batches of size w.batchSize and writes them to DynamoDB.
// Handles Put and Delete operations via BatchWriteItem, and Update operations via UpdateItem.
//
// HOT PATH: Called for every batch of decoded items.
// Profiling shows ~13% CPU time with most overhead in:
//   - BatchWriteItem API calls (network latency)
//   - Retry backoff sleeps for throttling
//
// Performance notes:
//   - Batch size of 25 (DynamoDB max) minimizes API calls
//   - Put/Delete operations are batched; Update operations are individual API calls
//   - Exponential backoff handles DynamoDB throttling
func (w *DynamoDBWriter) WriteBatch(ctx context.Context, ops []itemimage.Operation) error {
	if len(ops) == 0 {
		return nil
	}

	// Split into batches of size w.batchSize
	for i := 0; i < len(ops); i += w.batchSize {
		end := i + w.batchSize
		if end > len(ops) {
			end = len(ops)
		}
		batch := ops[i:end]

		// Convert operations to DynamoDB requests
		requests := make([]types.WriteRequest, 0, len(batch))
		for _, op := range batch {
			switch op.Type {
			case itemimage.OpPut:
				requests = append(requests, types.WriteRequest{
					PutRequest: &types.PutRequest{
						Item: op.NewImage,
					},
				})
			case itemimage.OpDelete:
				requests = append(requests, types.WriteRequest{
					DeleteRequest: &types.DeleteRequest{
						Key: op.Keys,
					},
				})
			case itemimage.OpUpdate:
				// For updates, we need to use UpdateItem
				// This is handled separately since it can't be batched
				if err := w.updateItem(ctx, op); err != nil {
					return fmt.Errorf("failed to update item: %w", err)
				}
			}
		}

		if len(requests) == 0 {
			continue
		}

		// Write the batch
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				w.tableName: requests,
			},
		}

		// Retry with exponential backoff
		var lastErr error
		retriesExhausted := true
		for retries := 0; retries < 5; retries++ {
			output, err := w.client.BatchWriteItem(ctx, input)
			if err != nil {
				lastErr = err
				time.Sleep(time.Duration(1<<uint(retries)) * 100 * time.Millisecond)
				continue
			}

			// Handle unprocessed items
			if len(output.UnprocessedItems) > 0 {
				input.RequestItems = output.UnprocessedItems
				time.Sleep(time.Duration(1<<uint(retries)) * 100 * time.Millisecond)
				continue
			}

			retriesExhausted = false
			break
		}

		if retriesExhausted {
			if lastErr != nil {
				return fmt.Errorf("failed to write batch after retries: %w", lastErr)
			}
			return fmt.Errorf("failed to write batch: unprocessed items remain after retries")
		}
	}

	return nil
}

// Flush implements the flush requirements from section 4.6.
// Since we write immediately, this is a no-op.
func (w *DynamoDBWriter) Flush(ctx context.Context) error {
	// No-op since we write immediately
	return nil
}

// updateItem is a helper function that handles individual UpdateItem operations
// as required by section 4.6 for operations that can't be batched.
// It uses SET for new/modified attributes and REMOVE for deleted attributes.
func (w *DynamoDBWriter) updateItem(ctx context.Context, op itemimage.Operation) error {
	// Build update expression and attribute maps
	// Preallocate with estimated capacity based on typical item size
	setExpr := make([]string, 0, len(op.NewImage))
	removeExpr := make([]string, 0, len(op.OldImage))
	values := make(map[string]types.AttributeValue, len(op.NewImage))
	names := make(map[string]string, len(op.NewImage)+len(op.OldImage))

	// Track which attributes are being modified (exist in NewImage)
	modifiedAttrs := make(map[string]bool, len(op.NewImage))

	// Process NEW image for SET operations
	for k, v := range op.NewImage {
		// Skip if this is a key attribute (exists in Keys)
		if _, isKey := op.Keys[k]; isKey {
			continue
		}
		setExpr = append(setExpr, fmt.Sprintf("#%s = :%s", k, k))
		values[":"+k] = v
		names["#"+k] = k
		modifiedAttrs[k] = true
	}

	// Process OLD image for REMOVE operations
	// Attributes that exist in OldImage but not in NewImage should be removed
	for k := range op.OldImage {
		// Skip if this is a key attribute (exists in Keys)
		if _, isKey := op.Keys[k]; isKey {
			continue
		}
		if !modifiedAttrs[k] {
			// Attribute exists in OLD but not in NEW - remove it
			removeExpr = append(removeExpr, fmt.Sprintf("#%s", k))
			names["#"+k] = k
		}
	}

	if len(setExpr) == 0 && len(removeExpr) == 0 {
		return nil // No changes to make
	}

	// Build the final update expression combining SET and REMOVE clauses
	var updateExpr string
	if len(setExpr) > 0 {
		updateExpr = "SET " + strings.Join(setExpr, ", ")
	}
	if len(removeExpr) > 0 {
		if updateExpr != "" {
			updateExpr += " "
		}
		updateExpr += "REMOVE " + strings.Join(removeExpr, ", ")
	}

	input := &dynamodb.UpdateItemInput{
		TableName:                &w.tableName,
		Key:                      op.Keys,
		UpdateExpression:         &updateExpr,
		ExpressionAttributeNames: names,
	}

	// Only set ExpressionAttributeValues if we have SET expressions
	if len(values) > 0 {
		input.ExpressionAttributeValues = values
	}

	_, err := w.client.UpdateItem(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to update item: %w", err)
	}

	return nil
}
