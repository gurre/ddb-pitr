// Package writer implements the DynamoDB writing functionality as specified in section 4.6
// of the design specification. It handles writing batches of operations to DynamoDB.
package writer

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
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

// Callbacks allows the writer to report metrics without coupling to a specific metrics implementation.
// All callbacks are optional - nil callbacks are safely ignored.
type Callbacks struct {
	OnThrottle func()                 // Called on throttle event
	OnRetry    func()                 // Called on successful retry after transient failure
	OnLost     func(count int)        // Called when items fail permanently after max retries
	OnWrite    func(items, bytes int) // Called on successful write with item count and byte size
}

// DynamoDBWriter implements the Writer interface using AWS DynamoDB as specified in section 4.6.
// It handles batching operations and retrying with exponential backoff.
type DynamoDBWriter struct {
	client    aws.DynamoDBClient
	callbacks Callbacks
	tableName string
	batchSize int // Maximum number of operations per batch (≤25)
}

// NewDynamoDBWriter creates a new DynamoDBWriter instance with the specified batch size and callbacks
func NewDynamoDBWriter(client aws.DynamoDBClient, tableName string, batchSize int, callbacks Callbacks) *DynamoDBWriter {
	return &DynamoDBWriter{
		client:    client,
		tableName: tableName,
		batchSize: batchSize,
		callbacks: callbacks,
	}
}

// isThrottlingError returns true if the error is a DynamoDB throughput throttling error.
// These errors indicate temporary capacity constraints and should trigger backoff and retry.
//
// DynamoDB throttles in four scenarios:
//  1. Key range throughput exceeded - hot partition, affects both provisioned and on-demand
//  2. Provisioned throughput exceeded - RCU/WCU exhausted in provisioned mode
//  3. Account-level service quotas exceeded - per-table limits in on-demand mode
//  4. On-demand maximum throughput exceeded - configured cost control limits
//
// All scenarios return ProvisionedThroughputExceededException or RequestLimitExceeded.
// These are recoverable by waiting - capacity refills over time.
func isThrottlingError(err error) bool {
	var throughputErr *types.ProvisionedThroughputExceededException
	var requestLimitErr *types.RequestLimitExceeded
	return errors.As(err, &throughputErr) || errors.As(err, &requestLimitErr)
}

// estimateItemSize returns an approximate byte size for a DynamoDB item.
// Uses a simple heuristic: 100 bytes base + 50 bytes per attribute.
// This avoids expensive serialization while providing reasonable estimates.
func estimateItemSize(item map[string]types.AttributeValue) int {
	if item == nil {
		return 0
	}
	// Base overhead + estimated bytes per attribute
	return 100 + len(item)*50
}

// backoffWait sleeps for an exponentially increasing duration with jitter.
// Returns false if the context is cancelled during the wait.
func backoffWait(ctx context.Context, attempt int) bool {
	// Base delay 100ms, max delay 30s
	base := 100 * time.Millisecond
	maxDelay := 30 * time.Second

	delay := base * time.Duration(1<<uint(attempt))
	if delay > maxDelay {
		delay = maxDelay
	}

	// Add jitter: random value between 0 and delay
	jitter := time.Duration(rand.Int64N(int64(delay)))
	delay = delay + jitter

	select {
	case <-time.After(delay):
		return true
	case <-ctx.Done():
		return false
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

		// Convert operations to DynamoDB requests and estimate byte size
		requests := make([]types.WriteRequest, 0, len(batch))
		batchBytes := 0
		for _, op := range batch {
			switch op.Type {
			case itemimage.OpPut:
				requests = append(requests, types.WriteRequest{
					PutRequest: &types.PutRequest{
						Item: op.NewImage,
					},
				})
				batchBytes += estimateItemSize(op.NewImage)
			case itemimage.OpDelete:
				requests = append(requests, types.WriteRequest{
					DeleteRequest: &types.DeleteRequest{
						Key: op.Keys,
					},
				})
				// Deletes have minimal size (just keys)
				batchBytes += estimateItemSize(op.Keys)
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

		// Track items in current batch for callbacks
		itemsInBatch := len(requests)

		// Retry with exponential backoff.
		// Throttling errors retry indefinitely until context is cancelled.
		// Other errors fail after maxRetries attempts.
		const maxRetries = 5
		attempt := 0
		hadRetry := false
		for {
			output, err := w.client.BatchWriteItem(ctx, input)
			if err != nil {
				if isThrottlingError(err) {
					// Throttling: wait and retry indefinitely
					if w.callbacks.OnThrottle != nil {
						w.callbacks.OnThrottle()
					}
					if !backoffWait(ctx, attempt) {
						return ctx.Err()
					}
					attempt++
					hadRetry = true
					continue
				}
				// Non-throttling error: retry up to maxRetries
				if attempt < maxRetries {
					if !backoffWait(ctx, attempt) {
						return ctx.Err()
					}
					attempt++
					hadRetry = true
					continue
				}
				// Permanent failure - record lost items
				if w.callbacks.OnLost != nil {
					w.callbacks.OnLost(itemsInBatch)
				}
				return fmt.Errorf("failed to write batch after %d retries: %w", maxRetries, err)
			}

			// Handle unprocessed items (indicates throttling)
			if len(output.UnprocessedItems) > 0 {
				if w.callbacks.OnThrottle != nil {
					w.callbacks.OnThrottle()
				}
				input.RequestItems = output.UnprocessedItems
				if !backoffWait(ctx, attempt) {
					return ctx.Err()
				}
				attempt++
				hadRetry = true
				continue
			}

			// Success - record retry if we had one
			if hadRetry && w.callbacks.OnRetry != nil {
				w.callbacks.OnRetry()
			}
			// Record successful write
			if w.callbacks.OnWrite != nil {
				w.callbacks.OnWrite(itemsInBatch, batchBytes)
			}
			break
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

	// Estimate byte size for the update
	updateBytes := estimateItemSize(op.NewImage)

	// Retry with exponential backoff.
	// Throttling errors retry indefinitely until context is cancelled.
	const maxRetries = 5
	attempt := 0
	hadRetry := false
	for {
		_, err := w.client.UpdateItem(ctx, input)
		if err != nil {
			if isThrottlingError(err) {
				// Throttling: wait and retry indefinitely
				if w.callbacks.OnThrottle != nil {
					w.callbacks.OnThrottle()
				}
				if !backoffWait(ctx, attempt) {
					return ctx.Err()
				}
				attempt++
				hadRetry = true
				continue
			}
			// Non-throttling error: retry up to maxRetries
			if attempt < maxRetries {
				if !backoffWait(ctx, attempt) {
					return ctx.Err()
				}
				attempt++
				hadRetry = true
				continue
			}
			// Permanent failure - record lost item
			if w.callbacks.OnLost != nil {
				w.callbacks.OnLost(1)
			}
			return fmt.Errorf("failed to update item after %d retries: %w", maxRetries, err)
		}

		// Success - record retry if we had one
		if hadRetry && w.callbacks.OnRetry != nil {
			w.callbacks.OnRetry()
		}
		// Record successful write
		if w.callbacks.OnWrite != nil {
			w.callbacks.OnWrite(1, updateBytes)
		}
		break
	}

	return nil
}
