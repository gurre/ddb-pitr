package mock

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoDBClient is a mock implementation of aws.DynamoDBClient interface for testing.
// It stores items using composite keys derived from all key attributes.
type DynamoDBClient struct {
	// Thread-safe map of table data: tableName -> compositeKey -> attributes
	tableData     map[string]map[string]map[string]types.AttributeValue
	mu            sync.RWMutex
	batchWrites   []dynamodb.BatchWriteItemInput
	updateItems   []dynamodb.UpdateItemInput
	failNextWrite bool
	failMu        sync.Mutex
}

// NewDynamoDBClient creates a new mock DynamoDB client
func NewDynamoDBClient() *DynamoDBClient {
	return &DynamoDBClient{
		tableData:   make(map[string]map[string]map[string]types.AttributeValue),
		batchWrites: make([]dynamodb.BatchWriteItemInput, 0),
		updateItems: make([]dynamodb.UpdateItemInput, 0),
	}
}

// extractCompositeKey extracts a composite key from item attributes.
// It looks for common key patterns (pk/sk, PK/SK, id/sort) and creates
// a deterministic string key for storage.
func extractCompositeKey(item map[string]types.AttributeValue) string {
	keyPairs := make([]string, 0, 2)

	// Common partition key names
	pkNames := []string{"pk", "PK", "id", "ID", "partition_key"}
	// Common sort key names
	skNames := []string{"sk", "SK", "sort", "sort_key", "range_key"}

	var pkValue, skValue string

	// Find partition key
	for _, name := range pkNames {
		if v, ok := item[name]; ok {
			pkValue = attributeToString(v)
			if pkValue != "" {
				keyPairs = append(keyPairs, name+"="+pkValue)
				break
			}
		}
	}

	// Find sort key
	for _, name := range skNames {
		if v, ok := item[name]; ok {
			skValue = attributeToString(v)
			if skValue != "" {
				keyPairs = append(keyPairs, name+"="+skValue)
				break
			}
		}
	}

	if len(keyPairs) == 0 {
		// Fallback: use all string attributes sorted by name
		for k, v := range item {
			if s := attributeToString(v); s != "" {
				keyPairs = append(keyPairs, k+"="+s)
			}
		}
		sort.Strings(keyPairs)
		if len(keyPairs) > 2 {
			keyPairs = keyPairs[:2]
		}
	}

	return strings.Join(keyPairs, "#")
}

// attributeToString converts an AttributeValue to a string for key generation
func attributeToString(av types.AttributeValue) string {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return v.Value
	default:
		return ""
	}
}

// SetFailNextWrite configures the client to fail the next write operation
func (m *DynamoDBClient) SetFailNextWrite(fail bool) {
	m.failMu.Lock()
	defer m.failMu.Unlock()

	m.failNextWrite = fail
}

// shouldFail safely checks and resets the failNextWrite flag
func (m *DynamoDBClient) shouldFail() bool {
	m.failMu.Lock()
	defer m.failMu.Unlock()

	if m.failNextWrite {
		m.failNextWrite = false
		return true
	}
	return false
}

// BatchWriteItem implements the DynamoDBClient interface for batch writing items.
// Uses composite keys for proper storage of items with pk+sk.
func (m *DynamoDBClient) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	m.batchWrites = append(m.batchWrites, *params)

	if m.shouldFail() {
		return nil, fmt.Errorf("simulated batch write failure")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for tableName, writeRequests := range params.RequestItems {
		if _, exists := m.tableData[tableName]; !exists {
			m.tableData[tableName] = make(map[string]map[string]types.AttributeValue)
		}

		for _, writeRequest := range writeRequests {
			if writeRequest.PutRequest != nil {
				item := writeRequest.PutRequest.Item
				compositeKey := extractCompositeKey(item)
				m.tableData[tableName][compositeKey] = item
			}

			if writeRequest.DeleteRequest != nil {
				key := writeRequest.DeleteRequest.Key
				compositeKey := extractCompositeKey(key)
				delete(m.tableData[tableName], compositeKey)
			}
		}
	}

	return &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: make(map[string][]types.WriteRequest),
	}, nil
}

// UpdateItem implements the DynamoDBClient interface for updating individual items.
// It parses and applies SET and REMOVE expressions to properly update item attributes.
func (m *DynamoDBClient) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	m.updateItems = append(m.updateItems, *params)

	if m.shouldFail() {
		return nil, fmt.Errorf("simulated update failure")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	tableName := *params.TableName

	if _, exists := m.tableData[tableName]; !exists {
		m.tableData[tableName] = make(map[string]map[string]types.AttributeValue)
	}

	compositeKey := extractCompositeKey(params.Key)

	// Create item if it doesn't exist
	if _, exists := m.tableData[tableName][compositeKey]; !exists {
		m.tableData[tableName][compositeKey] = make(map[string]types.AttributeValue)
		for k, v := range params.Key {
			m.tableData[tableName][compositeKey][k] = v
		}
	}

	item := m.tableData[tableName][compositeKey]

	// Parse and apply update expression
	if params.UpdateExpression != nil {
		expr := *params.UpdateExpression

		// Handle SET expressions: SET #attr1 = :val1, #attr2 = :val2
		if idx := strings.Index(expr, "SET "); idx != -1 {
			setEnd := strings.Index(expr, " REMOVE")
			setExpr := expr[idx+4:]
			if setEnd > idx {
				setExpr = expr[idx+4 : setEnd]
			}

			// Parse SET assignments
			assignments := strings.Split(setExpr, ", ")
			for _, assignment := range assignments {
				parts := strings.Split(strings.TrimSpace(assignment), " = ")
				if len(parts) != 2 {
					continue
				}
				attrNameRef := strings.TrimSpace(parts[0])
				valueRef := strings.TrimSpace(parts[1])

				// Resolve attribute name from ExpressionAttributeNames
				attrName := attrNameRef
				if params.ExpressionAttributeNames != nil {
					if resolved, ok := params.ExpressionAttributeNames[attrNameRef]; ok {
						attrName = resolved
					}
				}

				// Resolve value from ExpressionAttributeValues
				if params.ExpressionAttributeValues != nil {
					if val, ok := params.ExpressionAttributeValues[valueRef]; ok {
						item[attrName] = val
					}
				}
			}
		}

		// Handle REMOVE expressions: REMOVE #attr1, #attr2
		if idx := strings.Index(expr, "REMOVE "); idx != -1 {
			removeExpr := expr[idx+7:]
			attrs := strings.Split(removeExpr, ", ")
			for _, attr := range attrs {
				attrNameRef := strings.TrimSpace(attr)
				attrName := attrNameRef
				if params.ExpressionAttributeNames != nil {
					if resolved, ok := params.ExpressionAttributeNames[attrNameRef]; ok {
						attrName = resolved
					}
				}
				delete(item, attrName)
			}
		}
	}

	return &dynamodb.UpdateItemOutput{}, nil
}

// GetTableContents returns the contents of a table for verification
func (m *DynamoDBClient) GetTableContents(tableName string) map[string]map[string]types.AttributeValue {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if data, exists := m.tableData[tableName]; exists {
		return data
	}
	return nil
}

// GetItem returns a specific item from the table by its key attributes.
// Returns nil if the item doesn't exist.
func (m *DynamoDBClient) GetItem(tableName string, key map[string]types.AttributeValue) map[string]types.AttributeValue {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if data, exists := m.tableData[tableName]; exists {
		compositeKey := extractCompositeKey(key)
		if item, ok := data[compositeKey]; ok {
			return item
		}
	}
	return nil
}

// ItemExists checks if an item exists in the table
func (m *DynamoDBClient) ItemExists(tableName string, key map[string]types.AttributeValue) bool {
	return m.GetItem(tableName, key) != nil
}

// GetBatchWrites returns the batch write requests that were made
func (m *DynamoDBClient) GetBatchWrites() []dynamodb.BatchWriteItemInput {
	return m.batchWrites
}

// GetUpdateItems returns the update item requests that were made
func (m *DynamoDBClient) GetUpdateItems() []dynamodb.UpdateItemInput {
	return m.updateItems
}

// ClearHistory clears the history of operations
func (m *DynamoDBClient) ClearHistory() {
	m.batchWrites = make([]dynamodb.BatchWriteItemInput, 0)
	m.updateItems = make([]dynamodb.UpdateItemInput, 0)
}

// ClearTableData clears all table data
func (m *DynamoDBClient) ClearTableData() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tableData = make(map[string]map[string]map[string]types.AttributeValue)
}
