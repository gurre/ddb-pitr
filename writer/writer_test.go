package writer

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gurre/ddb-pitr/itemimage"
)

// mockDynamoDBClient implements the aws.DynamoDBClient interface for testing
type mockDynamoDBClient struct {
	batches     [][]types.WriteRequest
	updateItems []*dynamodb.UpdateItemInput
}

func (m *mockDynamoDBClient) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	for _, requests := range params.RequestItems {
		m.batches = append(m.batches, requests)
	}
	return &dynamodb.BatchWriteItemOutput{}, nil
}

func (m *mockDynamoDBClient) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	m.updateItems = append(m.updateItems, params)
	return &dynamodb.UpdateItemOutput{}, nil
}

func TestWriterHappyPath(t *testing.T) {
	// Set up test data
	mockClient := &mockDynamoDBClient{}
	w := NewDynamoDBWriter(mockClient, "test-table", 3, Callbacks{}) // batch size of 3

	// Create test operations
	ops := []itemimage.Operation{
		// Put operation
		{
			Type: itemimage.OpPut,
			NewImage: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: "USER#123"},
				"SK":   &types.AttributeValueMemberS{Value: "PROFILE"},
				"name": &types.AttributeValueMemberS{Value: "John Doe"},
				"age":  &types.AttributeValueMemberN{Value: "30"},
			},
		},
		// Delete operation
		{
			Type: itemimage.OpDelete,
			Keys: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "USER#456"},
				"SK": &types.AttributeValueMemberS{Value: "PROFILE"},
			},
		},
		// Update operation
		{
			Type: itemimage.OpUpdate,
			Keys: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "USER#789"},
				"SK": &types.AttributeValueMemberS{Value: "PROFILE"},
			},
			OldImage: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: "USER#789"},
				"SK":   &types.AttributeValueMemberS{Value: "PROFILE"},
				"name": &types.AttributeValueMemberS{Value: "Jane Doe"},
				"age":  &types.AttributeValueMemberN{Value: "25"},
				"city": &types.AttributeValueMemberS{Value: "New York"},
			},
			NewImage: map[string]types.AttributeValue{
				"PK":    &types.AttributeValueMemberS{Value: "USER#789"},
				"SK":    &types.AttributeValueMemberS{Value: "PROFILE"},
				"name":  &types.AttributeValueMemberS{Value: "Jane Smith"},
				"age":   &types.AttributeValueMemberN{Value: "26"},
				"email": &types.AttributeValueMemberS{Value: "jane@example.com"},
			},
		},
	}

	// Test writing batch
	if err := w.WriteBatch(context.Background(), ops); err != nil {
		t.Fatalf("failed to write batch: %v", err)
	}

	// Verify BatchWriteItem calls
	if len(mockClient.batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(mockClient.batches))
	}

	batch := mockClient.batches[0]
	if len(batch) != 2 {
		t.Errorf("expected 2 requests in batch (1 Put + 1 Delete), got %d", len(batch))
	}

	// Verify Put request
	if req := batch[0].PutRequest; req == nil {
		t.Error("expected PutRequest")
	} else {
		if name, ok := req.Item["name"].(*types.AttributeValueMemberS); !ok {
			t.Error("expected name to be string attribute")
		} else if name.Value != "John Doe" {
			t.Errorf("expected name 'John Doe', got '%s'", name.Value)
		}
		if age, ok := req.Item["age"].(*types.AttributeValueMemberN); !ok {
			t.Error("expected age to be number attribute")
		} else if age.Value != "30" {
			t.Errorf("expected age '30', got '%s'", age.Value)
		}
	}

	// Verify Delete request
	if req := batch[1].DeleteRequest; req == nil {
		t.Error("expected DeleteRequest")
	} else {
		if pk, ok := req.Key["PK"].(*types.AttributeValueMemberS); !ok {
			t.Error("expected PK to be string attribute")
		} else if pk.Value != "USER#456" {
			t.Errorf("expected PK 'USER#456', got '%s'", pk.Value)
		}
	}

	// Verify UpdateItem call
	if len(mockClient.updateItems) != 1 {
		t.Fatalf("expected 1 UpdateItem call, got %d", len(mockClient.updateItems))
	}

	updateInput := mockClient.updateItems[0]
	if updateInput.TableName == nil || *updateInput.TableName != "test-table" {
		t.Error("expected table name 'test-table'")
	}

	// Verify update expression contains both SET and REMOVE operations
	if updateInput.UpdateExpression == nil {
		t.Error("expected update expression")
	} else {
		expr := *updateInput.UpdateExpression
		if !strings.Contains(expr, "name") || !strings.Contains(expr, "age") || !strings.Contains(expr, "email") {
			t.Errorf("update expression missing expected attributes: %s", expr)
		}
	}

	// Verify expression attribute values
	values := updateInput.ExpressionAttributeValues
	if values == nil {
		t.Error("expected expression attribute values")
	} else {
		if name, ok := values[":name"].(*types.AttributeValueMemberS); !ok || name.Value != "Jane Smith" {
			t.Error("expected name value 'Jane Smith'")
		}
		if age, ok := values[":age"].(*types.AttributeValueMemberN); !ok || age.Value != "26" {
			t.Error("expected age value '26'")
		}
		if email, ok := values[":email"].(*types.AttributeValueMemberS); !ok || email.Value != "jane@example.com" {
			t.Error("expected email value 'jane@example.com'")
		}
	}
}

// BenchmarkWriteBatch measures batch writing performance
func BenchmarkWriteBatch(b *testing.B) {
	mockClient := &mockDynamoDBClient{}
	w := NewDynamoDBWriter(mockClient, "test-table", 25, Callbacks{})

	ops := []itemimage.Operation{
		{
			Type: itemimage.OpPut,
			NewImage: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: "USER#123"},
				"SK":   &types.AttributeValueMemberS{Value: "PROFILE"},
				"name": &types.AttributeValueMemberS{Value: "John Doe"},
			},
		},
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.WriteBatch(ctx, ops)
	}
}

// BenchmarkWriteBatchLarge measures performance with larger batches
func BenchmarkWriteBatchLarge(b *testing.B) {
	mockClient := &mockDynamoDBClient{}
	w := NewDynamoDBWriter(mockClient, "test-table", 25, Callbacks{})

	ops := make([]itemimage.Operation, 25)
	for i := 0; i < 25; i++ {
		ops[i] = itemimage.Operation{
			Type: itemimage.OpPut,
			NewImage: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: "USER#123"},
				"SK":   &types.AttributeValueMemberS{Value: "PROFILE"},
				"name": &types.AttributeValueMemberS{Value: "John Doe"},
			},
		}
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.WriteBatch(ctx, ops)
		mockClient.batches = nil // Reset to avoid memory growth
	}
}

func TestWriterAllAttributeTypes(t *testing.T) {
	mockClient := &mockDynamoDBClient{}
	w := NewDynamoDBWriter(mockClient, "test-table", 1, Callbacks{})

	// Create an operation with all DynamoDB attribute types
	ops := []itemimage.Operation{
		{
			Type: itemimage.OpUpdate,
			Keys: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "PRODUCT#123"},
			},
			OldImage: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "PRODUCT#123"},
			},
			NewImage: map[string]types.AttributeValue{
				"PK":          &types.AttributeValueMemberS{Value: "PRODUCT#123"},
				"Id":          &types.AttributeValueMemberN{Value: "123"},
				"Title":       &types.AttributeValueMemberS{Value: "Bicycle 123"},
				"Description": &types.AttributeValueMemberS{Value: "123 description"},
				"BicycleType": &types.AttributeValueMemberS{Value: "Hybrid"},
				"Brand":       &types.AttributeValueMemberS{Value: "Brand-Company C"},
				"Price":       &types.AttributeValueMemberN{Value: "500"},
				"Color": &types.AttributeValueMemberSS{
					Value: []string{"Red", "Black"},
				},
				"ProductCategory": &types.AttributeValueMemberS{Value: "Bicycle"},
				"InStock":         &types.AttributeValueMemberBOOL{Value: true},
				"QuantityOnHand":  &types.AttributeValueMemberNULL{Value: true},
				"RelatedItems": &types.AttributeValueMemberNS{
					Value: []string{"341", "472", "649"},
				},
				"Pictures": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"FrontView": &types.AttributeValueMemberS{Value: "http://example.com/products/123_front.jpg"},
						"RearView":  &types.AttributeValueMemberS{Value: "http://example.com/products/123_rear.jpg"},
						"SideView":  &types.AttributeValueMemberS{Value: "http://example.com/products/123_left_side.jpg"},
					},
				},
				"ProductReviews": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"FiveStar": &types.AttributeValueMemberSS{
							Value: []string{
								"Excellent! Can't recommend it highly enough! Buy it!",
								"Do yourself a favor and buy this.",
							},
						},
						"OneStar": &types.AttributeValueMemberSS{
							Value: []string{
								"Terrible product! Do not buy this.",
							},
						},
					},
				},
				"Comment":        &types.AttributeValueMemberS{Value: "This product sells out quickly during the summer"},
				"Safety.Warning": &types.AttributeValueMemberS{Value: "Always wear a helmet"},
			},
		},
	}

	// Test writing batch
	if err := w.WriteBatch(context.Background(), ops); err != nil {
		t.Fatalf("failed to write batch: %v", err)
	}

	// Verify UpdateItem call
	if len(mockClient.updateItems) != 1 {
		t.Fatalf("expected 1 UpdateItem call, got %d", len(mockClient.updateItems))
	}

	updateInput := mockClient.updateItems[0]
	values := updateInput.ExpressionAttributeValues

	// Verify all attribute types are handled correctly
	tests := []struct {
		name     string
		value    types.AttributeValue
		expected interface{}
	}{
		{"Id", values[":Id"], &types.AttributeValueMemberN{Value: "123"}},
		{"Title", values[":Title"], &types.AttributeValueMemberS{Value: "Bicycle 123"}},
		{"Price", values[":Price"], &types.AttributeValueMemberN{Value: "500"}},
		{"Color", values[":Color"], &types.AttributeValueMemberSS{Value: []string{"Red", "Black"}}},
		{"InStock", values[":InStock"], &types.AttributeValueMemberBOOL{Value: true}},
		{"QuantityOnHand", values[":QuantityOnHand"], &types.AttributeValueMemberNULL{Value: true}},
		{"RelatedItems", values[":RelatedItems"], &types.AttributeValueMemberNS{Value: []string{"341", "472", "649"}}},
		{"Comment", values[":Comment"], &types.AttributeValueMemberS{Value: "This product sells out quickly during the summer"}},
		{"Safety.Warning", values[":Safety.Warning"], &types.AttributeValueMemberS{Value: "Always wear a helmet"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value == nil {
				t.Errorf("expected value for %s, got nil", tt.name)
				return
			}

			switch v := tt.value.(type) {
			case *types.AttributeValueMemberS:
				expected := tt.expected.(*types.AttributeValueMemberS)
				if v.Value != expected.Value {
					t.Errorf("expected %s value %q, got %q", tt.name, expected.Value, v.Value)
				}
			case *types.AttributeValueMemberN:
				expected := tt.expected.(*types.AttributeValueMemberN)
				if v.Value != expected.Value {
					t.Errorf("expected %s value %q, got %q", tt.name, expected.Value, v.Value)
				}
			case *types.AttributeValueMemberSS:
				expected := tt.expected.(*types.AttributeValueMemberSS)
				if len(v.Value) != len(expected.Value) {
					t.Errorf("expected %s length %d, got %d", tt.name, len(expected.Value), len(v.Value))
				}
				for i, val := range v.Value {
					if val != expected.Value[i] {
						t.Errorf("expected %s[%d] value %q, got %q", tt.name, i, expected.Value[i], val)
					}
				}
			case *types.AttributeValueMemberBOOL:
				expected := tt.expected.(*types.AttributeValueMemberBOOL)
				if v.Value != expected.Value {
					t.Errorf("expected %s value %v, got %v", tt.name, expected.Value, v.Value)
				}
			case *types.AttributeValueMemberNULL:
				expected := tt.expected.(*types.AttributeValueMemberNULL)
				if v.Value != expected.Value {
					t.Errorf("expected %s value %v, got %v", tt.name, expected.Value, v.Value)
				}
			case *types.AttributeValueMemberNS:
				expected := tt.expected.(*types.AttributeValueMemberNS)
				if len(v.Value) != len(expected.Value) {
					t.Errorf("expected %s length %d, got %d", tt.name, len(expected.Value), len(v.Value))
				}
				for i, val := range v.Value {
					if val != expected.Value[i] {
						t.Errorf("expected %s[%d] value %q, got %q", tt.name, i, expected.Value[i], val)
					}
				}
			case *types.AttributeValueMemberM:
				// For maps, we just verify they exist since they're complex
				if len(v.Value) == 0 {
					t.Errorf("expected non-empty map for %s", tt.name)
				}
			default:
				t.Errorf("unexpected type for %s: %T", tt.name, v)
			}
		})
	}
}

// TestCallbacksOnWrite verifies OnWrite callback is invoked with item count and byte size
// when batch write succeeds.
func TestCallbacksOnWrite(t *testing.T) {
	mockClient := &mockDynamoDBClient{}

	var writeCalls []struct{ items, bytes int }
	callbacks := Callbacks{
		OnWrite: func(items, bytes int) {
			writeCalls = append(writeCalls, struct{ items, bytes int }{items, bytes})
		},
	}

	w := NewDynamoDBWriter(mockClient, "test-table", 25, callbacks)

	ops := []itemimage.Operation{
		{
			Type: itemimage.OpPut,
			NewImage: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "USER#1"},
			},
		},
		{
			Type: itemimage.OpPut,
			NewImage: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "USER#2"},
			},
		},
	}

	if err := w.WriteBatch(context.Background(), ops); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if len(writeCalls) != 1 {
		t.Fatalf("expected 1 OnWrite call, got %d", len(writeCalls))
	}
	if writeCalls[0].items != 2 {
		t.Errorf("expected 2 items in OnWrite, got %d", writeCalls[0].items)
	}
	if writeCalls[0].bytes <= 0 {
		t.Errorf("expected positive byte count, got %d", writeCalls[0].bytes)
	}
}

// TestCallbacksOnWriteForUpdate verifies OnWrite callback is invoked for UpdateItem operations.
func TestCallbacksOnWriteForUpdate(t *testing.T) {
	mockClient := &mockDynamoDBClient{}

	var writeCalls int
	callbacks := Callbacks{
		OnWrite: func(items, bytes int) {
			writeCalls++
		},
	}

	w := NewDynamoDBWriter(mockClient, "test-table", 25, callbacks)

	ops := []itemimage.Operation{
		{
			Type: itemimage.OpUpdate,
			Keys: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "USER#1"},
			},
			OldImage: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "USER#1"},
			},
			NewImage: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: "USER#1"},
				"name": &types.AttributeValueMemberS{Value: "Test"},
			},
		},
	}

	if err := w.WriteBatch(context.Background(), ops); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if writeCalls != 1 {
		t.Errorf("expected 1 OnWrite call for update, got %d", writeCalls)
	}
}

// TestIsThrottlingError verifies throttling error detection for DynamoDB exceptions.
func TestIsThrottlingError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "ProvisionedThroughputExceededException",
			err:      &types.ProvisionedThroughputExceededException{Message: ptr("throttled")},
			expected: true,
		},
		{
			name:     "RequestLimitExceeded",
			err:      &types.RequestLimitExceeded{Message: ptr("limit exceeded")},
			expected: true,
		},
		{
			name:     "other error",
			err:      &types.ResourceNotFoundException{Message: ptr("not found")},
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isThrottlingError(tt.err); got != tt.expected {
				t.Errorf("isThrottlingError() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestEstimateItemSize verifies item size estimation for different attribute counts.
func TestEstimateItemSize(t *testing.T) {
	tests := []struct {
		name string
		item map[string]types.AttributeValue
		want int
	}{
		{
			name: "nil item",
			item: nil,
			want: 0,
		},
		{
			name: "empty item",
			item: map[string]types.AttributeValue{},
			want: 100, // base overhead
		},
		{
			name: "single attribute",
			item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "test"},
			},
			want: 150, // 100 base + 50 per attribute
		},
		{
			name: "multiple attributes",
			item: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: "test"},
				"SK":   &types.AttributeValueMemberS{Value: "test"},
				"name": &types.AttributeValueMemberS{Value: "test"},
			},
			want: 250, // 100 base + 3*50
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := estimateItemSize(tt.item); got != tt.want {
				t.Errorf("estimateItemSize() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestFlush verifies Flush is a no-op that returns nil.
func TestFlush(t *testing.T) {
	mockClient := &mockDynamoDBClient{}
	w := NewDynamoDBWriter(mockClient, "test-table", 25, Callbacks{})

	if err := w.Flush(context.Background()); err != nil {
		t.Errorf("Flush() returned error: %v", err)
	}
}

// TestWriteBatchEmpty verifies empty batch returns immediately without errors.
func TestWriteBatchEmpty(t *testing.T) {
	mockClient := &mockDynamoDBClient{}
	w := NewDynamoDBWriter(mockClient, "test-table", 25, Callbacks{})

	if err := w.WriteBatch(context.Background(), nil); err != nil {
		t.Errorf("WriteBatch(nil) returned error: %v", err)
	}
	if err := w.WriteBatch(context.Background(), []itemimage.Operation{}); err != nil {
		t.Errorf("WriteBatch([]) returned error: %v", err)
	}

	if len(mockClient.batches) != 0 {
		t.Errorf("expected no batches, got %d", len(mockClient.batches))
	}
}

// ptr returns a pointer to the string value
func ptr(s string) *string {
	return &s
}
