package writer

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gurre/ddb-pitr/itemimage"
)

// TestSubmitSendsPutsDeletesAndUpdatesAsOneBatch verifies the three operation
// kinds an export produces all travel in a single BatchWriteItem: puts and updates as
// whole-item puts, deletes by key. An update sent as a separate per-item call would
// cost a round trip per changed item and bypass the batch's retry handling.
func TestSubmitSendsPutsDeletesAndUpdatesAsOneBatch(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	ops := []itemimage.Operation{
		putOps(1)[0],
		{
			Type: itemimage.OpDelete,
			Keys: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "USER#456"},
			},
		},
		updateOp(),
	}

	if err := writeAndWait(context.Background(), w, ops); err != nil {
		t.Fatalf("failed to write batch: %v", err)
	}

	if len(client.batchRequests) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(client.batchRequests))
	}
	batch := client.batchRequests[0]["test-table"]
	if len(batch) != 3 {
		t.Fatalf("expected 3 requests in the batch, got %d", len(batch))
	}
	if batch[0].PutRequest == nil || batch[1].DeleteRequest == nil || batch[2].PutRequest == nil {
		t.Errorf("expected put, delete, put; got %+v", batch)
	}
	if pk := batch[1].DeleteRequest.Key["PK"].(*types.AttributeValueMemberS); pk.Value != "USER#456" {
		t.Errorf("expected the delete keyed on USER#456, got %s", pk.Value)
	}
}

// TestUpdateIsWrittenAsAPutOfTheNewImage verifies an update replaces the item with its
// new image, attribute names taken as they are. The new image is the item's complete
// state after the change, so a put is exactly the change; an update expression built
// from the names would break on any name DynamoDB's expression grammar rejects, such
// as one containing a dot.
func TestUpdateIsWrittenAsAPutOfTheNewImage(t *testing.T) {
	client := &scriptedClient{}
	w := NewDynamoDBWriter(client, "test-table", Callbacks{}, WithBackoff(&instantBackoff{}), withPaceClock(newTestClock()))

	newImage := map[string]types.AttributeValue{
		"PK":             &types.AttributeValueMemberS{Value: "PRODUCT#123"},
		"Title":          &types.AttributeValueMemberS{Value: "Bicycle 123"},
		"Safety.Warning": &types.AttributeValueMemberS{Value: "Always wear a helmet"},
		"In Stock":       &types.AttributeValueMemberBOOL{Value: true},
		"unit-price":     &types.AttributeValueMemberN{Value: "500"},
	}
	op := itemimage.Operation{
		Type:     itemimage.OpUpdate,
		Keys:     map[string]types.AttributeValue{"PK": newImage["PK"]},
		OldImage: map[string]types.AttributeValue{"PK": newImage["PK"], "Colour": &types.AttributeValueMemberS{Value: "red"}},
		NewImage: newImage,
	}

	if err := writeAndWait(context.Background(), w, []itemimage.Operation{op}); err != nil {
		t.Fatalf("failed to write batch: %v", err)
	}

	if len(client.batchRequests) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(client.batchRequests))
	}
	put := client.batchRequests[0]["test-table"][0].PutRequest
	if put == nil {
		t.Fatal("expected the update sent as a put")
	}
	if len(put.Item) != len(newImage) {
		t.Errorf("expected the put to carry the whole new image, got %v", put.Item)
	}
	for name := range newImage {
		if put.Item[name] != newImage[name] {
			t.Errorf("expected attribute %q carried verbatim, got %v", name, put.Item[name])
		}
	}
}

// acceptingClient accepts every batch and records nothing, so a benchmark measures the
// writer rather than the growth of a recording double.
type acceptingClient struct{}

func (acceptingClient) BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	return &dynamodb.BatchWriteItemOutput{}, nil
}

// BenchmarkSubmit measures submitting a one-item batch and waiting for it, which is the
// writer's own cost per call with the table answering at once.
func BenchmarkSubmit(b *testing.B) {
	w := NewDynamoDBWriter(acceptingClient{}, "test-table", Callbacks{})
	ops := putOps(1)

	ctx := context.Background()
	for b.Loop() {
		_ = writeAndWait(ctx, w, ops)
	}
}

// BenchmarkSubmitFullBatch measures the same for a batch of the most items one call takes.
func BenchmarkSubmitFullBatch(b *testing.B) {
	w := NewDynamoDBWriter(acceptingClient{}, "test-table", Callbacks{})
	ops := putOps(25)

	ctx := context.Background()
	for b.Loop() {
		_ = writeAndWait(ctx, w, ops)
	}
}

// ptr returns a pointer to the string value
func ptr(s string) *string {
	return &s
}
