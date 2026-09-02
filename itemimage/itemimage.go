// Package itemimage implements decoding JSON lines into DynamoDB operations.
package itemimage

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	json "github.com/goccy/go-json"
)

// OperationType says how an operation is applied to the target table.
type OperationType uint8

const (
	OpPut    OperationType = iota // Insert or replace an item
	OpDelete                      // Remove an item
	OpUpdate                      // Replace an item that existed before the export window
)

// Operation is one record of an export, with everything needed to apply it to the
// target table. An update carries the item's full new state in NewImage, since that is
// what an incremental export records, so it is applied the same way a put is.
// Fields are ordered largest-to-smallest for memory alignment.
type Operation struct {
	Keys     map[string]types.AttributeValue // Primary key attributes
	NewImage map[string]types.AttributeValue // New state of the item
	OldImage map[string]types.AttributeValue // Previous state of the item
	Bytes    int32                           // Length of the export line the operation was decoded from
	Type     OperationType                   // Type of operation (Put/Delete/Update)
}

// ErrCorrupt is returned when a line cannot be parsed according to the format
// specified in section 2 of the design specification.
var ErrCorrupt = fmt.Errorf("corrupt line")

// Decoder interface as defined in section 4.5 of the spec.
// Implementations must handle decoding JSON lines into Operations.
type Decoder interface {
	Decode(line []byte) (Operation, error)
}

// JSONDecoder implements the Decoder interface for JSON lines as specified in section 4.5.
// It handles parsing the DynamoDB PITR export format described in section 2.
type JSONDecoder struct{}

// NewJSONDecoder creates a new JSONDecoder instance
func NewJSONDecoder() *JSONDecoder {
	return &JSONDecoder{}
}

// Decode parses one export line into an Operation and records the line's length on it.
//
// A FULL export line is {"Item": {...}} and is a put. An INCREMENTAL export line
// carries Keys and, depending on what happened to the item and which view the export
// was taken with, images:
//   - Keys + NewImage + OldImage: the item was updated (new and old images view)
//   - Keys + NewImage: the item was inserted, or updated in a new images only export
//   - Keys + OldImage: the item was deleted (new and old images view)
//   - Keys alone: the item was deleted (new images only view)
//
// A delete must name what to delete, so one whose Keys are empty is corrupt.
//
// HOT PATH: This function processes every record from S3.
// Profiling shows ~27% CPU time and ~99% memory allocation occurs here.
// The main costs are:
//   - json.Unmarshal: ~21% CPU, ~78% memory (standard library JSON parsing)
//   - attributevalue.UnmarshalMapJSON: ~20% CPU, ~93% memory (AWS SDK conversion)
func (d *JSONDecoder) Decode(line []byte) (Operation, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(line, &raw); err != nil {
		return Operation{}, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}

	op := Operation{Bytes: int32(len(line))}

	// Handle FULL export format: {"Item": {...}}
	if itemRaw, ok := raw["Item"]; ok {
		item, err := attributevalue.UnmarshalMapJSON(itemRaw)
		if err != nil {
			return Operation{}, fmt.Errorf("%w: failed to parse Item: %v", ErrCorrupt, err)
		}
		op.NewImage = item
		op.Type = OpPut
		return op, nil
	}

	// Handle INCREMENTAL export format: {"Keys": {...}, "NewImage": {...}, "OldImage": {...}}
	if keysRaw, ok := raw["Keys"]; ok {
		keys, err := attributevalue.UnmarshalMapJSON(keysRaw)
		if err != nil {
			return Operation{}, fmt.Errorf("%w: failed to parse Keys: %v", ErrCorrupt, err)
		}
		op.Keys = keys
	}

	if newImageRaw, ok := raw["NewImage"]; ok {
		newImage, err := attributevalue.UnmarshalMapJSON(newImageRaw)
		if err != nil {
			return Operation{}, fmt.Errorf("%w: failed to parse NewImage: %v", ErrCorrupt, err)
		}
		op.NewImage = newImage
	}

	if oldImageRaw, ok := raw["OldImage"]; ok {
		oldImage, err := attributevalue.UnmarshalMapJSON(oldImageRaw)
		if err != nil {
			return Operation{}, fmt.Errorf("%w: failed to parse OldImage: %v", ErrCorrupt, err)
		}
		op.OldImage = oldImage
	}

	// Determine operation type for incremental exports
	switch {
	case op.NewImage != nil && op.OldImage != nil:
		op.Type = OpUpdate
	case op.NewImage != nil:
		op.Type = OpPut
	case op.OldImage != nil || op.Keys != nil:
		if len(op.Keys) == 0 {
			return Operation{}, fmt.Errorf("%w: delete names no keys", ErrCorrupt)
		}
		op.Type = OpDelete
	default:
		return Operation{}, fmt.Errorf("%w: no image data found", ErrCorrupt)
	}

	return op, nil
}
