// Package itemimage implements decoding JSON lines into DynamoDB operations.
package itemimage

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	json "github.com/goccy/go-json"
)

// OperationType represents the type of DynamoDB operation as defined in section 4.5.
// It determines how the operation should be applied to the target table.
type OperationType int

const (
	OpPut    OperationType = iota // Insert or replace an item
	OpDelete                      // Remove an item
	OpUpdate                      // Modify an existing item
)

// Operation represents a DynamoDB operation as defined in section 4.5.
// It contains all the data needed to perform the operation on the target table.
type Operation struct {
	Type     OperationType                   // Type of operation (Put/Delete/Update)
	Keys     map[string]types.AttributeValue // Primary key attributes
	NewImage map[string]types.AttributeValue // New state of the item
	OldImage map[string]types.AttributeValue // Previous state of the item
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

// Decode implements the decoding requirements from section 4.5.
// It parses a JSON line into an Operation, handling all required fields
// and determining the operation type based on the presence of NewImage/OldImage.
//
// Supports two export formats:
//   - FULL export: {"Item": {...}} - treated as OpPut
//   - INCREMENTAL export: {"Keys": {...}, "NewImage": {...}, "OldImage": {...}}
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

	op := Operation{}

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
	case op.OldImage != nil:
		op.Type = OpDelete
	default:
		return Operation{}, fmt.Errorf("%w: no image data found", ErrCorrupt)
	}

	return op, nil
}
