package itemimage

import (
	"errors"
	"testing"

	stdjson "encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	goccyjson "github.com/goccy/go-json"
)

// TestOperationTypeDetection verifies correct operation type is assigned based on
// presence of OldImage and NewImage fields in incremental exports.
func TestOperationTypeDetection(t *testing.T) {
	decoder := NewJSONDecoder()

	tests := []struct {
		name     string
		input    string
		wantType OperationType
		wantErr  bool
	}{
		{
			name:     "Update: both OldImage and NewImage present",
			input:    `{"Keys":{"PK":{"S":"1"}},"OldImage":{"PK":{"S":"1"},"val":{"S":"old"}},"NewImage":{"PK":{"S":"1"},"val":{"S":"new"}}}`,
			wantType: OpUpdate,
		},
		{
			name:     "Put: only NewImage present (insert)",
			input:    `{"Keys":{"PK":{"S":"1"}},"NewImage":{"PK":{"S":"1"},"val":{"S":"new"}}}`,
			wantType: OpPut,
		},
		{
			name:     "Delete: only OldImage present",
			input:    `{"Keys":{"PK":{"S":"1"}},"OldImage":{"PK":{"S":"1"},"val":{"S":"old"}}}`,
			wantType: OpDelete,
		},
		{
			name:     "FULL export format: Item field",
			input:    `{"Item":{"PK":{"S":"1"},"val":{"S":"test"}}}`,
			wantType: OpPut,
		},
		{
			name:    "Error: no image data",
			input:   `{"Keys":{"PK":{"S":"1"}}}`,
			wantErr: true,
		},
		{
			name:    "Error: empty object",
			input:   `{}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := decoder.Decode([]byte(tt.input))
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if op.Type != tt.wantType {
				t.Errorf("got type %d, want %d", op.Type, tt.wantType)
			}
		})
	}
}

// TestFullExportFormat verifies correct parsing of FULL export format {"Item": {...}}.
func TestFullExportFormat(t *testing.T) {
	decoder := NewJSONDecoder()

	input := `{"Item":{"PK":{"S":"USER#123"},"SK":{"S":"PROFILE"},"name":{"S":"John"},"age":{"N":"30"}}}`

	op, err := decoder.Decode([]byte(input))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if op.Type != OpPut {
		t.Errorf("expected OpPut, got %d", op.Type)
	}

	// NewImage should contain the item
	if op.NewImage == nil {
		t.Fatal("NewImage is nil")
	}

	// Verify attributes
	if pk, ok := op.NewImage["PK"].(*types.AttributeValueMemberS); !ok || pk.Value != "USER#123" {
		t.Errorf("PK mismatch: got %v", op.NewImage["PK"])
	}
	if name, ok := op.NewImage["name"].(*types.AttributeValueMemberS); !ok || name.Value != "John" {
		t.Errorf("name mismatch: got %v", op.NewImage["name"])
	}
	if age, ok := op.NewImage["age"].(*types.AttributeValueMemberN); !ok || age.Value != "30" {
		t.Errorf("age mismatch: got %v", op.NewImage["age"])
	}

	// Keys should not be populated for FULL export
	if op.Keys != nil {
		t.Errorf("Keys should be nil for FULL export, got %v", op.Keys)
	}
}

// TestIncrementalExportKeys verifies Keys field is correctly extracted from incremental exports.
func TestIncrementalExportKeys(t *testing.T) {
	decoder := NewJSONDecoder()

	input := `{"Keys":{"PK":{"S":"ITEM#99"},"SK":{"S":"META"}},"NewImage":{"PK":{"S":"ITEM#99"},"SK":{"S":"META"},"data":{"S":"test"}}}`

	op, err := decoder.Decode([]byte(input))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if op.Keys == nil {
		t.Fatal("Keys is nil")
	}

	pk, ok := op.Keys["PK"].(*types.AttributeValueMemberS)
	if !ok || pk.Value != "ITEM#99" {
		t.Errorf("PK key mismatch: got %v", op.Keys["PK"])
	}

	sk, ok := op.Keys["SK"].(*types.AttributeValueMemberS)
	if !ok || sk.Value != "META" {
		t.Errorf("SK key mismatch: got %v", op.Keys["SK"])
	}
}

// TestAllAttributeTypes verifies all DynamoDB attribute types are correctly parsed.
func TestAllAttributeTypes(t *testing.T) {
	decoder := NewJSONDecoder()

	// Comprehensive test with all attribute types
	input := `{
		"Item": {
			"pk": {"S": "test"},
			"str": {"S": "hello"},
			"num": {"N": "42"},
			"bin": {"B": "SGVsbG8="},
			"bool_true": {"BOOL": true},
			"bool_false": {"BOOL": false},
			"null_val": {"NULL": true},
			"str_set": {"SS": ["a", "b", "c"]},
			"num_set": {"NS": ["1", "2", "3"]},
			"bin_set": {"BS": ["SGVsbG8=", "V29ybGQ="]},
			"list": {"L": [{"S": "item1"}, {"N": "123"}]},
			"map": {"M": {"nested": {"S": "value"}, "count": {"N": "5"}}}
		}
	}`

	op, err := decoder.Decode([]byte(input))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	tests := []struct {
		attr  string
		check func(types.AttributeValue) bool
	}{
		{"str", func(v types.AttributeValue) bool {
			s, ok := v.(*types.AttributeValueMemberS)
			return ok && s.Value == "hello"
		}},
		{"num", func(v types.AttributeValue) bool {
			n, ok := v.(*types.AttributeValueMemberN)
			return ok && n.Value == "42"
		}},
		{"bin", func(v types.AttributeValue) bool {
			b, ok := v.(*types.AttributeValueMemberB)
			return ok && string(b.Value) == "Hello"
		}},
		{"bool_true", func(v types.AttributeValue) bool {
			b, ok := v.(*types.AttributeValueMemberBOOL)
			return ok && b.Value == true
		}},
		{"bool_false", func(v types.AttributeValue) bool {
			b, ok := v.(*types.AttributeValueMemberBOOL)
			return ok && b.Value == false
		}},
		{"null_val", func(v types.AttributeValue) bool {
			n, ok := v.(*types.AttributeValueMemberNULL)
			return ok && n.Value == true
		}},
		{"str_set", func(v types.AttributeValue) bool {
			ss, ok := v.(*types.AttributeValueMemberSS)
			return ok && len(ss.Value) == 3
		}},
		{"num_set", func(v types.AttributeValue) bool {
			ns, ok := v.(*types.AttributeValueMemberNS)
			return ok && len(ns.Value) == 3
		}},
		{"bin_set", func(v types.AttributeValue) bool {
			bs, ok := v.(*types.AttributeValueMemberBS)
			return ok && len(bs.Value) == 2
		}},
		{"list", func(v types.AttributeValue) bool {
			l, ok := v.(*types.AttributeValueMemberL)
			return ok && len(l.Value) == 2
		}},
		{"map", func(v types.AttributeValue) bool {
			m, ok := v.(*types.AttributeValueMemberM)
			if !ok || len(m.Value) != 2 {
				return false
			}
			nested, ok := m.Value["nested"].(*types.AttributeValueMemberS)
			return ok && nested.Value == "value"
		}},
	}

	for _, tt := range tests {
		t.Run(tt.attr, func(t *testing.T) {
			v, exists := op.NewImage[tt.attr]
			if !exists {
				t.Fatalf("attribute %s not found", tt.attr)
			}
			if !tt.check(v) {
				t.Errorf("attribute %s check failed: %T = %v", tt.attr, v, v)
			}
		})
	}
}

// TestUpdateOperationImages verifies both OldImage and NewImage are captured for updates.
func TestUpdateOperationImages(t *testing.T) {
	decoder := NewJSONDecoder()

	input := `{
		"Keys": {"PK": {"S": "ITEM#1"}, "SK": {"S": "META"}},
		"OldImage": {"PK": {"S": "ITEM#1"}, "SK": {"S": "META"}, "status": {"S": "pending"}, "count": {"N": "5"}},
		"NewImage": {"PK": {"S": "ITEM#1"}, "SK": {"S": "META"}, "status": {"S": "complete"}, "count": {"N": "10"}, "timestamp": {"N": "12345"}}
	}`

	op, err := decoder.Decode([]byte(input))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if op.Type != OpUpdate {
		t.Errorf("expected OpUpdate, got %d", op.Type)
	}

	// Verify OldImage
	if op.OldImage == nil {
		t.Fatal("OldImage is nil")
	}
	if status, ok := op.OldImage["status"].(*types.AttributeValueMemberS); !ok || status.Value != "pending" {
		t.Errorf("OldImage status mismatch")
	}

	// Verify NewImage
	if op.NewImage == nil {
		t.Fatal("NewImage is nil")
	}
	if status, ok := op.NewImage["status"].(*types.AttributeValueMemberS); !ok || status.Value != "complete" {
		t.Errorf("NewImage status mismatch")
	}

	// Verify new attribute in NewImage
	if _, exists := op.NewImage["timestamp"]; !exists {
		t.Error("timestamp should exist in NewImage")
	}

	// Verify removed attribute detection (count changed, not removed in this case)
	oldCount := op.OldImage["count"].(*types.AttributeValueMemberN).Value
	newCount := op.NewImage["count"].(*types.AttributeValueMemberN).Value
	if oldCount == newCount {
		t.Error("count should have changed")
	}
}

// TestDeleteOperation verifies delete operations have OldImage but no NewImage.
func TestDeleteOperation(t *testing.T) {
	decoder := NewJSONDecoder()

	input := `{
		"Keys": {"PK": {"S": "ITEM#99"}, "SK": {"S": "META"}},
		"OldImage": {"PK": {"S": "ITEM#99"}, "SK": {"S": "META"}, "data": {"S": "deleted"}}
	}`

	op, err := decoder.Decode([]byte(input))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if op.Type != OpDelete {
		t.Errorf("expected OpDelete, got %d", op.Type)
	}

	if op.OldImage == nil {
		t.Fatal("OldImage should not be nil for delete")
	}

	if op.NewImage != nil {
		t.Error("NewImage should be nil for delete")
	}

	// Keys should match OldImage keys
	if pk, ok := op.Keys["PK"].(*types.AttributeValueMemberS); !ok || pk.Value != "ITEM#99" {
		t.Errorf("Keys PK mismatch")
	}
}

// TestInsertOperation verifies insert operations have NewImage but no OldImage.
func TestInsertOperation(t *testing.T) {
	decoder := NewJSONDecoder()

	input := `{
		"Keys": {"PK": {"S": "ITEM#NEW"}, "SK": {"S": "META"}},
		"NewImage": {"PK": {"S": "ITEM#NEW"}, "SK": {"S": "META"}, "data": {"S": "inserted"}}
	}`

	op, err := decoder.Decode([]byte(input))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if op.Type != OpPut {
		t.Errorf("expected OpPut for insert, got %d", op.Type)
	}

	if op.NewImage == nil {
		t.Fatal("NewImage should not be nil for insert")
	}

	if op.OldImage != nil {
		t.Error("OldImage should be nil for insert")
	}
}

// TestCorruptDataHandling verifies error handling for malformed input.
func TestCorruptDataHandling(t *testing.T) {
	decoder := NewJSONDecoder()

	tests := []struct {
		name  string
		input string
	}{
		{"invalid JSON", `{not valid json`},
		{"empty string", ``},
		{"null", `null`},
		{"array instead of object", `[]`},
		{"invalid Item format", `{"Item": "not an object"}`},
		{"invalid Keys format", `{"Keys": "not an object", "NewImage": {}}`},
		{"invalid NewImage format", `{"Keys": {}, "NewImage": "not an object"}`},
		{"invalid OldImage format", `{"Keys": {}, "OldImage": "not an object"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decoder.Decode([]byte(tt.input))
			if err == nil {
				t.Error("expected error for corrupt data")
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Errorf("expected ErrCorrupt, got %v", err)
			}
		})
	}
}

// TestMetadataIgnored verifies that Metadata field is correctly ignored during parsing.
func TestMetadataIgnored(t *testing.T) {
	decoder := NewJSONDecoder()

	input := `{
		"Metadata": {"WriteTimestampMicros": {"N": "1746609560577628"}},
		"Keys": {"PK": {"S": "1"}},
		"NewImage": {"PK": {"S": "1"}, "data": {"S": "test"}}
	}`

	op, err := decoder.Decode([]byte(input))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Should parse successfully despite Metadata field
	if op.Type != OpPut {
		t.Errorf("expected OpPut, got %d", op.Type)
	}
}

// TestAttributeRemovedInUpdate verifies detecting attributes removed during update.
// This is critical for the writer to generate correct REMOVE expressions.
func TestAttributeRemovedInUpdate(t *testing.T) {
	decoder := NewJSONDecoder()

	// OldImage has 'removed_attr', NewImage doesn't
	input := `{
		"Keys": {"PK": {"S": "1"}},
		"OldImage": {"PK": {"S": "1"}, "kept": {"S": "stays"}, "removed_attr": {"S": "gone"}},
		"NewImage": {"PK": {"S": "1"}, "kept": {"S": "stays"}, "added": {"S": "new"}}
	}`

	op, err := decoder.Decode([]byte(input))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify removed_attr is in OldImage but not NewImage
	if _, exists := op.OldImage["removed_attr"]; !exists {
		t.Error("removed_attr should exist in OldImage")
	}
	if _, exists := op.NewImage["removed_attr"]; exists {
		t.Error("removed_attr should NOT exist in NewImage")
	}

	// Verify added attr is only in NewImage
	if _, exists := op.OldImage["added"]; exists {
		t.Error("added should NOT exist in OldImage")
	}
	if _, exists := op.NewImage["added"]; !exists {
		t.Error("added should exist in NewImage")
	}
}

var testData = [][]byte{
	[]byte("{\"Metadata\":{\"WriteTimestampMicros\":{\"N\":\"1746609560577628\"}},\"Keys\":{\"PK\":{\"S\":\"ITEM#99\"},\"SK\":{\"S\":\"METADATA\"}},\"OldImage\":{\"AttrIndex\":{\"N\":\"780\"},\"AttrLevel\":{\"BOOL\":true},\"AttrSize\":{\"BS\":[\"VWN5WVJncVplZQ==\"]},\"DataName\":{\"NULL\":true},\"DataScore\":{\"L\":[{\"S\":\"dYjXIBvLCgEkkzWixMGv\"},{\"S\":\"lOtkmjiAPSQR\"},{\"S\":\"GdqvFgCQSzvxVlqZ\"},{\"S\":\"HsIwGEt\"},{\"S\":\"ulxOLDKpbqDZ\"}]},\"DataStatus\":{\"L\":[{\"S\":\"KwStgafmLbvmKqCKdH\"}]},\"FieldSize\":{\"SS\":[\"DOvAQjN\"]},\"FieldStatus\":{\"BS\":[\"ZHBZUGc=\",\"dGFyQmN4\",\"enpBbks=\"]},\"InfoName\":{\"BS\":[\"QWFNYlloaQ==\",\"Tklld0FBU2c=\",\"YnpCZ256ZXZhTg==\"]},\"InfoType\":{\"M\":{\"OwOVW\":{\"S\":\"tKBpVavbBjEZGFZ\"},\"Ysfcp\":{\"S\":\"YMCAzpsztdX\"}}},\"PK\":{\"S\":\"ITEM#99\"},\"SK\":{\"S\":\"METADATA\"},\"SettingIndex\":{\"B\":\"VHlnRnR3Qk5OV0xl\"},\"SettingLevel\":{\"BOOL\":false},\"ValueCount\":{\"N\":\"965\"},\"ValueStatus\":{\"BS\":[\"a0pkanBIeVdBcA==\"]}},\"NewImage\":{\"AttrType\":{\"BOOL\":false},\"ConfigIndex\":{\"NS\":[\"590\"]},\"ConfigType\":{\"L\":[{\"S\":\"RciouzoMWuwzH\"}]},\"DataLevel\":{\"S\":\"FuJTEYobbkzPAWpPgXxkiLaPwkIYSHdYPFH\"},\"DataScore\":{\"BOOL\":false},\"DataStatus\":{\"BOOL\":true},\"InfoCount\":{\"B\":\"b2toWnQ=\"},\"InfoIndex\":{\"M\":{\"KJjUj\":{\"S\":\"MktTjmYlCqkSPXLT\"},\"MxqTd\":{\"S\":\"NitYMpqLoCkpLJ\"},\"bqwyu\":{\"S\":\"RAbqIBkAtLNTGGYFFQxZ\"},\"hYWig\":{\"S\":\"pZGWmkdFISbUvNTB\"},\"qHLXr\":{\"S\":\"sTrasrtAzKSLeEwCsE\"}}},\"InfoLevel\":{\"BS\":[\"RElJQU5IREc=\"]},\"MetaCount\":{\"S\":\"lhvZLNAyB\"},\"PK\":{\"S\":\"ITEM#99\"},\"SK\":{\"S\":\"METADATA\"},\"SettingLevel\":{\"NS\":[\"409\",\"783\"]}}}"),
	[]byte("{\"Metadata\":{\"WriteTimestampMicros\":{\"N\":\"1746609559255204\"}},\"Keys\":{\"PK\":{\"S\":\"ITEM#32\"},\"SK\":{\"S\":\"METADATA\"}},\"OldImage\":{\"AttrLevel\":{\"S\":\"YSxXsIVRNgPTjqiZGZJgYRSGcFyE\"},\"AttrName\":{\"B\":\"SU1mbm15SkhSd01kSWlaTWI=\"},\"DataIndex\":{\"SS\":[\"aHZEWX\",\"bPJxNNsJSwQQu\",\"dbGQLzXeTztbCwLxm\",\"hQpIbzfMX\",\"viNBcSmD\"]},\"InfoName\":{\"N\":\"985\"},\"MetaIndex\":{\"B\":\"S1hPVHdXS2Fwcnp0V1Vxb1NX\"},\"MetaLevel\":{\"M\":{\"BIhlq\":{\"S\":\"pCxwZQOEPB\"},\"LVeSm\":{\"S\":\"WwiJrYveIrt\"},\"iylSA\":{\"S\":\"FoclZOHjy\"}}},\"MetaStatus\":{\"L\":[{\"S\":\"tcZvzFnpHpHKNxpwKUQ\"}]},\"PK\":{\"S\":\"ITEM#32\"},\"SK\":{\"S\":\"METADATA\"},\"SettingLevel\":{\"M\":{\"TstYP\":{\"S\":\"jBncHBnGHBnkc\"},\"qWRPf\":{\"S\":\"VIDpChoPMNoA\"},\"vMieD\":{\"S\":\"gvyCafgQaHKpvoDEyqb\"}}},\"SettingScore\":{\"M\":{\"KKHuX\":{\"S\":\"yIvya\"},\"NTAas\":{\"S\":\"NQoSduNCEXOphuBAC\"},\"RwoGu\":{\"S\":\"wCyObZCGGUrqzNrmeT\"},\"cAmyw\":{\"S\":\"cojrHTEVpPJwEkl\"}}},\"SettingStatus\":{\"B\":\"QXdtRElOVm9Bd0R1dWtu\"},\"ValueLevel\":{\"NS\":[\"3\",\"323\",\"364\",\"588\",\"813\"]}},\"NewImage\":{\"DataIndex\":{\"L\":[{\"S\":\"ozEpsqOBU\"},{\"S\":\"YrrmPY\"},{\"S\":\"UztnQnUnTqQRUsBxVgMG\"},{\"S\":\"KUoPIgpuouuScyX\"}]},\"DataLevel\":{\"SS\":[\"DHSBzIDYkpn\",\"EZpffGCqLuFmSmIyXJcC\",\"WiqdKnRmIdygd\"]},\"FieldLevel\":{\"L\":[{\"S\":\"qrYQBCixEvnecQP\"},{\"S\":\"JXvKFXpZRYVsB\"}]},\"FieldName\":{\"L\":[{\"S\":\"eOuopkfiwGQyBFrh\"}]},\"MetaStatus\":{\"BS\":[\"TWx0bWtlY0dLTw==\"]},\"PK\":{\"S\":\"ITEM#32\"},\"SK\":{\"S\":\"METADATA\"},\"SettingScore\":{\"NS\":[\"486\",\"701\"]},\"SettingStatus\":{\"N\":\"510\"},\"ValueType\":{\"SS\":[\"pwElqpZzx\"]}}}"),
	[]byte("{\"Metadata\":{\"WriteTimestampMicros\":{\"N\":\"1746609558717943\"}},\"Keys\":{\"PK\":{\"S\":\"ITEM#5\"},\"SK\":{\"S\":\"METADATA\"}},\"OldImage\":{\"AttrIndex\":{\"N\":\"715\"},\"ConfigLevel\":{\"NULL\":true},\"DataIndex\":{\"S\":\"yOVKOivHFNUNrUy\"},\"DataType\":{\"S\":\"MyQhXzpibVpXpoElZXGfIXvRCBDtfYXQWHIcXpjKXDSSZz\"},\"FieldCount\":{\"L\":[{\"S\":\"pfiZyrYKlyMDOfzvZLO\"},{\"S\":\"fPOIZSyppIYQegZj\"},{\"S\":\"BsPIeXQeuxxTsWib\"}]},\"FieldIndex\":{\"NS\":[\"222\",\"532\",\"543\"]},\"FieldName\":{\"M\":{\"NcdtB\":{\"S\":\"WuvjsmlrdWLwQl\"},\"lZxlr\":{\"S\":\"CUgzFGSj\"},\"qqvPG\":{\"S\":\"HIlvrReyX\"},\"voxjz\":{\"S\":\"NSAUrHoUgNNl\"}}},\"InfoSize\":{\"N\":\"321\"},\"InfoType\":{\"BOOL\":false},\"MetaCount\":{\"NULL\":true},\"MetaSize\":{\"NULL\":true},\"MetaType\":{\"NULL\":true},\"PK\":{\"S\":\"ITEM#5\"},\"SK\":{\"S\":\"METADATA\"},\"SettingName\":{\"NULL\":true},\"ValueIndex\":{\"B\":\"SFhnWEtndg==\"}},\"NewImage\":{\"AttrCount\":{\"SS\":[\"AhePQWf\",\"BOIRYhwKC\",\"EBgtL\",\"GrDGtOMc\",\"sNNXdiGpGQTb\"]},\"AttrSize\":{\"NULL\":true},\"ConfigScore\":{\"NS\":[\"81\",\"290\",\"882\",\"963\"]},\"DataCount\":{\"B\":\"WVR3QVZ4clVTRExiSlV1\"},\"DataIndex\":{\"N\":\"95\"},\"DataType\":{\"NULL\":true},\"FieldStatus\":{\"NS\":[\"320\",\"804\",\"993\"]},\"InfoSize\":{\"NULL\":true},\"PK\":{\"S\":\"ITEM#5\"},\"SK\":{\"S\":\"METADATA\"},\"SettingCount\":{\"NS\":[\"287\",\"339\",\"704\",\"743\",\"760\"]},\"SettingName\":{\"M\":{\"BMZvQ\":{\"S\":\"ksLkSVCUB\"},\"FhWqK\":{\"S\":\"dEcLlYiUYtVTbAyb\"},\"QgYKF\":{\"S\":\"RvlNpw\"},\"lYEfq\":{\"S\":\"CbDUJTWRYSlImjdJ\"},\"rdslN\":{\"S\":\"CXCDPBHTMdcNusdOwDw\"}}},\"ValueCount\":{\"NULL\":true},\"ValueName\":{\"SS\":[\"EDLOZeWyr\",\"NlFcilLLDSedAw\",\"meDZmZJgkztEoZnSse\",\"vqGyxXt\",\"yQpQidzdwHOsWrxQfI\"]}}}"),
	[]byte("{\"Metadata\":{\"WriteTimestampMicros\":{\"N\":\"1746609558939738\"}},\"Keys\":{\"PK\":{\"S\":\"ITEM#16\"},\"SK\":{\"S\":\"METADATA\"}},\"OldImage\":{\"AttrLevel\":{\"BOOL\":false},\"AttrSize\":{\"S\":\"lQUPvIVWcCFsTIjnNTCpYDClxmNYsEd\"},\"ConfigName\":{\"M\":{\"FeYQf\":{\"S\":\"BOZiOLi\"},\"IJYSD\":{\"S\":\"DZQvC\"},\"rmASI\":{\"S\":\"cIWDbUHiktf\"},\"tdUNm\":{\"S\":\"rDZLHGXX\"}}},\"FieldCount\":{\"NS\":[\"154\",\"187\",\"449\",\"747\",\"941\"]},\"FieldLevel\":{\"BS\":[\"T01vVFFReGZu\",\"VU1FV3M=\",\"aXdKYkhZ\"]},\"InfoStatus\":{\"B\":\"Y29sc1RnelVZckREeXB4VGp2WFg=\"},\"PK\":{\"S\":\"ITEM#16\"},\"SK\":{\"S\":\"METADATA\"},\"ValueCount\":{\"L\":[{\"S\":\"XqTqFraNulpEavxndaZ\"},{\"S\":\"qpKEzj\"}]},\"ValueName\":{\"B\":\"RUJNak5R\"}},\"NewImage\":{\"AttrName\":{\"BS\":[\"Z2x6Qk1qS2w=\"]},\"ConfigName\":{\"M\":{\"NqxNS\":{\"S\":\"iYXXlZLIrfY\"},\"rSJUD\":{\"S\":\"jgHoAbn\"},\"rbEOu\":{\"S\":\"XWqycNfGBGfBjqeIz\"}}},\"FieldIndex\":{\"NS\":[\"13\",\"449\"]},\"FieldStatus\":{\"S\":\"SthNXtqzDJaqwuZPXTLwFESWcaFARCthtcrEOcBKObl\"},\"InfoCount\":{\"BS\":[\"YUtoZGxDV1Bp\",\"c3RKeFc=\",\"eGNMU2ltUg==\"]},\"InfoIndex\":{\"NULL\":true},\"MetaIndex\":{\"NS\":[\"283\",\"589\",\"697\",\"776\"]},\"MetaName\":{\"NS\":[\"501\"]},\"MetaScore\":{\"BS\":[\"UmJIWU9YS1U=\"]},\"PK\":{\"S\":\"ITEM#16\"},\"SK\":{\"S\":\"METADATA\"},\"SettingLevel\":{\"M\":{\"kogjW\":{\"S\":\"aRJSoZnhYP\"},\"sjPLr\":{\"S\":\"UxdpVf\"}}},\"SettingScore\":{\"BOOL\":false},\"SettingStatus\":{\"B\":\"dWpRSUNraWF0eg==\"},\"ValueStatus\":{\"S\":\"NCNlsMMbFdaMKSNrzuZqTIACmUbkDWPF\"},\"ValueType\":{\"M\":{\"deNBD\":{\"S\":\"VgAOcTDpsNrCEqCuIM\"},\"gMezT\":{\"S\":\"lLUhyzuKSx\"}}}}}"),
	[]byte("{\"Metadata\":{\"WriteTimestampMicros\":{\"N\":\"1746609559682521\"}},\"Keys\":{\"PK\":{\"S\":\"ITEM#53\"},\"SK\":{\"S\":\"METADATA\"}},\"OldImage\":{\"AttrScore\":{\"BOOL\":true},\"ConfigName\":{\"M\":{\"WCEff\":{\"S\":\"tUOdrSuGNVXARoWlQLLD\"},\"aBgXt\":{\"S\":\"uQWCbfWlXJQK\"}}},\"DataName\":{\"N\":\"636\"},\"DataScore\":{\"BS\":[\"WHhVTlhq\",\"ZEZ5VktSS2w=\",\"ZkZXckdEZw==\"]},\"FieldSize\":{\"BS\":[\"Q2tUekV5\",\"SkhnanRjWVNaVw==\",\"ZEJrdW9QRWw=\"]},\"FieldStatus\":{\"BS\":[\"UXd2a1FX\",\"cnF2cmNvSFRiWA==\"]},\"InfoName\":{\"N\":\"847\"},\"InfoScore\":{\"L\":[{\"S\":\"bTWswsjEppLChzwgBQqu\"},{\"S\":\"HfOQHNGuQIGImP\"},{\"S\":\"UmSOOiWdQDVOhOOekeB\"},{\"S\":\"XLrqOoGcHNCajokGX\"}]},\"InfoSize\":{\"B\":\"YWhWYmFVb3pKZ3F1bA==\"},\"MetaLevel\":{\"NULL\":true},\"MetaStatus\":{\"L\":[{\"S\":\"xlixPXCXLRdFyvh\"}]},\"PK\":{\"S\":\"ITEM#53\"},\"SK\":{\"S\":\"METADATA\"},\"SettingCount\":{\"B\":\"eWZEUUZ5cVBteXFBdlZ4dEZDaEg=\"},\"SettingIndex\":{\"L\":[{\"S\":\"cxBxDWRmb\"}]},\"ValueLevel\":{\"BS\":[\"RXF4bVE=\",\"T29PY3c=\",\"VlR0em8=\"]}},\"NewImage\":{\"ConfigCount\":{\"N\":\"489\"},\"ConfigSize\":{\"L\":[{\"S\":\"gsYjYFCZterjfCIomJyY\"},{\"S\":\"yRYLtuDjDKQuw\"}]},\"ConfigType\":{\"M\":{\"Wuyxf\":{\"S\":\"rByvGlwHkbKuyLcj\"},\"dTAkf\":{\"S\":\"miTPkBsd\"}}},\"FieldCount\":{\"S\":\"hnEocJaThRZUZZxUiLlxjmRd\"},\"FieldIndex\":{\"BS\":[\"T0tpalFL\",\"VndTbm9qSGZodQ==\",\"WkxGR0hqQQ==\"]},\"FieldName\":{\"L\":[{\"S\":\"OIQDUqfpVJrGmdh\"},{\"S\":\"KKnJgWQoQHf\"}]},\"FieldStatus\":{\"NS\":[\"42\",\"226\"]},\"InfoLevel\":{\"BOOL\":false},\"InfoScore\":{\"S\":\"ZDXSrnoIYFqVSEBYiutEPXBzklQrDDDLetifdeKQfaZWRY\"},\"MetaCount\":{\"S\":\"sqAMrWCiGKXECdsJqVGOfnOycvUhRRGGL\"},\"MetaName\":{\"NS\":[\"27\",\"349\",\"732\",\"874\",\"964\"]},\"MetaStatus\":{\"NS\":[\"50\",\"684\",\"869\",\"897\"]},\"PK\":{\"S\":\"ITEM#53\"},\"SK\":{\"S\":\"METADATA\"},\"SettingSize\":{\"SS\":[\"dUCgsDOgHpuFhy\",\"ghtLSPV\",\"jFwAmEAjM\"]},\"ValueCount\":{\"NULL\":true},\"ValueLevel\":{\"BOOL\":false}}}"),
}

func TestNewJSONDecoder(t *testing.T) {
	decoder := NewJSONDecoder()
	if decoder == nil {
		t.Fatal("NewJSONDecoder() returned nil")
	}
}

// BenchmarkDecode measures JSON decoding performance for the hot path
func BenchmarkDecode(b *testing.B) {
	decoder := NewJSONDecoder()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, data := range testData {
			_, _ = decoder.Decode(data)
		}
	}
}

// BenchmarkDecodeSingle measures single item decode performance
func BenchmarkDecodeSingle(b *testing.B) {
	decoder := NewJSONDecoder()
	data := testData[0]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = decoder.Decode(data)
	}
}

// BenchmarkDecodeParallel measures parallel decode performance
func BenchmarkDecodeParallel(b *testing.B) {
	decoder := NewJSONDecoder()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for _, data := range testData {
				_, _ = decoder.Decode(data)
			}
		}
	})
}

// TestProcessRealData processes real DynamoDB export data
func TestProcessRealData(t *testing.T) {
	decoder := NewJSONDecoder()

	// Process all test data
	for i, data := range testData {
		op, err := decoder.Decode(data)
		if err != nil {
			t.Fatalf("Failed to decode test data %d: %v", i, err)
		}

		// Verify that operation has a Keys field
		if op.Keys == nil {
			t.Errorf("Test data %d: Keys is nil", i)
		}

		// Check that either NewImage, OldImage, or both are present
		if op.NewImage == nil && op.OldImage == nil {
			t.Errorf("Test data %d: Both NewImage and OldImage are nil", i)
		}

	}
}

// BenchmarkJSONComparison compares encoding/json vs goccy/go-json
func BenchmarkJSONComparison(b *testing.B) {
	data := testData[0]

	b.Run("stdlib", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var raw map[string]stdjson.RawMessage
			_ = stdjson.Unmarshal(data, &raw)
		}
	})

	b.Run("goccy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var raw map[string]goccyjson.RawMessage
			_ = goccyjson.Unmarshal(data, &raw)
		}
	})
}
