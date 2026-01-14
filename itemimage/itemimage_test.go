package itemimage

import (
	"testing"

	stdjson "encoding/json"

	goccyjson "github.com/goccy/go-json"
)

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
