package writer

import (
	"bytes"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// appendHandedBack appends to rejected the index of every request in call that the
// table handed back unprocessed, where the call began at index offset of its batch.
//
// The response carries the handed-back requests themselves, decoded afresh, so they are
// matched to what was sent by content. The writer does not know the table's key schema,
// and a full export's records carry no key of their own, so whole items are compared.
// A request that matches nothing sent is a response the writer does not understand;
// rather than risk leaving an item out, the whole call is reported handed back. Writing
// an item twice is harmless, since every write replaces or removes a whole item.
//
// This runs only when the table hands something back, so its cost, a comparison of
// every handed-back item with every item sent, is paid only when writes are refused.
func appendHandedBack(rejected []int, call []types.WriteRequest, offset int, handedBack []types.WriteRequest) []int {
	var matched [MaxBatch]bool
	found := make([]int, 0, len(handedBack))
	for _, back := range handedBack {
		hit := -1
		for i := range call {
			if !matched[i] && sameRequest(call[i], back) {
				hit = i
				break
			}
		}
		if hit < 0 {
			for i := range call {
				rejected = append(rejected, offset+i)
			}
			return rejected
		}
		matched[hit] = true
		found = append(found, offset+hit)
	}
	return append(rejected, found...)
}

// sameRequest reports whether two write requests put the same item or delete the same
// key.
func sameRequest(a, b types.WriteRequest) bool {
	switch {
	case a.PutRequest != nil && b.PutRequest != nil:
		return sameItem(a.PutRequest.Item, b.PutRequest.Item)
	case a.DeleteRequest != nil && b.DeleteRequest != nil:
		return sameItem(a.DeleteRequest.Key, b.DeleteRequest.Key)
	}
	return false
}

// sameItem reports whether two items hold the same attributes with the same values.
func sameItem(a, b map[string]types.AttributeValue) bool {
	if len(a) != len(b) {
		return false
	}
	for name, av := range a {
		bv, ok := b[name]
		if !ok || !sameValue(av, bv) {
			return false
		}
	}
	return true
}

// sameValue reports whether two attribute values are the same type holding the same
// value. Sets are compared in the order they hold their members, which is the order the
// writer sent them in; a response that reordered one only costs a rewrite.
func sameValue(a, b types.AttributeValue) bool {
	switch av := a.(type) {
	case *types.AttributeValueMemberS:
		bv, ok := b.(*types.AttributeValueMemberS)
		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberN:
		bv, ok := b.(*types.AttributeValueMemberN)
		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberB:
		bv, ok := b.(*types.AttributeValueMemberB)
		return ok && bytes.Equal(av.Value, bv.Value)
	case *types.AttributeValueMemberBOOL:
		bv, ok := b.(*types.AttributeValueMemberBOOL)
		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberNULL:
		bv, ok := b.(*types.AttributeValueMemberNULL)
		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberSS:
		bv, ok := b.(*types.AttributeValueMemberSS)
		return ok && sameStrings(av.Value, bv.Value)
	case *types.AttributeValueMemberNS:
		bv, ok := b.(*types.AttributeValueMemberNS)
		return ok && sameStrings(av.Value, bv.Value)
	case *types.AttributeValueMemberBS:
		bv, ok := b.(*types.AttributeValueMemberBS)
		if !ok || len(av.Value) != len(bv.Value) {
			return false
		}
		for i := range av.Value {
			if !bytes.Equal(av.Value[i], bv.Value[i]) {
				return false
			}
		}
		return true
	case *types.AttributeValueMemberL:
		bv, ok := b.(*types.AttributeValueMemberL)
		if !ok || len(av.Value) != len(bv.Value) {
			return false
		}
		for i := range av.Value {
			if !sameValue(av.Value[i], bv.Value[i]) {
				return false
			}
		}
		return true
	case *types.AttributeValueMemberM:
		bv, ok := b.(*types.AttributeValueMemberM)
		return ok && sameItem(av.Value, bv.Value)
	}
	// A type this build does not know cannot be shown equal, which reports the whole
	// call handed back rather than guessing.
	return false
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
