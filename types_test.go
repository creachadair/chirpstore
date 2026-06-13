package chirpstore_test

import (
	"testing"

	"github.com/creachadair/chirpstore"
	"github.com/google/go-cmp/cmp"
)

type roundTripper[T any] interface {
	*T // the receiver must be a pointer

	Encode() []byte
	Decode([]byte) error
}

func testRoundTrip[T any, TP roundTripper[T]](in TP) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		bits := in.Encode()
		t.Logf("Encoding of %T: %+q", in, bits)

		var out TP = new(T)
		if err := out.Decode(bits); err != nil {
			t.Fatalf("UnmarshalBinary: %v", err)
		}
		if diff := cmp.Diff(out, in); diff != "" {
			t.Fatalf("Wrong decoded value (-got, +want):\n%s", diff)
		}
	}
}

func TestTypes(t *testing.T) {
	t.Run("PutRequest", testRoundTrip(&chirpstore.PutRequest{
		ID:      1,
		Key:     []byte("hey there delilah"),
		Data:    []byte("what's it like in new york city"),
		Replace: true,
	}))
	t.Run("ListRequest", testRoundTrip(&chirpstore.ListRequest{
		ID:    2,
		Start: []byte("the coolth of your evening smile"),
		Count: 122,
	}))
	t.Run("ListResponse", testRoundTrip(&chirpstore.ListResponse{
		Next: []byte("toad"),
		Keys: keyBytes("crossroads", "spoonful", "train time"),
	}))
	t.Run("HasRequest", testRoundTrip(&chirpstore.HasRequest{
		ID:   4,
		Keys: []string{"apple", "", "pear", "plum", "cherry"},
	}))
	t.Run("GetRequest", testRoundTrip(&chirpstore.GetRequest{
		ID:  5,
		Key: []byte("fall into sleep, fall into me"),
	}))
	t.Run("KeyspaceRequest", testRoundTrip(&chirpstore.KeyspaceRequest{
		ID:  6,
		Key: []byte("la padrona soy yo"),
	}))
	t.Run("KeyspaceResponse", testRoundTrip(&chirpstore.KeyspaceResponse{
		ID: 7,
	}))
	t.Run("LenRequest", testRoundTrip(&chirpstore.LenRequest{
		ID: 8,
	}))
}

func keyBytes(keys ...string) [][]byte {
	out := make([][]byte, len(keys))
	for i, key := range keys {
		out[i] = []byte(key)
	}
	return out
}
