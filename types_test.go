package chirpstore_test

import (
	"testing"

	"github.com/creachadair/chirpstore"
	"github.com/google/go-cmp/cmp"
)

type roundTripper interface {
	Encode() []byte
	Decode([]byte) error
}

func testRoundTrip(out, in roundTripper) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		bits := in.Encode()
		t.Logf("Encoding: %+q", bits)
		if err := out.Decode(bits); err != nil {
			t.Fatalf("UnmarshalBinary: %v", err)
		}
		if diff := cmp.Diff(in, out); diff != "" {
			t.Fatalf("Wrong decoded value (-want, +got):\n%s", diff)
		}
	}
}

func TestTypes(t *testing.T) {
	t.Run("PutRequest", testRoundTrip(new(chirpstore.PutRequest), &chirpstore.PutRequest{
		Key:     []byte("hey there delilah"),
		Data:    []byte("what's it like in new york city"),
		Replace: true,
	}))
	t.Run("ListRequest", testRoundTrip(new(chirpstore.ListRequest), &chirpstore.ListRequest{
		Start: []byte("the coolth of your evening smile"),
		Count: 122,
	}))
	t.Run("ListResponse", testRoundTrip(new(chirpstore.ListResponse), &chirpstore.ListResponse{
		Next: []byte("toad"),
		Keys: [][]byte{[]byte("crossroads"), []byte("spoonful"), []byte("train time")},
	}))
	t.Run("CASPutRequest", testRoundTrip(new(chirpstore.CASPutRequest), &chirpstore.CASPutRequest{
		Prefix: []byte("this goes first"),
		Suffix: []byte("this goes last"),
		Data:   []byte("all the data that are fit to print"),
	}))
	t.Run("SyncRequest", testRoundTrip(new(chirpstore.SyncRequest), &chirpstore.SyncRequest{
		Keys: [][]byte{[]byte("apple"), []byte("pear"), []byte("plum"), []byte("cherry")},
	}))
}
