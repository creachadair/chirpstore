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

func testRoundTrip(t *testing.T, out, in roundTripper) {
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

func TestTypes(t *testing.T) {
	t.Run("PutRequest", func(t *testing.T) {
		testRoundTrip(t, new(chirpstore.PutRequest), &chirpstore.PutRequest{
			Key:     []byte("hey there delilah"),
			Data:    []byte("what's it like in new york city"),
			Replace: true,
		})
	})
	t.Run("ListRequest", func(t *testing.T) {
		testRoundTrip(t, new(chirpstore.ListRequest), &chirpstore.ListRequest{
			Start: []byte("the coolth of your evening smile"),
			Count: 122,
		})
	})
	t.Run("ListResponse", func(t *testing.T) {
		testRoundTrip(t, new(chirpstore.ListResponse), &chirpstore.ListResponse{
			Next: []byte("toad"),
			Keys: [][]byte{[]byte("crossroads"), []byte("spoonful"), []byte("train time")},
		})
	})
	t.Run("CASPutRequest", func(t *testing.T) {
		testRoundTrip(t, new(chirpstore.CASPutRequest), &chirpstore.CASPutRequest{
			Prefix: []byte("this goes first"),
			Suffix: []byte("this goes last"),
			Data:   []byte("all the data that are fit to print"),
		})
	})
}
