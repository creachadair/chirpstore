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
		ID:      1,
		Key:     []byte("hey there delilah"),
		Data:    []byte("what's it like in new york city"),
		Replace: true,
	}))
	t.Run("ListRequest", testRoundTrip(new(chirpstore.ListRequest), &chirpstore.ListRequest{
		ID:    2,
		Start: []byte("the coolth of your evening smile"),
		Count: 122,
	}))
	t.Run("ListResponse", testRoundTrip(new(chirpstore.ListResponse), &chirpstore.ListResponse{
		Next: []byte("toad"),
		Keys: keyBytes("crossroads", "spoonful", "train time"),
	}))
	t.Run("CASPutRequest", testRoundTrip(new(chirpstore.CASPutRequest), &chirpstore.CASPutRequest{
		ID:   3,
		Data: []byte("all the data that are fit to print"),
	}))
	t.Run("StatRequest", testRoundTrip(new(chirpstore.StatRequest), &chirpstore.StatRequest{
		ID:   4,
		Keys: keyBytes("apple", "pear", "plum", "cherry"),
	}))
	t.Run("StatResponse", testRoundTrip(new(chirpstore.StatResponse), &chirpstore.StatResponse{
		{Key: []byte("klaatu"), Size: 17},
		{Key: []byte("barada"), Size: 1951},
		{Key: []byte("nikto"), Size: 1992},
	}))
	t.Run("GetRequest", testRoundTrip(new(chirpstore.GetRequest), &chirpstore.GetRequest{
		ID:  5,
		Key: []byte("fall into sleep, fall into me"),
	}))
	t.Run("KeyspaceRequest", testRoundTrip(new(chirpstore.KeyspaceRequest), &chirpstore.KeyspaceRequest{
		ID:  6,
		Key: []byte("la padrona soy yo"),
	}))
	t.Run("KeyspaceResponse", testRoundTrip(new(chirpstore.KeyspaceResponse), &chirpstore.KeyspaceResponse{
		ID: 7,
	}))
	t.Run("LenRequest", testRoundTrip(new(chirpstore.LenRequest), &chirpstore.LenRequest{
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
