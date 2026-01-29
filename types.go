// Package chirpstore implements a Chirp v0 client and server exposing the [blob.KV]
// interface.
//
// See https://github.com/creachadair/chirp for information about Chirp v0.
// See https://godoc.org/creachadair/ffs for information about [blob.KV].
package chirpstore

import (
	"errors"
	"fmt"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/packet"
	"github.com/creachadair/ffs/blob"
)

const (
	codeKeyExists   = 400
	codeKeyNotFound = 404
)

// IDKeyRequest is a shared type for requests that take an ID and a key.
type IDKeyRequest struct {
	ID  int
	Key []byte

	// [V] id [rest] key
}

// Encode converts g into a binary string for request data.
func (r IDKeyRequest) Encode() []byte {
	var b packet.Builder
	b.Grow(packet.Vint30(r.ID).Size() + len(r.Key))
	b.Vint30(uint32(r.ID))
	packet.Append(&b, r.Key)
	return b.Bytes()
}

// Decode data from binary format and replace the contents of g.
func (r *IDKeyRequest) Decode(data []byte) error {
	s := packet.NewScanner(data)
	id, err := s.Vint30()
	if err != nil {
		return fmt.Errorf("invalid get request: %w", err)
	}
	r.ID = id
	r.Key = s.Rest()
	return nil
}

// IDOnly is a shared type for requests and responses that report only an ID.
type IDOnly struct {
	ID int

	// [V] int
}

// Encode converts r into a binary string.
func (r IDOnly) Encode() []byte { return packet.Vint30(r.ID).Append(nil) }

// Decode parses data into the contents of r.
func (r *IDOnly) Decode(data []byte) error {
	s := packet.NewScanner(data)
	id, err := s.Vint30()
	if err != nil {
		return fmt.Errorf("invalid ID request (malformed ID): %w", err)
	} else if s.Len() != 0 {
		return errors.New("extra data after ID")
	}
	r.ID = id
	return nil
}

type GetRequest = IDKeyRequest
type DeleteRequest = IDKeyRequest

// HasRequest is an encoding wrapper for the arguments of the Has method.
type HasRequest struct {
	ID   int
	Keys []string

	// Encoding:
	// [V] id |: [Vk] klen [k] key :|
}

// Encode converts s into a binary string.
func (s HasRequest) Encode() []byte {
	size := packet.Vint30(s.ID).Size()
	for _, key := range s.Keys {
		size += packet.VLen(key)
	}
	var b packet.Builder
	b.Grow(size)
	b.Vint30(uint32(s.ID))
	for _, key := range s.Keys {
		b.VString(key)
	}
	return b.Bytes()
}

// Decode parses data into the contents of s.
func (s *HasRequest) Decode(data []byte) error {
	sc := packet.NewScanner(data)
	id, err := sc.Vint30()
	if err != nil {
		return fmt.Errorf("invalid has request: %w", err)
	}
	s.ID = id
	s.Keys = s.Keys[:0]
	for sc.Len() != 0 {
		key, err := packet.VGet[string](sc)
		if err != nil {
			return fmt.Errorf("invalid has request: malformed key: %w", err)
		}
		s.Keys = append(s.Keys, key)
	}
	return nil
}

// PutRequest is an encoding wrapper for the arguments of the Put method.
type PutRequest struct {
	ID      int
	Key     []byte
	Data    []byte
	Replace bool

	// Encoding:
	// [V] id [1] replace [Vn] keylen [n] key [rest] data
}

// Encode converts p into a binary string for request data.
func (p PutRequest) Encode() []byte {
	var b packet.Builder
	b.Vint30(uint32(p.ID))
	b.Bool(p.Replace)
	b.VBytes(p.Key)
	packet.Append(&b, p.Data)
	return b.Bytes()
}

// Decode data from binary format and replace the contents of p.
func (p *PutRequest) Decode(data []byte) error {
	s := packet.NewScanner(data)
	id, err := s.Vint30()
	if err != nil {
		return fmt.Errorf("invalid put request: %w", err)
	}
	p.ID = id
	p.Replace, err = s.Bool()
	if err != nil {
		return fmt.Errorf("invalid put request: %w", err)
	}
	p.Key, err = packet.VGet[[]byte](s)
	if err != nil {
		return fmt.Errorf("invalid put request: %w", err)
	}
	p.Data = s.Rest()
	return nil
}

// ListRequest is the an encoding wrapper for the arguments to the List method.
type ListRequest struct {
	ID    int
	Start []byte
	Count int

	// Encoding:
	// [V] id [V] count [rest] start
}

// Encode converts r into a binary string for request data.
func (r ListRequest) Encode() []byte {
	var b packet.Builder
	b.Vint30(uint32(r.ID))
	b.Vint30(uint32(r.Count))
	packet.Append(&b, r.Start)
	return b.Bytes()
}

// Decode data from binary format and replace the contents of r.
func (r *ListRequest) Decode(data []byte) error {
	s := packet.NewScanner(data)
	id, err := s.Vint30()
	if err != nil {
		return fmt.Errorf("invalid list request: %w", err)
	}
	r.ID = id
	r.Count, err = s.Vint30()
	if err != nil {
		return fmt.Errorf("invalid list request: %w", err)
	}
	r.Start = s.Rest()
	return nil
}

// ListResponse is the an encoding wrapper for the List method response.
type ListResponse struct {
	Keys [][]byte
	Next []byte

	// [Vn] nlen [n] next |: [Vk] klen [k] key :|
}

// Encode converts r into a binary string for response data.
func (r ListResponse) Encode() []byte {
	size := packet.VLen(r.Next)
	for _, key := range r.Keys {
		size += packet.VLen(key)
	}
	var b packet.Builder
	b.Grow(size)
	b.VBytes(r.Next)
	for _, key := range r.Keys {
		b.VBytes(key)
	}
	return b.Bytes()
}

// Decode data from binary format and replaces the contents of r.
func (r *ListResponse) Decode(data []byte) error {
	s := packet.NewScanner(data)
	next, err := packet.VGet[[]byte](s)
	if err != nil {
		return fmt.Errorf("invalid list response: %w", err)
	}
	r.Next = next
	r.Keys = r.Keys[:0]
	for s.Len() != 0 {
		key, err := packet.VGet[[]byte](s)
		if err != nil {
			return fmt.Errorf("invalid list response: %w", err)
		}
		r.Keys = append(r.Keys, key)
	}
	return nil
}

// KeyspaceRequest is the encoding wrapper for a Keyspace request.
type KeyspaceRequest = IDKeyRequest

// KeyspaceResponse is the encoding wrapper for a Keyspace response.
type KeyspaceResponse = IDOnly

// SubRequest is the encoding wrapper for a Sub request.
type SubRequest = IDKeyRequest

// SubResponse is the encoding wrapper for a Sub response.
type SubResponse = IDOnly

// LenRequest is the encoding wrapper for a Len request.
type LenRequest = IDOnly

func filterErr(err error) error {
	var kerr *blob.KeyError

	if blob.IsKeyNotFound(err) {
		ed := &chirp.ErrorData{Code: codeKeyNotFound, Message: "key not found"}
		if errors.As(err, &kerr) {
			ed.Data = []byte(kerr.Key)
		}
		return ed
	} else if blob.IsKeyExists(err) {
		ed := &chirp.ErrorData{Code: codeKeyExists, Message: "key exists"}
		if errors.As(err, &kerr) {
			ed.Data = []byte(kerr.Key)
		}
		return ed
	}
	return err
}

func unfilterErr(err error) error {
	var ce *chirp.CallError
	if errors.As(err, &ce) {
		key := string(ce.Data)

		if ce.Code == codeKeyExists {
			if key != "" {
				return blob.KeyExists(key)
			}
			return blob.ErrKeyExists
		} else if ce.Code == codeKeyNotFound {
			if key != "" {
				return blob.KeyNotFound(key)
			}
			return blob.ErrKeyNotFound
		}
		// fall through
	}
	return err
}

func packInt64(z int64) []byte {
	var buf [8]byte
	if z == 0 {
		return buf[:1]
	}
	i := 0
	for u := uint64(z); u != 0; u >>= 8 {
		buf[i] = byte(u & 0xff)
		i++
	}
	return buf[:i]
}

func unpackInt64(buf []byte) int64 {
	var v uint64
	for i := len(buf) - 1; i >= 0; i-- {
		v = (v << 8) | uint64(buf[i])
	}
	return int64(v)
}
