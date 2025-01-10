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
	return packet.Slice{packet.Vint30(r.ID), packet.Raw(r.Key)}.Encode(nil)
}

// Decode data from binary format and replace the contents of g.
func (r *IDKeyRequest) Decode(data []byte) error {
	var err error
	r.ID, r.Key, err = parseIDAndData(data)
	if err != nil {
		return fmt.Errorf("invalid get request: %w", err)
	}
	return nil
}

// IDOnly is a shared type for requests and responses that report only an ID.
type IDOnly struct {
	ID int

	// [V] int
}

// Encode converts r into a binary string.
func (r IDOnly) Encode() []byte { return packet.Vint30(r.ID).Encode(nil) }

// Decode parses data into the contents of r.
func (r *IDOnly) Decode(data []byte) error {
	nb, id := packet.ParseVint30(data)
	if nb < 0 {
		return errors.New("invalid ID request (malformed ID)")
	} else if len(data) > nb {
		return errors.New("extra data after ID")
	}
	r.ID = int(id)
	return nil
}

type GetRequest = IDKeyRequest
type DeleteRequest = IDKeyRequest

// HasRequest is an encoding wrapper for the arguments of the Has method.
type HasRequest struct {
	ID   int
	Keys [][]byte

	// Encoding:
	// [V] id |: [Vk] klen [k] key :|
}

// Encode converts s into a binary string.
func (s HasRequest) Encode() []byte {
	return packet.Slice{packet.Vint30(s.ID), packet.MBytes(s.Keys)}.Encode(nil)
}

// Decode parses data into the contents of s.
func (s *HasRequest) Decode(data []byte) error {
	id, rest, err := parseIDAndData(data)
	if err != nil {
		return fmt.Errorf("invalid has request: %w", err)
	}
	if (*packet.MBytes)(&s.Keys).Decode(rest) < 0 {
		return errors.New("invalid has request: malformed keys")
	}
	s.ID = id
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
	return packet.Slice{
		packet.Vint30(p.ID),
		packet.Bool(p.Replace),
		packet.Bytes(p.Key),
		packet.Raw(p.Data),
	}.Encode(nil)
}

// Decode data from binary format and replace the contents of p.
func (p *PutRequest) Decode(data []byte) error {
	var id packet.Vint30
	nb, err := packet.Parse(data,
		&id,
		(*packet.Bool)(&p.Replace),
		(*packet.Bytes)(&p.Key),
	)
	if err != nil {
		return fmt.Errorf("invalid put request: %w", err)
	}
	p.ID = int(id)
	p.Data = data[nb:]
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
	return packet.Slice{
		packet.Vint30(r.ID),
		packet.Vint30(r.Count),
		packet.Raw(r.Start),
	}.Encode(nil)
}

// Decode data from binary format and replace the contents of r.
func (r *ListRequest) Decode(data []byte) error {
	var id, count packet.Vint30
	nb, err := packet.Parse(data, &id, &count)
	if err != nil {
		return fmt.Errorf("invalid list request: %w", err)
	}
	r.ID = int(id)
	r.Count = int(count)
	r.Start = data[nb:]
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
	return packet.Slice{packet.Bytes(r.Next), packet.MBytes(r.Keys)}.Encode(nil)
}

// Decode data from binary format and replaces the contents of r.
func (r *ListResponse) Decode(data []byte) error {
	_, err := packet.Parse(data,
		(*packet.Bytes)(&r.Next),
		(*packet.MBytes)(&r.Keys),
	)
	if err != nil {
		return fmt.Errorf("invalid list response: %w", err)
	}
	return nil
}

// CASPutRequest is the encoding wrapper for the CASPut method.
type CASPutRequest struct {
	ID   int
	Data []byte

	// Encoding:
	// [V] id [rest] data
}

// Encode converts p into a binary string for request data.
func (p CASPutRequest) Encode() []byte {
	return packet.Slice{packet.Vint30(p.ID), packet.Raw(p.Data)}.Encode(nil)
}

// Decode decodes data from binary format and replace the contents of
func (p *CASPutRequest) Decode(data []byte) error {
	var err error
	p.ID, p.Data, err = parseIDAndData(data)
	if err != nil {
		return fmt.Errorf("invalid CAS put request: %w", err)
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

func parseIDAndData(data []byte) (int, []byte, error) {
	nb, id := packet.ParseVint30(data)
	if nb < 0 {
		return 0, nil, errors.New("malformed ID")
	}
	return int(id), data[nb:], nil
}
