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
	id := packet.Vint30(r.ID)
	buf := make([]byte, 0, id.EncodedLen()+len(r.Key))
	return append(id.Encode(buf), r.Key...)
}

// Decode data from binary format and replace the contents of g.
func (r *IDKeyRequest) Decode(data []byte) error {
	nb, id := packet.ParseVint30(data)
	if nb < 0 {
		return errors.New("invalid get request (malformed space ID)")
	}
	r.ID = int(id)
	r.Key = data[nb:]
	return nil
}

// IDOnly is a shared type for requests and responses that report only an ID.
type IDOnly struct {
	ID int

	// [V] int
}

// Encode converts r into a binary string.
func (r IDOnly) Encode() []byte {
	var buf [4]byte
	return packet.Vint30(r.ID).Encode(buf[:0])
}

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
	s := packet.Slice{
		packet.Vint30(p.ID),
		packet.Bool(p.Replace),
		packet.Bytes(p.Key),
		packet.Raw(p.Data),
	}
	buf := make([]byte, 0, s.EncodedLen())
	return s.Encode(buf)
}

// Decode data from binary format and replace the contents of p.
func (p *PutRequest) Decode(data []byte) error {
	var id packet.Vint30
	nb, err := packet.Parse(data, &id,
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
	s := packet.Slice{
		packet.Vint30(r.ID),
		packet.Vint30(r.Count),
		packet.Raw(r.Start),
	}
	buf := make([]byte, 0, s.EncodedLen())
	return s.Encode(buf)
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
	s := make(packet.Slice, 1+len(r.Keys))
	s[0] = packet.Bytes(r.Next)
	for i, key := range r.Keys {
		s[i+1] = packet.Bytes(key)
	}
	buf := make([]byte, 0, s.EncodedLen())
	return s.Encode(buf)
}

// Decode data from binary format and replaces the contents of r.
func (r *ListResponse) Decode(data []byte) error {
	nb, next := packet.ParseBytes(data)
	if nb < 0 {
		return errors.New("invalid list response (malformed next-key)")
	}
	r.Next = []byte(next)
	r.Keys = r.Keys[:0]
	data = data[nb:]
	for len(data) != 0 {
		nb, key := packet.ParseBytes(data)
		if nb < 0 {
			return errors.New("invalid list response (malformed key)")
		}
		r.Keys = append(r.Keys, []byte(key))
		data = data[nb:]
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
	id := packet.Vint30(p.ID)
	buf := make([]byte, 0, id.EncodedLen()+len(p.Data))
	return append(id.Encode(buf), p.Data...)
}

// Decode decodes data from binary format and replace the contents of
func (p *CASPutRequest) Decode(data []byte) error {
	nb, id := packet.ParseVint30(data)
	if nb < 0 {
		return errors.New("invalid put request (malformed space ID)")
	}
	p.ID = int(id)
	p.Data = data[nb:]
	return nil
}

// SyncRequest is the encoding wrapper for a sync request message.
type SyncRequest struct {
	ID   int
	Keys [][]byte

	// [V] id |: [Vk] klen [k] key :| */
}

// Encode converts s into a binary string.
func (s SyncRequest) Encode() []byte {
	pkt := make(packet.Slice, len(s.Keys)+1)
	pkt[0] = packet.Vint30(s.ID)
	for i, key := range s.Keys {
		pkt[i+1] = packet.Bytes(key)
	}
	buf := make([]byte, 0, pkt.EncodedLen())
	return pkt.Encode(buf)
}

// Decode parses data into the contents of s.
func (s *SyncRequest) Decode(data []byte) error {
	nb, id := packet.ParseVint30(data)
	if nb < 0 {
		return errors.New("invalid sync request (malformed space ID)")
	}
	s.ID = int(id)
	data = data[nb:]

	s.Keys = s.Keys[:0]
	for len(data) != 0 {
		nb, key := packet.ParseBytes(data)
		if nb < 0 {
			return errors.New("invalid sync data (malformed key)")
		}
		s.Keys = append(s.Keys, key)
		data = data[nb:]
	}
	return nil
}

// SyncResponse is the encoding wrapper for a sync response message.
type SyncResponse struct {
	Missing [][]byte

	// |: [Vk] klen [k] key :| */
}

// Encode converts s into a binary string.
func (s SyncResponse) Encode() []byte {
	pkt := make(packet.Slice, len(s.Missing))
	for i, key := range s.Missing {
		pkt[i] = packet.Bytes(key)
	}
	buf := make([]byte, 0, pkt.EncodedLen())
	return pkt.Encode(buf)
}

// Decode parses data into the contents of s.
func (s *SyncResponse) Decode(data []byte) error {
	s.Missing = s.Missing[:0]
	for len(data) != 0 {
		nb, key := packet.ParseBytes(data)
		if nb < 0 {
			return errors.New("invalid sync data (malformed key)")
		}
		s.Missing = append(s.Missing, key)
		data = data[nb:]
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
	if blob.IsKeyNotFound(err) {
		ed := &chirp.ErrorData{Code: codeKeyNotFound, Message: "key not found"}
		if e, ok := err.(*blob.KeyError); ok {
			ed.Data = []byte(e.Key)
		}
		return ed
	} else if blob.IsKeyExists(err) {
		return &chirp.ErrorData{Code: codeKeyExists, Message: "key exists"}
	}
	return err
}

func unfilterErr(err error) error {
	if ce, ok := err.(*chirp.CallError); ok {
		if ce.Code == codeKeyExists {
			return blob.ErrKeyExists
		} else if ce.Code == codeKeyNotFound {
			if len(ce.Data) != 0 {
				return blob.KeyNotFound(string(ce.Data))
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

func setKeys(target *[][]byte, keys []string) {
	*target = (*target)[:0]
	for _, key := range keys {
		*target = append(*target, []byte(key))
	}
}

func getKeys(keys *[][]byte) []string {
	if len(*keys) == 0 {
		return nil
	}
	out := make([]string, len(*keys))
	for i, key := range *keys {
		out[i] = string(key)
	}
	return out
}
