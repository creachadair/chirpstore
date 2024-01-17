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

// PutRequest is an encoding wrapper for the arguments of the Put method.
type PutRequest struct {
	Key     []byte
	Data    []byte
	Replace bool

	// Encoding:
	// [1] replace [Vn] keylen [n] key [rest] data
}

// Encode converts p into a binary string for request data.
func (p PutRequest) Encode() []byte {
	s := packet.Slice{
		packet.Bool(p.Replace),
		packet.Bytes(p.Key),
		packet.Raw(p.Data),
	}
	buf := make([]byte, 0, s.EncodedLen())
	return s.Encode(buf)
}

// Decode data from binary format and replace the contents of p.
func (p *PutRequest) Decode(data []byte) error {
	nb, err := packet.Parse(data,
		(*packet.Bool)(&p.Replace),
		(*packet.Bytes)(&p.Key),
	)
	if err != nil {
		return fmt.Errorf("invalid put request: %w", err)
	}
	p.Data = data[nb:]
	return nil
}

// ListRequest is the an encoding wrapper for the arguments to the List method.
type ListRequest struct {
	Start []byte
	Count int

	// Encoding:
	// [V] count [rest] start
}

// Encode converts r into a binary string for request data.
func (r ListRequest) Encode() []byte {
	s := packet.Slice{packet.Vint30(r.Count), packet.Raw(r.Start)}
	buf := make([]byte, 0, s.EncodedLen())
	return s.Encode(buf)
}

// Decode data from binary format and replace the contents of r.
func (r *ListRequest) Decode(data []byte) error {
	nb, c := packet.ParseVint30(data)
	if nb < 0 {
		return fmt.Errorf("invalid list request (%d bytes)", len(data))
	}
	r.Count = int(c)
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
	Prefix, Suffix []byte
	Data           []byte

	// Encoding:
	// [Vp] plen [p] prefix [Vs] slen [s] suffix [rest] data
}

// Encode converts p into a binary string for request data.
func (p CASPutRequest) Encode() []byte {
	s := packet.Slice{packet.Bytes(p.Prefix), packet.Bytes(p.Suffix), packet.Raw(p.Data)}
	buf := make([]byte, 0, s.EncodedLen())
	return s.Encode(buf)
}

// Decode decodes data from binary format and replace the contents of
func (p *CASPutRequest) Decode(data []byte) error {
	nb, err := packet.Parse(data,
		(*packet.Bytes)(&p.Prefix),
		(*packet.Bytes)(&p.Suffix),
	)
	if err != nil {
		return fmt.Errorf("invalid CAS put request: %w", err)
	}
	p.Data = data[nb:]
	return nil
}

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
