package chirpstore

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/creachadair/chirp"
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
	// [1] replace [2] keylen=n [n] key [rest] data
}

// Encode converts p into a binary string for request data.
func (p PutRequest) Encode() []byte {
	buf := make([]byte, 3+len(p.Key)+len(p.Data)) // 1 replace, 2 keylen
	if p.Replace {
		buf[0] = 1
	}
	putBytes(buf, p.Key, 1)
	copy(buf[3:], p.Key)
	copy(buf[3+len(p.Key):], p.Data)
	return buf
}

// UnmarshalBinary data from binary format and replaces the contents of p.
// It implements encoding.BinaryMarshaler.
func (p *PutRequest) UnmarshalBinary(data []byte) error {
	if len(data) < 3 {
		return fmt.Errorf("invalid put request (%d bytes)", len(data))
	}
	_, key, err := getBytes(data, 1)
	if err != nil {
		return fmt.Errorf("invalid put request: %w", err)
	}
	p.Replace = data[0] != 0
	p.Key = key
	p.Data = data[3+len(key):]
	return nil
}

// ListRequest is the an encoding wrapper for the arguments to the List method.
type ListRequest struct {
	Start []byte
	Count int

	// Encoding:
	// [4] count [rest] start
}

// Encode converts r into a binary string for request data.
func (r ListRequest) Encode() []byte {
	buf := make([]byte, 4+len(r.Start)) // 4 count
	binary.BigEndian.PutUint32(buf[0:], uint32(r.Count))
	copy(buf[4:], r.Start)
	return buf
}

// UnmarshalBinary data from binary format and replaces the contents of r.
// It implements encoding.BinaryMarshaler.
func (r *ListRequest) UnmarshalBinary(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("invalid list request (%d bytes)", len(data))
	}
	r.Start = data[4:]
	r.Count = int(binary.BigEndian.Uint32(data[0:]))
	return nil
}

// ListResponse is the an encoding wrapper for the List method response.
type ListResponse struct {
	Keys [][]byte
	Next []byte

	// [2] nlen=n [n] next |: [2] klen=k [k] key :|
}

// Encode converts r into a binary string for response data.
func (r ListResponse) Encode() []byte {
	bufSize := 2 + len(r.Next)
	for _, key := range r.Keys {
		bufSize += 2 + len(key)
	}
	buf := make([]byte, bufSize)
	pos := putBytes(buf, r.Next, 0)
	for _, key := range r.Keys {
		pos = putBytes(buf, key, pos)
	}
	return buf
}

// UnmarshalBinary data from binary format and replaces the contents of r.
// It implements encoding.BinaryMarshaler.
func (r *ListResponse) UnmarshalBinary(data []byte) error {
	pos, next, err := getBytes(data, 0)
	if err != nil {
		return fmt.Errorf("invalid list response: %w", err)
	}
	r.Next = next
	r.Keys = nil
	for pos < len(data) {
		pos, next, err = getBytes(data, pos)
		if err != nil {
			return fmt.Errorf("invalid list response: %w", err)
		}
		r.Keys = append(r.Keys, next)
	}
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

func uint16be(buf []byte) uint16 { return uint16(buf[0])<<8 | uint16(buf[1]) }

func putUint16be(buf []byte, u uint16) {
	buf[0] = byte((u >> 8) & 0xff)
	buf[1] = byte(u & 0xff)
}

func putBytes(buf, src []byte, pos int) int {
	putUint16be(buf[pos:], uint16(len(src)))
	n := copy(buf[pos+2:], src)
	return pos + 2 + n
}

func getBytes(buf []byte, pos int) (int, []byte, error) {
	if pos+2 > len(buf) {
		return pos, nil, errors.New("invalid length")
	}
	nb := int(uint16be(buf[pos:]))
	end := pos + 2 + nb
	if end > len(buf) {
		return pos, nil, errors.New("truncated value")
	}
	return end, buf[pos+2 : end], nil
}

func packInt64(z int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(z))
	return buf[:]
}
