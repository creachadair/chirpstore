package chirpstore

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/creachadair/chirp"
	"github.com/creachadair/ffs/blob"
)

// Store implements the blob.Store interface by calling a Chirp v0 peer.
type Store struct{ peer *chirp.Peer }

// NewStore constructs a Store that delegates through the given peer.
func NewStore(peer *chirp.Peer, opts *StoreOpts) Store { return Store{peer: peer} }

// StoreOpts provide optional settings for a Store peer.
type StoreOpts struct{}

// Get implements a method of blob.Store.
func (s Store) Get(ctx context.Context, key string) ([]byte, error) {
	rsp, err := s.peer.Call(ctx, mGet, []byte(key))
	if err != nil {
		return nil, unfilterErr(err)
	}
	return rsp.Data, nil
}

// Put implements a method of blob.Store.
func (s Store) Put(ctx context.Context, opts blob.PutOptions) error {
	_, err := s.peer.Call(ctx, mPut, PutRequest{
		Key:     []byte(opts.Key),
		Data:    opts.Data,
		Replace: opts.Replace,
	}.Encode())
	return unfilterErr(err)
}

// Delete implements a method of blob.Store.
func (s Store) Delete(ctx context.Context, key string) error {
	_, err := s.peer.Call(ctx, mDelete, []byte(key))
	return unfilterErr(err)
}

// Size implements a method of blob.Store.
func (s Store) Size(ctx context.Context, key string) (int64, error) {
	rsp, err := s.peer.Call(ctx, mSize, []byte(key))
	if err != nil {
		return 0, unfilterErr(err)
	} else if len(rsp.Data) != 8 {
		return 0, errors.New("size: invalid response format")
	}
	return int64(binary.BigEndian.Uint64(rsp.Data)), nil
}

// List implements a method of blob.Store.
func (s Store) List(ctx context.Context, start string, f func(string) error) error {
	next := start
	for {
		// Fetch another batch of keys.
		var rsp ListResponse
		if lres, err := s.peer.Call(ctx, mList, ListRequest{
			Start: []byte(next),
		}.Encode()); err != nil {
			return err
		} else if err := rsp.UnmarshalBinary(lres.Data); err != nil {
			return err
		}
		if len(rsp.Keys) == 0 {
			break
		}

		// Deliver keys to the callback.
		for _, key := range rsp.Keys {
			if err := f(string(key)); err == blob.ErrStopListing {
				return nil
			} else if err != nil {
				return err
			}
		}
		if len(rsp.Next) == 0 {
			break
		}
		next = string(rsp.Next)
	}
	return nil
}

// Len implements a method of blob.Store.
func (s Store) Len(ctx context.Context) (int64, error) {
	rsp, err := s.peer.Call(ctx, mLen, nil)
	if err != nil {
		return 0, err
	} else if len(rsp.Data) != 8 {
		return 0, errors.New("len: invalid response format")
	}
	return int64(binary.BigEndian.Uint64(rsp.Data)), nil
}

// CAS implements the blob.CAS interface by calling a Chirp v0 peer.
type CAS struct {
	Store
}

// NewCAS constructs a CAS that delegates through the given peer.
func NewCAS(peer *chirp.Peer, opts *StoreOpts) CAS {
	return CAS{Store: NewStore(peer, opts)}
}

// CASPut implements part of the blob.CAS type.
func (c CAS) CASPut(ctx context.Context, data []byte) (string, error) {
	rsp, err := c.peer.Call(ctx, mCASPut, data)
	if err != nil {
		return "", err
	}
	return string(rsp.Data), nil
}

// CASKey implements part of the blob.CAS type.
func (c CAS) CASKey(ctx context.Context, data []byte) (string, error) {
	rsp, err := c.peer.Call(ctx, mCASKey, data)
	if err != nil {
		return "", err
	}
	return string(rsp.Data), nil
}
