package chirpstore

import (
	"context"
	"errors"

	"github.com/creachadair/chirp"
	"github.com/creachadair/ffs/blob"
)

// Store implements the blob.Store interface by calling a Chirp v0 peer.
// It also supports the blob.CAS and blob.SyncKeyer extension interfaces.
type Store struct {
	pfx  string
	peer *chirp.Peer
}

// NewStore constructs a Store that delegates through the given peer.
func NewStore(peer *chirp.Peer, opts *StoreOpts) Store { return Store{pfx: opts.prefix(), peer: peer} }

// StoreOpts provide optional settings for a Store peer.
type StoreOpts struct {
	// A prefix to prepend to all the method names exported by the service.
	Prefix string
}

func (o *StoreOpts) prefix() string {
	if o == nil {
		return ""
	}
	return o.Prefix
}

func (s Store) method(m string) string { return s.pfx + m }

// Close implements the blob.Closer interface.
func (s Store) Close(_ context.Context) error { return s.peer.Stop() }

// Get implements a method of blob.Store.
func (s Store) Get(ctx context.Context, key string) ([]byte, error) {
	rsp, err := s.peer.Call(ctx, s.method(mGet), []byte(key))
	if err != nil {
		return nil, unfilterErr(err)
	}
	return rsp.Data, nil
}

// Put implements a method of blob.Store.
func (s Store) Put(ctx context.Context, opts blob.PutOptions) error {
	_, err := s.peer.Call(ctx, s.method(mPut), PutRequest{
		Key:     []byte(opts.Key),
		Data:    opts.Data,
		Replace: opts.Replace,
	}.Encode())
	return unfilterErr(err)
}

// Delete implements a method of blob.Store.
func (s Store) Delete(ctx context.Context, key string) error {
	_, err := s.peer.Call(ctx, s.method(mDelete), []byte(key))
	return unfilterErr(err)
}

// List implements a method of blob.Store.
func (s Store) List(ctx context.Context, start string, f func(string) error) error {
	next := start
	for {
		// Fetch another batch of keys.
		var rsp ListResponse
		if lres, err := s.peer.Call(ctx, s.method(mList), ListRequest{
			Start: []byte(next),
		}.Encode()); err != nil {
			return err
		} else if err := rsp.Decode(lres.Data); err != nil {
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
	rsp, err := s.peer.Call(ctx, s.method(mLen), nil)
	if err != nil {
		return 0, err
	} else if len(rsp.Data) == 0 {
		return 0, errors.New("len: invalid response format")
	}
	return unpackInt64(rsp.Data), nil
}

// Status calls the status method of the store service.
func (s Store) Status(ctx context.Context) ([]byte, error) {
	rsp, err := s.peer.Call(ctx, s.method(mStatus), nil)
	if err != nil {
		return nil, err
	}
	return rsp.Data, nil
}

// SyncKeys implements part of the blob.SyncKeyer interface.
func (s Store) SyncKeys(ctx context.Context, keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil // no sense calling the peer in this case
	}
	var sreq SyncRequest
	sreq.setKeys(keys)
	rsp, err := s.peer.Call(ctx, s.method(mSyncKeys), sreq.Encode())
	if err != nil {
		return nil, err
	}
	var srsp SyncResponse
	if err := srsp.Decode(rsp.Data); err != nil {
		return nil, err
	}
	return srsp.getKeys(), nil
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
func (c CAS) CASPut(ctx context.Context, opts blob.CASPutOptions) (string, error) {
	rsp, err := c.peer.Call(ctx, c.method(mCASPut), CASPutRequest{
		Data:   opts.Data,
		Prefix: []byte(opts.Prefix),
		Suffix: []byte(opts.Suffix),
	}.Encode())
	if err != nil {
		return "", err
	}
	return string(rsp.Data), nil
}

// CASKey implements part of the blob.CAS type.
func (c CAS) CASKey(ctx context.Context, opts blob.CASPutOptions) (string, error) {
	rsp, err := c.peer.Call(ctx, c.method(mCASKey), CASPutRequest{
		Data:   opts.Data,
		Prefix: []byte(opts.Prefix),
		Suffix: []byte(opts.Suffix),
	}.Encode())
	if err != nil {
		return "", err
	}
	return string(rsp.Data), nil
}
