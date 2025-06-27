package chirpstore

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/creachadair/chirp"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/ffs/storage/monitor"
)

// Store implements the [blob.StoreCloser] interface by delegating requests to
// a Chirp v0 peer. Store and KV operations are delegated to the remote peer.
type Store struct {
	*monitor.M[chirpStub, KV]
}

// NewStore constructs a Store that delegates through the given peer.
func NewStore(peer *chirp.Peer, opts *StoreOptions) Store {
	return Store{M: monitor.New(monitor.Config[chirpStub, KV]{
		DB: chirpStub{pfx: opts.methodPrefix(), id: 0, peer: peer},
		NewKV: func(ctx context.Context, db chirpStub, pfx dbkey.Prefix, name string) (KV, error) {
			var rsp KeyspaceResponse
			if krsp, err := db.peer.Call(ctx, db.method(mKV), KeyspaceRequest{
				ID:  db.id,
				Key: []byte(name),
			}.Encode()); err != nil {
				return KV{}, err
			} else if rsp.Decode(krsp.Data); err != nil {
				return KV{}, err
			}
			return KV{spaceID: rsp.ID, pfx: db.pfx, peer: db.peer}, nil
		},
		NewSub: func(ctx context.Context, db chirpStub, pfx dbkey.Prefix, name string) (chirpStub, error) {
			var rsp SubResponse
			if srsp, err := db.peer.Call(ctx, db.method(mSub), SubRequest{
				ID:  db.id,
				Key: []byte(name),
			}.Encode()); err != nil {
				return db, err
			} else if rsp.Decode(srsp.Data); err != nil {
				return db, err
			}
			return db.withID(rsp.ID), nil
		},
	})}
}

// StoreOptions provide optional settings for a [Store].
// A nil is ready for use and provides default values.
type StoreOptions struct {
	// A prefix to prepend to all the method names exported by the service.
	MethodPrefix string
}

func (o *StoreOptions) methodPrefix() string {
	if o == nil {
		return ""
	}
	return o.MethodPrefix
}

// chirpStub contains the metadata for a substore.
type chirpStub struct {
	pfx  string
	id   int
	peer *chirp.Peer
}

func (s chirpStub) method(m string) string { return s.pfx + m }

func (s chirpStub) withID(id int) chirpStub { s.id = id; return s }

// Close implements part of the [blob.StoreCloser] interface.
func (s Store) Close(context.Context) error { return s.DB.peer.Stop() }

// KV implements the [blob.KV] interface by calling a Chirp v0 peer.
type KV struct {
	spaceID int
	pfx     string
	peer    *chirp.Peer
}

func (s KV) method(m string) string { return s.pfx + m }

// Get implements a method of [blob.KV].
func (s KV) Get(ctx context.Context, key string) ([]byte, error) {
	rsp, err := s.peer.Call(ctx, s.method(mGet), GetRequest{
		ID:  s.spaceID,
		Key: []byte(key),
	}.Encode())
	if err != nil {
		return nil, unfilterErr(err)
	}
	return rsp.Data, nil
}

// Has implements a method of [blob.KV].
func (s KV) Has(ctx context.Context, keys ...string) (blob.KeySet, error) {
	if len(keys) == 0 {
		return nil, nil // no sense calling the peer in this case
	}
	rsp, err := s.peer.Call(ctx, s.method(mHas), HasRequest{
		ID:   s.spaceID,
		Keys: keys,
	}.Encode())
	if err != nil {
		return nil, err
	} else if w := 8 * len(rsp.Data); w < len(keys) {
		return nil, fmt.Errorf("has: got %d results, want %d", w, len(keys))
	}
	var out blob.KeySet
	for i, key := range keys {
		w := rsp.Data[i/8]
		if w&(1<<(i%8)) != 0 {
			out.Add(key)
		}
	}
	return out, nil
}

// Put implements a method of [blob.KV].
func (s KV) Put(ctx context.Context, opts blob.PutOptions) error {
	_, err := s.peer.Call(ctx, s.method(mPut), PutRequest{
		ID:      s.spaceID,
		Key:     []byte(opts.Key),
		Data:    opts.Data,
		Replace: opts.Replace,
	}.Encode())
	return unfilterErr(err)
}

// Delete implements a method of [blob.KV].
func (s KV) Delete(ctx context.Context, key string) error {
	_, err := s.peer.Call(ctx, s.method(mDelete), DeleteRequest{
		ID:  s.spaceID,
		Key: []byte(key),
	}.Encode())
	return unfilterErr(err)
}

// List implements a method of [blob.KV].
func (s KV) List(ctx context.Context, start string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		next := start
		for {
			// Fetch another batch of keys.
			var rsp ListResponse
			if lres, err := s.peer.Call(ctx, s.method(mList), ListRequest{
				ID:    s.spaceID,
				Start: []byte(next),
			}.Encode()); err != nil {
				yield("", err)
				return
			} else if err := rsp.Decode(lres.Data); err != nil {
				yield("", err)
				return
			}
			if len(rsp.Keys) == 0 {
				break
			}

			// Deliver keys to the callback.
			for _, key := range rsp.Keys {
				if !yield(string(key), nil) {
					return
				}
			}
			if len(rsp.Next) == 0 {
				return
			}
			next = string(rsp.Next)
		}
	}
}

// Len implements a method of [blob.KV].
func (s KV) Len(ctx context.Context) (int64, error) {
	rsp, err := s.peer.Call(ctx, s.method(mLen), LenRequest{
		ID: s.spaceID,
	}.Encode())
	if err != nil {
		return 0, err
	} else if len(rsp.Data) == 0 {
		return 0, errors.New("len: invalid response format")
	}
	return unpackInt64(rsp.Data), nil
}

// Status calls the status method of the store service.
func (s KV) Status(ctx context.Context) ([]byte, error) {
	rsp, err := s.peer.Call(ctx, s.method(mStatus), nil)
	if err != nil {
		return nil, err
	}
	return rsp.Data, nil
}
