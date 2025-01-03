package chirpstore

import (
	"context"
	"errors"

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

// Close implements part of the [blob.KV] interface.
func (s KV) Close(_ context.Context) error { return s.peer.Stop() }

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

// Stat implements a method of [blob.KV].
func (s KV) Stat(ctx context.Context, keys ...string) (blob.StatMap, error) {
	if len(keys) == 0 {
		return nil, nil // no sense calling the peer in this case
	}
	sreq := StatRequest{ID: s.spaceID}
	setKeys(&sreq.Keys, keys)
	rsp, err := s.peer.Call(ctx, s.method(mStat), sreq.Encode())
	if err != nil {
		return nil, err
	}
	var srsp StatResponse
	if err := srsp.Decode(rsp.Data); err != nil {
		return nil, err
	}
	out := make(blob.StatMap, len(srsp))
	for _, kv := range srsp {
		out[string(kv.Key)] = blob.Stat{Size: kv.Size}
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
func (s KV) List(ctx context.Context, start string, f func(string) error) error {
	next := start
	for {
		// Fetch another batch of keys.
		var rsp ListResponse
		if lres, err := s.peer.Call(ctx, s.method(mList), ListRequest{
			ID:    s.spaceID,
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
			if err := f(string(key)); errors.Is(err, blob.ErrStopListing) {
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

// SyncKeys implements the [blob.SyncKeyer] interface.
func (s KV) SyncKeys(ctx context.Context, keys []string) ([]string, error) {
	stat, err := s.Stat(ctx, keys...)
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, want := range keys {
		if !stat.Has(want) {
			missing = append(missing, want)
		}
	}
	return missing, nil
}
