package chirpstore

import (
	"context"
	"errors"
	"sync"

	"github.com/creachadair/chirp"
	"github.com/creachadair/ffs/blob"
)

// Store implements the [blob.StoreCloser] interface by delegating requests to
// a Chirp v0 peer. Store and KV operations are delegated to the remote peer.
type Store struct {
	*storeStub // root (embedded to implement blob.Store)
}

// NewStore constructs a Store that delegates through the given peer.
func NewStore(peer *chirp.Peer, opts *StoreOptions) Store {
	init := storeStub{pfx: opts.methodPrefix(), peer: peer}
	return Store{storeStub: init.child(0)}
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

// storeStub is the root of a (sub)tree of stores and KVs.
type storeStub struct {
	pfx  string
	id   int
	peer *chirp.Peer

	μ    sync.Mutex
	subs map[string]*storeStub
	kvs  map[string]KV
}

// child constructs a new empty stub with the specified ID, using the same
// method prefix and peer as s.
func (s *storeStub) child(id int) *storeStub {
	return &storeStub{
		pfx:  s.pfx,
		id:   id,
		peer: s.peer,
		subs: make(map[string]*storeStub),
		kvs:  make(map[string]KV),
	}
}

// Keyspace implements part of the [blob.Store] interface.
// The concrete type of a successful result is [KV], which implements the
// [blob.CAS] and [blob.SyncKeyer] extension interfaces.
func (s *storeStub) Keyspace(ctx context.Context, name string) (blob.KV, error) {
	s.μ.Lock()
	defer s.μ.Unlock()

	kv, ok := s.kvs[name]
	if !ok {
		// We're holding the lock here during the call. I deem this is OK because
		// keyspace requests are infrequent and generally done at or near program
		// initialization. A program for which this is a performance gap should
		// reconsider its life choices (and maybe file an issue).
		var rsp KeyspaceResponse
		if krsp, err := s.peer.Call(ctx, s.method(mKeyspace), KeyspaceRequest{
			ID:  s.id,
			Key: []byte(name),
		}.Encode()); err != nil {
			return nil, err
		} else if rsp.Decode(krsp.Data); err != nil {
			return nil, err
		}
		kv = KV{spaceID: rsp.ID, pfx: s.pfx, peer: s.peer}
		s.kvs[name] = kv
	}
	return kv, nil
}

// Sub implements part of the [blob.Store] interface.
func (s *storeStub) Sub(ctx context.Context, name string) (blob.Store, error) {
	s.μ.Lock()
	defer s.μ.Unlock()

	sub, ok := s.subs[name]
	if !ok {
		// We're holding the lock here during the call. I deem this is OK because
		// substore requests are infrequent and generally done at or near program
		// initialization. A program for which this is a performance gap should
		// reconsider its life choices (and maybe file an issue).
		var rsp SubResponse
		if srsp, err := s.peer.Call(ctx, s.method(mSubstore), SubRequest{
			ID:  s.id,
			Key: []byte(name),
		}.Encode()); err != nil {
			return nil, err
		} else if rsp.Decode(srsp.Data); err != nil {
			return nil, err
		}
		sub = s.child(rsp.ID)
		s.subs[name] = sub
	}
	return sub, nil
}

func (s *storeStub) method(m string) string { return s.pfx + m }

// Close implements part of the [blob.StoreCloser] interface.
func (s Store) Close(context.Context) error { return s.storeStub.peer.Stop() }

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
	if len(keys) == 0 {
		return nil, nil // no sense calling the peer in this case
	}
	sreq := SyncRequest{ID: s.spaceID}
	setKeys(&sreq.Keys, keys)
	rsp, err := s.peer.Call(ctx, s.method(mSyncKeys), sreq.Encode())
	if err != nil {
		return nil, err
	}
	var srsp SyncResponse
	if err := srsp.Decode(rsp.Data); err != nil {
		return nil, err
	}
	return getKeys(&srsp.Missing), nil
}

// CASPut implements part of the [blob.CAS] interface.
func (s KV) CASPut(ctx context.Context, opts blob.CASPutOptions) (string, error) {
	rsp, err := s.peer.Call(ctx, s.method(mCASPut), CASPutRequest{
		ID:     s.spaceID,
		Data:   opts.Data,
		Prefix: []byte(opts.Prefix),
		Suffix: []byte(opts.Suffix),
	}.Encode())
	if err != nil {
		return "", err
	}
	return string(rsp.Data), nil
}

// CASKey implements part of the [blob.CAS] interface.
func (s KV) CASKey(ctx context.Context, opts blob.CASPutOptions) (string, error) {
	rsp, err := s.peer.Call(ctx, s.method(mCASKey), CASPutRequest{
		ID:     s.spaceID,
		Data:   opts.Data,
		Prefix: []byte(opts.Prefix),
		Suffix: []byte(opts.Suffix),
	}.Encode())
	if err != nil {
		return "", err
	}
	return string(rsp.Data), nil
}
