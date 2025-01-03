package chirpstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/creachadair/chirp"
	"github.com/creachadair/ffs/blob"
)

// Constants defining the method names for the store service.
const (
	// Metadata.
	mStatus = "status"

	// Keyspace (KV) methods.
	mGet    = "get"
	mStat   = "stat"
	mPut    = "put"
	mDelete = "delete"
	mList   = "list"
	mLen    = "len"
	mCASPut = "cas-put"
	mCASKey = "cas-key"

	// Store methods.
	mKV  = "kv"
	mCAS = "cas" // alias for mKV
	mSub = "sub"
)

type Service struct {
	pfx string

	μ      sync.Mutex
	lastID int
	subs   map[int]*storeInfo
	kvs    map[int]blob.KV
}

// NewService constructs a service that delegates to the given [blob.KV].
func NewService(st blob.Store, opts *ServiceOptions) *Service {
	s := &Service{
		pfx:  opts.prefix(),
		subs: map[int]*storeInfo{0: newStoreInfo(st)},
		kvs:  make(map[int]blob.KV),
	}
	return s
}

type storeInfo struct {
	store blob.Store
	subs  map[string]int // name to store ID
	kvs   map[string]int // name to keyspace ID
}

func newStoreInfo(st blob.Store) *storeInfo {
	return &storeInfo{store: st, subs: make(map[string]int), kvs: make(map[string]int)}
}

// ServiceOptions provides optional settings for constructing a [Service].
type ServiceOptions struct {
	// A prefix to prepend to all the method names exported by the service.
	Prefix string
}

func (o *ServiceOptions) prefix() string {
	if o == nil {
		return ""
	}
	return o.Prefix
}

func (s *Service) method(m string) string { return s.pfx + m }

// Register adds method handlers to p for each of the applicable methods of s.
func (s *Service) Register(p *chirp.Peer) {
	p.Handle(s.method(mStatus), s.Status)
	p.Handle(s.method(mGet), s.Get)
	p.Handle(s.method(mStat), s.Stat)
	p.Handle(s.method(mPut), s.Put)
	p.Handle(s.method(mDelete), s.Delete)
	p.Handle(s.method(mList), s.List)
	p.Handle(s.method(mLen), s.Len)
	p.Handle(s.method(mKV), s.KV)
	p.Handle(s.method(mCAS), s.KV) // alias for "kv", the server treats them the same
	p.Handle(s.method(mSub), s.Sub)
}

// KV implements the eponymous method of the [blob.Store] interface.
// The client is returned an integer descriptor (ID) that must be presented in
// subsequent requests to identify which keyspace to affect.
func (s *Service) KV(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var kreq KeyspaceRequest
	if err := kreq.Decode(req.Data); err != nil {
		return nil, err
	}
	s.μ.Lock()
	defer s.μ.Unlock()

	si := s.subs[kreq.ID]
	if si == nil {
		return nil, fmt.Errorf("invalid store ID %d", kreq.ID)
	}
	name := string(kreq.Key)
	kvID, ok := si.kvs[name]
	if !ok {
		kv, err := si.store.KV(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("create keyspace %q in store %d: %w", name, kreq.ID, err)
		}
		s.lastID++
		kvID = s.lastID
		s.kvs[kvID] = kv
		si.kvs[name] = kvID
	}
	return KeyspaceResponse{ID: kvID}.Encode(), nil
}

// Sub implements the eponymous method of the [blob.Store] interface.
// The client is returned an integer descriptor (ID) that must be presented in
// subsequent substore and keyspace requests to identify which store to affect.
func (s *Service) Sub(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var sreq SubRequest
	if err := sreq.Decode(req.Data); err != nil {
		return nil, err
	}
	s.μ.Lock()
	defer s.μ.Unlock()

	si := s.subs[sreq.ID]
	if si == nil {
		return nil, fmt.Errorf("invalid store ID %d", sreq.ID)
	}
	name := string(sreq.Key)
	subID, ok := si.subs[name]
	if !ok {
		sub, err := si.store.Sub(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("create substore %q in store %d: %w", name, sreq.ID, err)
		}
		s.lastID++
		subID = s.lastID
		s.subs[subID] = newStoreInfo(sub)
		si.subs[name] = subID
	}
	return SubResponse{ID: subID}.Encode(), nil
}

// Status returns a JSON blob of server metrics.
func (s *Service) Status(ctx context.Context, req *chirp.Request) ([]byte, error) {
	// TODO(creachadair): Add some metrics about substore and keyspace usage.
	mx := chirp.ContextPeer(ctx).Metrics()
	return []byte(mx.String()), nil
}

// Get handles the corresponding method of [blob.KV].
func (s *Service) Get(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var greq GetRequest
	if err := greq.Decode(req.Data); err != nil {
		return nil, err
	}
	kv := s.idToKV(greq.ID)
	if kv == nil {
		return invalidKeyspaceID(greq.ID)
	}
	data, err := kv.Get(ctx, string(greq.Key))
	return data, filterErr(err)
}

// Stat handles the corresponding method of [blob.KV].
func (s *Service) Stat(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var sreq StatRequest
	if err := sreq.Decode(req.Data); err != nil {
		return nil, err
	}
	kv := s.idToKV(sreq.ID)
	if kv == nil {
		return invalidKeyspaceID(sreq.ID)
	}
	keys := make([]string, len(sreq.Keys))
	for i, key := range sreq.Keys {
		keys[i] = string(key)
	}
	data, err := kv.Stat(ctx, keys...)
	if err != nil {
		return nil, filterErr(err)
	}
	srsp := make(StatResponse, 0, len(data))
	for key, stat := range data {
		srsp = append(srsp, keyStat{Key: []byte(key), Size: stat.Size})
	}
	return srsp.Encode(), nil
}

// Put handles the corresponding method of [blob.KV].
func (s *Service) Put(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var preq PutRequest
	if err := preq.Decode(req.Data); err != nil {
		return nil, err
	}
	kv := s.idToKV(preq.ID)
	if kv == nil {
		return invalidKeyspaceID(preq.ID)
	}
	return nil, filterErr(kv.Put(ctx, blob.PutOptions{
		Key:     string(preq.Key),
		Data:    preq.Data,
		Replace: preq.Replace,
	}))
}

// Delete handles the corresponding method of [blob.KV].
func (s *Service) Delete(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var dreq DeleteRequest
	if err := dreq.Decode(req.Data); err != nil {
		return nil, err
	}
	kv := s.idToKV(dreq.ID)
	if kv == nil {
		return invalidKeyspaceID(dreq.ID)
	}
	return nil, filterErr(kv.Delete(ctx, string(dreq.Key)))
}

// List handles the corresponding method of [blob.KV].
func (s *Service) List(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var lreq ListRequest
	if err := lreq.Decode(req.Data); err != nil {
		return nil, err
	}
	kv := s.idToKV(lreq.ID)
	if kv == nil {
		return invalidKeyspaceID(lreq.ID)
	}

	limit := lreq.Count
	if limit <= 0 {
		limit = 256
	}

	var lrsp ListResponse
	if err := kv.List(ctx, string(lreq.Start), func(key string) error {
		if len(lrsp.Keys) == limit {
			lrsp.Next = []byte(key)
			return blob.ErrStopListing
		}
		lrsp.Keys = append(lrsp.Keys, []byte(key))
		return nil
	}); err != nil {
		return nil, err
	}
	return lrsp.Encode(), nil
}

// Len handles the corresponding method of [blob.KV].
func (s *Service) Len(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var lreq LenRequest
	if err := lreq.Decode(req.Data); err != nil {
		return nil, err
	}
	kv := s.idToKV(lreq.ID)
	if kv == nil {
		return invalidKeyspaceID(lreq.ID)
	}
	size, err := kv.Len(ctx)
	if err != nil {
		return nil, err
	}
	return packInt64(size), nil
}

// CASPut implements the corresponding method of [blob.CAS].
// It reports an error if the underlying keyspace does not implement it.
func (s *Service) CASPut(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var preq CASPutRequest
	if err := preq.Decode(req.Data); err != nil {
		return nil, err
	}
	kv := s.idToKV(preq.ID)
	if kv == nil {
		return invalidKeyspaceID(preq.ID)
	}
	cas := blob.CASFromKV(kv)
	key, err := cas.CASPut(ctx, preq.Data)
	return []byte(key), err
}

// CASKey implements the corresponding method of [blob.CAS].
// It reports an error if the underlying keyspace does not implement it.
func (s *Service) CASKey(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var preq CASPutRequest
	if err := preq.Decode(req.Data); err != nil {
		return nil, err
	}
	kv := s.idToKV(preq.ID)
	if kv == nil {
		return invalidKeyspaceID(preq.ID)
	}
	cas := blob.CASFromKV(kv)
	return []byte(cas.CASKey(ctx, preq.Data)), nil
}

func (s *Service) idToKV(id int) blob.KV {
	s.μ.Lock()
	defer s.μ.Unlock()
	return s.kvs[id]
}

func invalidKeyspaceID(id int) ([]byte, error) {
	return nil, fmt.Errorf("invalid keyspace ID %d", id)
}
