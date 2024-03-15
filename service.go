package chirpstore

import (
	"context"
	"errors"

	"github.com/creachadair/chirp"
	"github.com/creachadair/ffs/blob"
)

// Constants defining the method names for the store service.
const (
	mStatus   = "status"
	mGet      = "get"
	mPut      = "put"
	mDelete   = "delete"
	mList     = "list"
	mLen      = "len"
	mCASPut   = "cas-put"
	mCASKey   = "cas-key"
	mSyncKeys = "sync-keys"
)

type Service struct {
	pfx string
	st  blob.SyncKeyer
	cas blob.CAS // populated iff st implements blob.CAS
}

// NewService constructs a service that delegates to the given blob.Store.
func NewService(st blob.Store, opts *ServiceOpts) *Service {
	s := &Service{pfx: opts.prefix()}
	if sk, ok := st.(blob.SyncKeyer); ok {
		s.st = sk
	} else {
		s.st = blob.ListSyncKeyer{Store: st}
	}
	if cas, ok := st.(blob.CAS); ok {
		s.cas = cas
	}
	return s
}

// ServiceOpts provides optional settings for constructing a Service.
type ServiceOpts struct {
	// A prefix to prepend to all the method names exported by the service.
	Prefix string
}

func (o *ServiceOpts) prefix() string {
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
	p.Handle(s.method(mPut), s.Put)
	p.Handle(s.method(mDelete), s.Delete)
	p.Handle(s.method(mList), s.List)
	p.Handle(s.method(mLen), s.Len)
	p.Handle(s.method(mSyncKeys), s.SyncKeys)
	if s.cas != nil {
		p.Handle(s.method(mCASPut), s.CASPut)
		p.Handle(s.method(mCASKey), s.CASKey)
	}
}

// Status returns a JSON blob of server metrics.
func (s *Service) Status(ctx context.Context, req *chirp.Request) ([]byte, error) {
	mx := chirp.ContextPeer(ctx).Metrics()
	return []byte(mx.String()), nil
}

// Get handles the corresponding method of blob.Store.
func (s *Service) Get(ctx context.Context, req *chirp.Request) ([]byte, error) {
	data, err := s.st.Get(ctx, string(req.Data))
	return data, filterErr(err)
}

// Put handles the corresponding method of blob.Store.
func (s *Service) Put(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var preq PutRequest
	if err := preq.Decode(req.Data); err != nil {
		return nil, err
	}
	return nil, filterErr(s.st.Put(ctx, blob.PutOptions{
		Key:     string(preq.Key),
		Data:    preq.Data,
		Replace: preq.Replace,
	}))
}

// Delete handles the corresponding method of blob.Store.
func (s *Service) Delete(ctx context.Context, req *chirp.Request) ([]byte, error) {
	return nil, filterErr(s.st.Delete(ctx, string(req.Data)))
}

// List handles the corresponding method of blob.Store.
func (s *Service) List(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var lreq ListRequest
	if err := lreq.Decode(req.Data); err != nil {
		return nil, err
	}

	limit := lreq.Count
	if limit <= 0 {
		limit = 256
	}

	var lrsp ListResponse
	if err := s.st.List(ctx, string(lreq.Start), func(key string) error {
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

// Len handles the corresponding method of blob.Store.
func (s *Service) Len(ctx context.Context, req *chirp.Request) ([]byte, error) {
	size, err := s.st.Len(ctx)
	if err != nil {
		return nil, err
	}
	return packInt64(size), nil
}

// CASPut implements content-addressable storage if the service has a CAS.
// It reports an error if the underlying store is not a blob.CAS.
func (s *Service) CASPut(ctx context.Context, req *chirp.Request) ([]byte, error) {
	if s.cas == nil {
		return nil, errors.New("store does not implement content addressing")
	}
	var preq CASPutRequest
	if err := preq.Decode(req.Data); err != nil {
		return nil, err
	}
	key, err := s.cas.CASPut(ctx, blob.CASPutOptions{
		Data:   preq.Data,
		Prefix: string(preq.Prefix),
		Suffix: string(preq.Suffix),
	})
	return []byte(key), err
}

// CASKey computes the hash key for the specified data, if the service has a CAS.
// It reports an error if the underlying store is not a blob.CAS.
func (s *Service) CASKey(ctx context.Context, req *chirp.Request) ([]byte, error) {
	if s.cas == nil {
		return nil, errors.New("store does not implement content addressing")
	}
	var preq CASPutRequest
	if err := preq.Decode(req.Data); err != nil {
		return nil, err
	}
	key, err := s.cas.CASKey(ctx, blob.CASPutOptions{
		Data:   preq.Data,
		Prefix: string(preq.Prefix),
		Suffix: string(preq.Suffix),
	})
	return []byte(key), err
}

// SyncKeys reports which of the specified keys are not present in the store.
func (s *Service) SyncKeys(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var sreq SyncRequest
	if err := sreq.Decode(req.Data); err != nil {
		return nil, err
	}
	missing, err := s.st.SyncKeys(ctx, sreq.getKeys())
	if err != nil {
		return nil, err
	}
	sreq.setKeys(missing)
	return sreq.Encode(), nil
}
