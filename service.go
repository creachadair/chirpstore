package chirpstore

import (
	"context"
	"errors"

	"github.com/creachadair/chirp"
	"github.com/creachadair/ffs/blob"
)

// Constants defining the method IDs for the store service.
const (
	mGet    = 100
	mPut    = 101
	mDelete = 102
	mSize   = 103
	mList   = 104
	mLen    = 105
	mCASPut = 201
	mCASKey = 202
)

type Service struct {
	st  blob.Store
	cas blob.CAS // populated iff st implements blob.CAS
}

// NewService constructs a service that delegates to the given blob.Store.
func NewService(st blob.Store, opts *ServiceOpts) *Service {
	s := &Service{st: st}
	if cas, ok := st.(blob.CAS); ok {
		s.cas = cas
	}
	return s
}

// ServiceOpts provides optional settings for constructing a Service.
type ServiceOpts struct{}

// Register adds method handlers to p for each of the applicable methods of s.
func (s *Service) Register(p *chirp.Peer) {
	p.Handle(mGet, s.Get)
	p.Handle(mPut, s.Put)
	p.Handle(mDelete, s.Delete)
	p.Handle(mSize, s.Size)
	p.Handle(mList, s.List)
	p.Handle(mLen, s.Len)
	if s.cas != nil {
		p.Handle(mCASPut, s.CASPut)
		p.Handle(mCASKey, s.CASKey)
	}
}

// Get handles the corresponding method of blob.Store.
func (s *Service) Get(ctx context.Context, req *chirp.Request) ([]byte, error) {
	data, err := s.st.Get(ctx, string(req.Data))
	return data, filterErr(err)
}

// Put handles the corresponding method of blob.Store.
func (s *Service) Put(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var preq PutRequest
	if err := preq.UnmarshalBinary(req.Data); err != nil {
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

// Size handles the corresponding method of blob.Store.
func (s *Service) Size(ctx context.Context, req *chirp.Request) ([]byte, error) {
	size, err := s.st.Size(ctx, string(req.Data))
	if err != nil {
		return nil, filterErr(err)
	}
	return packInt64(size), nil
}

// List handles the corresponding method of blob.Store.
func (s *Service) List(ctx context.Context, req *chirp.Request) ([]byte, error) {
	var lreq ListRequest
	if err := lreq.UnmarshalBinary(req.Data); err != nil {
		return nil, err
	}

	limit := lreq.Count
	if limit <= 0 {
		limit = 64
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
	key, err := s.cas.CASPut(ctx, req.Data)
	return []byte(key), err
}

// CASKey computes teh hash key for the specified data, if the service has a CAS.
// It reports an error if the underlying store is not a blob.CAS.
func (s *Service) CASKey(ctx context.Context, req *chirp.Request) ([]byte, error) {
	if s.cas == nil {
		return nil, errors.New("store does not implement content addressing")
	}
	key, err := s.cas.CASKey(ctx, req.Data)
	return []byte(key), err
}
