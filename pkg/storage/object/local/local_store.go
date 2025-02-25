package local

import (
	"context"
	"sync"

	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type localStoreObject struct {
	contents *object.Contents
	leases   []LocalLease
}

type localStore struct {
	lock    sync.Mutex
	objects map[object.LocalReference]*localStoreObject
}

func NewLocalStore() object.Store[object.LocalReference, LocalLease] {
	return &localStore{
		objects: map[object.LocalReference]*localStoreObject{},
	}
}

func (s *localStore) DownloadObject(ctx context.Context, reference object.LocalReference) (*object.Contents, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	o, ok := s.objects[reference]
	if !ok {
		return nil, status.Error(codes.NotFound, "Object not found")
	}
	return o.contents, nil
}

func (s *localStore) UploadObject(ctx context.Context, reference object.LocalReference, contents *object.Contents, childrenLeases []LocalLease, wantContentsIfIncomplete bool) (object.UploadObjectResult[LocalLease], error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	o, ok := s.objects[reference]
	if !ok {
		if contents == nil {
			return object.UploadObjectMissing[LocalLease]{}, nil
		}
		o = &localStoreObject{
			contents: contents,
			leases:   make([]LocalLease, reference.GetDegree()),
		}
		s.objects[reference] = o
	}

	// TODO: Update leases, and report which ones are incomplete!
	return object.UploadObjectComplete[LocalLease]{}, nil
}
