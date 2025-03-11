package local

import (
	"context"
	"sync"

	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type storeObject struct {
	contents *object.Contents
	leases   []Lease
}

type store struct {
	lock    sync.Mutex
	objects map[object.LocalReference]*storeObject
}

// NewStore creates an object store that uses locally connected disks as
// its backing store.
func NewStore() object.Store[object.LocalReference, Lease] {
	return &store{
		objects: map[object.LocalReference]*storeObject{},
	}
}

func (s *store) DownloadObject(ctx context.Context, reference object.LocalReference) (*object.Contents, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	o, ok := s.objects[reference]
	if !ok {
		return nil, status.Error(codes.NotFound, "Object not found")
	}
	return o.contents, nil
}

func (s *store) UploadObject(ctx context.Context, reference object.LocalReference, contents *object.Contents, childrenLeases []Lease, wantContentsIfIncomplete bool) (object.UploadObjectResult[Lease], error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	o, ok := s.objects[reference]
	if !ok {
		if contents == nil {
			return object.UploadObjectMissing[Lease]{}, nil
		}
		o = &storeObject{
			contents: contents,
			leases:   make([]Lease, reference.GetDegree()),
		}
		s.objects[reference] = o
	}

	// TODO: Update leases, and report which ones are incomplete!
	return object.UploadObjectComplete[Lease]{}, nil
}
