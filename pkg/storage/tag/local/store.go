package local

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/storage/object"
	object_local "github.com/buildbarn/bonanza/pkg/storage/object/local"
	"github.com/buildbarn/bonanza/pkg/storage/tag"

	"google.golang.org/protobuf/types/known/anypb"
)

type store struct{}

// NewStore creates a tag store that is backed by local disks.
func NewStore() tag.Store[object.Namespace, object.GlobalReference, object_local.Lease] {
	return &store{}
}

func (s *store) ResolveTag(ctx context.Context, namespace object.Namespace, tag *anypb.Any) (reference object.LocalReference, complete bool, err error) {
	panic("TODO")
}

func (s *store) UpdateTag(ctx context.Context, tag *anypb.Any, reference object.GlobalReference, lease object_local.Lease, overwrite bool) error {
	panic("TODO")
}
