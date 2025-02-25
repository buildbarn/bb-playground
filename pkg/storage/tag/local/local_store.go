package local

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/storage/object"
	object_local "github.com/buildbarn/bonanza/pkg/storage/object/local"
	"github.com/buildbarn/bonanza/pkg/storage/tag"

	"google.golang.org/protobuf/types/known/anypb"
)

type localStore struct{}

func NewLocalStore() tag.Store[object.Namespace, object.GlobalReference, object_local.LocalLease] {
	return &localStore{}
}

func (s *localStore) ResolveTag(ctx context.Context, namespace object.Namespace, tag *anypb.Any) (reference object.LocalReference, complete bool, err error) {
	panic("TODO")
}

func (s *localStore) UpdateTag(ctx context.Context, tag *anypb.Any, reference object.GlobalReference, lease object_local.LocalLease, overwrite bool) error {
	panic("TODO")
}
