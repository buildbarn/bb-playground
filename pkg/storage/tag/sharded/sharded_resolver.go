package sharded

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/object/sharded"
	"github.com/buildbarn/bb-playground/pkg/storage/tag"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type shardedResolver[TNamespace any] struct {
	shards []tag.Resolver[TNamespace]
	picker sharded.Picker
}

// NewShardedResolver creates a decorator for one or more tag.Resolvers
// that spreads out incoming requests based on the provided reference.
func NewShardedResolver[TNamespace any](shards []tag.Resolver[TNamespace]) tag.Resolver[TNamespace] {
	return &shardedResolver[TNamespace]{
		shards: shards,
		picker: sharded.NewPicker(len(shards)),
	}
}

func (r *shardedResolver[TNamespace]) ResolveTag(ctx context.Context, namespace TNamespace, tag *anypb.Any) (object.LocalReference, bool, error) {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(tag)
	if err != nil {
		var badReference object.LocalReference
		return badReference, false, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal tag")
	}
	return r.shards[r.picker.PickShard(data)].ResolveTag(ctx, namespace, tag)
}
