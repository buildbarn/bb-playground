package sharded

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/storage/object"
	"github.com/buildbarn/bonanza/pkg/storage/object/sharded"
	"github.com/buildbarn/bonanza/pkg/storage/tag"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type shardedResolver[TNamespace any] struct {
	shards     []tag.Resolver[TNamespace]
	shardNames []string
	picker     sharded.Picker
}

// NewShardedResolver creates a decorator for one or more tag.Resolvers
// that spreads out incoming requests based on the provided reference.
func NewShardedResolver[TNamespace any](shards []tag.Resolver[TNamespace], shardNames []string, picker sharded.Picker) tag.Resolver[TNamespace] {
	return &shardedResolver[TNamespace]{
		shards:     shards,
		shardNames: shardNames,
		picker:     picker,
	}
}

func (r *shardedResolver[TNamespace]) ResolveTag(ctx context.Context, namespace TNamespace, tag *anypb.Any) (object.LocalReference, bool, error) {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(tag)
	if err != nil {
		var badReference object.LocalReference
		return badReference, false, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal tag")
	}
	shardIndex := r.picker.PickShard(data)
	reference, complete, err := r.shards[shardIndex].ResolveTag(ctx, namespace, tag)
	if err != nil {
		var badReference object.LocalReference
		return badReference, false, util.StatusWrapf(err, "Shard %#v", r.shardNames[shardIndex])
	}
	return reference, complete, err
}
