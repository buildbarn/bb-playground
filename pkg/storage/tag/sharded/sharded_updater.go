package sharded

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/storage/object/sharded"
	"github.com/buildbarn/bonanza/pkg/storage/tag"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type shardedUpdater[TReference any, TLease any] struct {
	shards     []tag.Updater[TReference, TLease]
	shardNames []string
	picker     sharded.Picker
}

// NewShardedUpdater creates a decorator for one or more tag.Updaters
// that spreads out incoming requests based on the provided reference.
func NewShardedUpdater[TReference, TLease any](shards []tag.Updater[TReference, TLease], shardNames []string, picker sharded.Picker) tag.Updater[TReference, TLease] {
	return &shardedUpdater[TReference, TLease]{
		shards:     shards,
		shardNames: shardNames,
		picker:     picker,
	}
}

func (u *shardedUpdater[TReference, TLease]) UpdateTag(ctx context.Context, tag *anypb.Any, reference TReference, lease TLease, overwrite bool) error {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(tag)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal tag")
	}
	shardIndex := u.picker.PickShard(data)
	if err := u.shards[shardIndex].UpdateTag(ctx, tag, reference, lease, overwrite); err != nil {
		return util.StatusWrapf(err, "Shard %#v", u.shardNames[shardIndex])
	}
	return nil
}
