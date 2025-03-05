package sharded

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

type shardedUploader[TReference object.BasicReference, TLease any] struct {
	shards     []object.Uploader[TReference, TLease]
	shardNames []string
	picker     Picker
}

// NewShardedUploader creates a decorator for one or more
// object.Uploaders that spreads out incoming requests based on the
// provided reference.
func NewShardedUploader[TReference object.BasicReference, TLease any](shards []object.Uploader[TReference, TLease], shardNames []string, picker Picker) object.Uploader[TReference, TLease] {
	return &shardedUploader[TReference, TLease]{
		shards:     shards,
		shardNames: shardNames,
		picker:     picker,
	}
}

func (u *shardedUploader[TReference, TLease]) UploadObject(ctx context.Context, reference TReference, contents *object.Contents, childrenLeases []TLease, wantContentsIfIncomplete bool) (object.UploadObjectResult[TLease], error) {
	shardIndex := u.picker.PickShard(reference.GetRawReference())
	result, err := u.shards[shardIndex].UploadObject(ctx, reference, contents, childrenLeases, wantContentsIfIncomplete)
	if err != nil {
		return nil, util.StatusWrapf(err, "Shard %#v", u.shardNames[shardIndex])
	}
	return result, nil
}
