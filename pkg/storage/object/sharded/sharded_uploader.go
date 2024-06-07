package sharded

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/storage/object"
)

type ShardedReference interface {
	GetRawReference() []byte
}

type shardedUploader[TReference ShardedReference, TLease any] struct {
	shards []object.Uploader[TReference, TLease]
	picker Picker
}

// NewShardedUploader creates a decorator for one or more
// object.Uploaders that spreads out incoming requests based on the
// provided reference.
func NewShardedUploader[TReference ShardedReference, TLease any](shards []object.Uploader[TReference, TLease]) object.Uploader[TReference, TLease] {
	return &shardedUploader[TReference, TLease]{
		shards: shards,
		picker: NewPicker(len(shards)),
	}
}

func (u *shardedUploader[TReference, TLease]) UploadObject(ctx context.Context, reference TReference, contents *object.Contents, childrenLeases []TLease, wantContentsIfIncomplete bool) (object.UploadObjectResult[TLease], error) {
	return u.shards[u.picker.PickShard(reference.GetRawReference())].UploadObject(ctx, reference, contents, childrenLeases, wantContentsIfIncomplete)
}
