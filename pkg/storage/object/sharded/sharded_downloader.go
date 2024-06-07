package sharded

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/storage/object"
)

type shardedDownloader[TReference ShardedReference] struct {
	shards []object.Downloader[TReference]
	picker Picker
}

// NewShardedDownloader creates a decorator for one or more
// object.Downloaders that spreads out incoming requests based on the
// provided reference.
func NewShardedDownloader[TReference ShardedReference](shards []object.Downloader[TReference]) object.Downloader[TReference] {
	return &shardedDownloader[TReference]{
		shards: shards,
		picker: NewPicker(len(shards)),
	}
}

func (d *shardedDownloader[TReference]) DownloadObject(ctx context.Context, reference TReference) (*object.Contents, error) {
	return d.shards[d.picker.PickShard(reference.GetRawReference())].DownloadObject(ctx, reference)
}
