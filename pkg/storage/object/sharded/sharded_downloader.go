package sharded

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

type shardedDownloader[TReference ShardedReference] struct {
	shards     []object.Downloader[TReference]
	shardNames []string
	picker     Picker
}

// NewShardedDownloader creates a decorator for one or more
// object.Downloaders that spreads out incoming requests based on the
// provided reference.
func NewShardedDownloader[TReference ShardedReference](shards []object.Downloader[TReference], shardNames []string, picker Picker) object.Downloader[TReference] {
	return &shardedDownloader[TReference]{
		shards:     shards,
		shardNames: shardNames,
		picker:     picker,
	}
}

func (d *shardedDownloader[TReference]) DownloadObject(ctx context.Context, reference TReference) (*object.Contents, error) {
	shardIndex := d.picker.PickShard(reference.GetRawReference())
	contents, err := d.shards[shardIndex].DownloadObject(ctx, reference)
	if err != nil {
		return nil, util.StatusWrapf(err, "Shard %#v", d.shardNames[shardIndex])
	}
	return contents, nil
}
