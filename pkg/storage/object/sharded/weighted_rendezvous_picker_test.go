package sharded_test

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/buildbarn/bonanza/pkg/storage/object/sharded"
	"github.com/stretchr/testify/assert"
)

func TestWeightedRendezvousPicker(t *testing.T) {
	t.Run("Uniform", func(t *testing.T) {
		// When using 10 shards, each shard should receive
		// approximately 1/10th of all traffic.
		const shardCount = 10
		var shards [shardCount]sharded.WeightedShard
		for i := range shards {
			shards[i] = sharded.WeightedShard{
				Key:    []byte(fmt.Sprintf("Shard %d", i)),
				Weight: 1,
			}
		}
		picker := sharded.NewWeightedRendezvousPicker(shards[:])

		var hits [shardCount]int
		for i := uint32(0); i < shardCount*1000; i++ {
			var objectIdentifier [4]byte
			binary.LittleEndian.PutUint32(objectIdentifier[:], uint32(i))
			hits[picker.PickShard(objectIdentifier[:])]++
		}
		for _, shardHits := range hits {
			assert.Less(t, 900, shardHits)
			assert.Greater(t, 1100, shardHits)
		}
	})

	t.Run("Stable", func(t *testing.T) {
		// Adding more shards should only cause parts of the key
		// space to be assigned to the new shards. It should not
		// cause any objects to move between existing shards.
		const shardCount = 10
		var shards [shardCount]sharded.WeightedShard
		for i := range shards {
			shards[i] = sharded.WeightedShard{
				Key:    []byte(fmt.Sprintf("Shard %d", i)),
				Weight: 1,
			}
		}
		var pickers [shardCount]sharded.Picker
		for i := range pickers {
			pickers[i] = sharded.NewWeightedRendezvousPicker(shards[:i+1])
		}

		for i := uint32(0); i < 1000; i++ {
			var objectIdentifier [4]byte
			binary.LittleEndian.PutUint32(objectIdentifier[:], uint32(i))

			lastIndex := -1
			for pickerIndex, picker := range pickers {
				index := picker.PickShard(objectIdentifier[:])
				assert.True(t, index == lastIndex || index == pickerIndex)
				lastIndex = index
			}
		}
	})

	t.Run("Weights", func(t *testing.T) {
		// If shards have different weights, this should also
		// lead to a proportional difference in traffic.
		const shardCount = 5
		var shards [shardCount]sharded.WeightedShard
		for i := range shards {
			shards[i] = sharded.WeightedShard{
				Key:    []byte(fmt.Sprintf("Shard %d", i)),
				Weight: uint32(i + 1),
			}
		}
		picker := sharded.NewWeightedRendezvousPicker(shards[:])

		var hits [shardCount]int
		for i := uint32(0); i < shardCount*(shardCount+1)/2*1000; i++ {
			var objectIdentifier [4]byte
			binary.LittleEndian.PutUint32(objectIdentifier[:], uint32(i))
			hits[picker.PickShard(objectIdentifier[:])]++
		}
		for i, shardHits := range hits {
			assert.Less(t, 900*(i+1), shardHits)
			assert.Greater(t, 1100*(i+1), shardHits)
		}
	})
}
