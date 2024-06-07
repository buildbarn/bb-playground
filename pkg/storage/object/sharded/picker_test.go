package sharded_test

import (
	"log"
	"testing"

	"github.com/buildbarn/bb-playground/pkg/storage/object/sharded"
	"github.com/stretchr/testify/assert"
)

func TestPicker(t *testing.T) {
	t.Run("SingleByteDistribution", func(t *testing.T) {
		// For single-byte inputs, we should see a uniform
		// distribution for small shard counts.
		for shardCount := 2; shardCount < 11; shardCount++ {
			picker := sharded.NewPicker(shardCount)
			pickedCounts := make([]int, shardCount)
			for value := 0; value < 256; value++ {
				pickedCounts[picker.PickShard([]byte{byte(value)})]++
			}

			log.Printf("--- %d", shardCount)
			for _, pickedCount := range pickedCounts {
				log.Print(pickedCount)
			}

			for _, pickedCount := range pickedCounts {
				assert.LessOrEqual(t, 256/shardCount, pickedCount)
			}
		}
	})

	t.Run("SingleByteDistributionWithMask", func(t *testing.T) {
		// There shouldn't be a single bit within the input that
		// causes a disproportionate imbalance in selection of
		// shards.
		for shardCount := 1; shardCount < 7; shardCount++ {
			for mask := 1; mask <= 256; mask <<= 1 {
				picker := sharded.NewPicker(shardCount)
				pickedCounts := make([]int, shardCount)
				for value := 0; value < 256; value++ {
					pickedCounts[picker.PickShard([]byte{byte(value &^ mask)})]++
				}

				log.Printf("--- %d", shardCount)
				for _, pickedCount := range pickedCounts {
					log.Print(pickedCount)
				}

				for _, pickedCount := range pickedCounts {
					assert.LessOrEqual(t, 80/shardCount, pickedCount)
				}
			}
		}
	})
}
