package sharded

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"hash/fnv"
	"math"
	"slices"
)

// WeightedShard describes the properties of a single shard that may be
// selected by WeightedRendezvousPicker.
type WeightedShard struct {
	Key    []byte
	Weight uint32
}

type preprocessedShard struct {
	hash   uint64
	weight uint64
	index  int
}

type weightedRendezvousPicker struct {
	shards []preprocessedShard
}

// NewWeightedRendezvousPicker creates a picker of shards that uses
// rendezvous hashing. This ensures that if backends are added or
// removed, the key space is only affected minimally.
func NewWeightedRendezvousPicker(weightedShards []WeightedShard) Picker {
	// Weights in the configuration file tend to be very small.
	// However, to get decent accuracy we want them to be as large
	// as possible. Determine the maximum weight, so they can be
	// scaled.
	maximumWeight := uint32(1)
	for _, weightedShard := range weightedShards {
		if maximumWeight < weightedShard.Weight {
			maximumWeight = weightedShard.Weight
		}
	}

	// Instead of hashing the keys of all shards during every
	// request, precompute it.
	preprocessedShards := make([]preprocessedShard, 0, len(weightedShards))
	weightScalingFactor := math.MaxUint64 / uint64(maximumWeight)
	for index, weightedShard := range weightedShards {
		hash := sha256.Sum256(weightedShard.Key)
		preprocessedShards = append(preprocessedShards, preprocessedShard{
			hash:   binary.LittleEndian.Uint64(hash[:]) | 1,
			weight: uint64(weightedShard.Weight) * weightScalingFactor,
			index:  index,
		})
	}

	// In the configuration file shards may be listed in random
	// order (e.g., using a map). In our case we want the order to
	// be stable, as in case of ties we still want the shard
	// selection process to be deterministic. Sort the entries by
	// key.
	slices.SortFunc(preprocessedShards, func(a, b preprocessedShard) int {
		return bytes.Compare(weightedShards[a.index].Key, weightedShards[b.index].Key)
	})

	return &weightedRendezvousPicker{
		shards: preprocessedShards,
	}
}

func (p *weightedRendezvousPicker) PickShard(data []byte) int {
	// Compute a hash of the object identifier.
	dataHasher := fnv.New64a()
	dataHasher.Write(data)
	dataHash := dataHasher.Sum64()

	bestIndex, bestScore := 0, uint64(0)
	for index, shard := range p.shards {
		// Mix the hash of the object identifier and the server
		// together. Simple multiplication should work well
		// enough, considering that we ensure the server hash
		// has its lowest bit set.
		combinedHash := dataHash * shard.hash

		// Computed score = weight / -log(hash).
		minusLogHash := 1<<32 - Log2Fixed64(combinedHash)>>32
		if score := shard.weight / minusLogHash; bestScore < score {
			bestIndex, bestScore = index, score
		}
	}
	return bestIndex
}
