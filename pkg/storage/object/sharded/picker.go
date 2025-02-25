package sharded

// Picker of shards. Based on opaque data, Picker computes a hash and
// uses it to select a backend with a uniform distribution.
type Picker interface {
	PickShard(data []byte) int
}
