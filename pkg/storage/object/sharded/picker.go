package sharded

// Picker of shards. Based on opaque data, Picker computes a hash and
// uses it to select a backend with a uniform distribution.
type Picker struct {
	count uint32
}

func NewPicker(count int) Picker {
	return Picker{
		count: uint32(count),
	}
}

func (p Picker) PickShard(data []byte) int {
	// Compute a 32-bit FNV-1a hash.
	hash := uint32(0x811c9dc5)
	for _, b := range data {
		hash ^= uint32(b)
		hash *= 0x01000193
	}

	// The bottom bits of FNV-1a hashes don't diffuse well. For
	// example, the lowest bit of the hash is always equal to the
	// XOR of the lowest bit of every input byte. The top bits
	// diffuse very well, but they may remain unaffected by bits in
	// the final set of bytes.
	//
	// By multiplying by 0x55555555, we force bits from the final
	// bytes of input to propagate to the top bits. We then use
	// multiplication and bit shifting to obtain an index that is
	// based on the top bits.
	return int(uint64(hash*0x55555555) * uint64(p.count) >> 32)
}
