package sharded

import (
	"math/bits"
)

const log2LUTBits = 6

// log2LUT is a lookup table for the significand of the floating point
// representation of log2(x) between [1, 2] at fixed intervals. It can
// be recomputed as follows:
//
//	for i := range log2LUT {
//		log2LUT[i] = uint16(math.Round(math.Log2(1+float64(i)/(1<<log2LUTBits)) * (1 << 16)))
//	}
var log2LUT = [1<<log2LUTBits + 1]uint16{
	0x0000, 0x05ba, 0x0b5d, 0x10eb, 0x1664, 0x1bc8, 0x2119, 0x2656,
	0x2b80, 0x3098, 0x359f, 0x3a94, 0x3f78, 0x444c, 0x4910, 0x4dc5,
	0x526a, 0x5700, 0x5b89, 0x6003, 0x646f, 0x68ce, 0x6d20, 0x7165,
	0x759d, 0x79ca, 0x7dea, 0x81ff, 0x8608, 0x8a06, 0x8dfa, 0x91e2,
	0x95c0, 0x9994, 0x9d5e, 0xa11e, 0xa4d4, 0xa881, 0xac24, 0xafbe,
	0xb350, 0xb6d9, 0xba59, 0xbdd1, 0xc140, 0xc4a8, 0xc807, 0xcb5f,
	0xceaf, 0xd1f7, 0xd538, 0xd872, 0xdba5, 0xded0, 0xe1f5, 0xe513,
	0xe82a, 0xeb3b, 0xee45, 0xf149, 0xf446, 0xf73e, 0xfa2f, 0xfd1a,
	// The final entry is supposed to be equal to 0x10000, but this
	// does not fit in a uint16. This is not a problem, because it
	// is only used to compute the delta, which is also computed
	// using 16-bit arithmetic.
	0x0000,
}

// Log2Fixed64 computes log2(x) using fixed-point arithmetic. As the
// input is a 64-bit unsigned integer, this function yields a value
// between [0, 64), shifted by 64-log2(64)=58 bits to the left.
func Log2Fixed64(in uint64) uint64 {
	// Convert the input to a floating point number.
	exponent := bits.Len64(in >> 1)
	significand := in << (64 - exponent)

	// Use the top bits of the significand to get the approximate
	// range of log2(x) from our lookup table.
	lutIndex := significand >> (64 - log2LUTBits)
	begin := log2LUT[lutIndex]
	end := log2LUT[lutIndex+1]
	delta := uint64(end - begin)

	// Perform linear approximation of log2(x).
	remainingSignificand := significand << log2LUTBits
	linearApproximation := uint64(begin)<<(64-16) + (remainingSignificand>>16)*delta

	const lengthLength = 6 // log2(64).
	return (uint64(exponent) << (64 - lengthLength)) | (linearApproximation >> lengthLength)
}
