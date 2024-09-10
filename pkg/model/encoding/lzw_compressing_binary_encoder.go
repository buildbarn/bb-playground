package encoding

import (
	"github.com/buildbarn/bb-playground/pkg/compress/simplelzw"
)

type lzwCompressingBinaryEncoder struct {
	maximumDecodedSizeBytes uint32
}

// NewLZWCompressingBinaryEncoder creates a BinaryEncoder that encodes
// data by compressing data using the "simple LZW" algorithm.
func NewLZWCompressingBinaryEncoder(maximumDecodedSizeBytes uint32) BinaryEncoder {
	return &lzwCompressingBinaryEncoder{
		maximumDecodedSizeBytes: maximumDecodedSizeBytes,
	}
}

func (be *lzwCompressingBinaryEncoder) EncodeBinary(in []byte) ([]byte, error) {
	return simplelzw.MaybeCompress(in)
}

func (be *lzwCompressingBinaryEncoder) DecodeBinary(in []byte) ([]byte, error) {
	return simplelzw.Decompress(in, be.maximumDecodedSizeBytes)
}
