package simplelzw

import (
	"encoding/binary"
	"math/bits"

	"github.com/buildbarn/bonanza/pkg/encoding/varint"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Compress data using a simplified version of the Lempel-Ziv-Welch
// (LZW) algorithm.
//
// This algorithm differs from the one used by GIF, PDF, TIFF, etc. in
// that it does not place an upper limit on the number of codes. There
// is also no support for issuing reset codes. As this makes the amount
// of state needed to compress and decompress proportional to the size
// of the resulting compressed data, this algorithm is not suitable for
// compressing streams of data.
//
// This algorithm provides a compression ratio that is worse than
// Zstandard. It is also slower. It is only provided because its output
// is deterministic and reproducible. Because it is easy to implement,
// it is easy to achieve reproducibility across implementations.
func Compress(uncompressed []byte) ([]byte, error) {
	if len(uncompressed) == 0 {
		return []byte{}, nil
	}

	// Compressed output is always prefixed with the size of the
	// uncompressed data. That allows the decompressor to allocate
	// the right amount of space.
	compressed := varint.AppendForward(nil, len(uncompressed))

	// Hash table that maps the code of the current prefix and next
	// symbol to the code of the next prefix. Each entry has the
	// following format:
	//
	// - Top 28 bits: Code of the current prefix.
	// - Middle 8 bits: Next symbol.
	// - Bottom 28 bits: Code of the next prefix.
	const compressMaximumCodeBits = (64 - 8) / 2
	table := make([]uint64, 0x1000)
	tableMask := uint64(0xfff)

	// The lowest 256 codes always map to individual literals. The
	// width of the currently highest code determines the width of
	// codes written to the output.
	highestCode := uint64(0xff)
	highestCodeBits := 8

	var pendingOutput uint64
	pendingBits := 0
	currentCode := uint64(uncompressed[0])
ProcessBytes:
	for _, nextSymbol := range uncompressed[1:] {
		// Check if the current prefix and next symbol are
		// already part of our dictionary. If so, use the code
		// that's assigned to it.
		lookupKey := currentCode<<8 | uint64(nextSymbol)
		lookupHash := lookupKey>>8 ^ lookupKey
		for h, i := lookupHash, uint64(1); ; h, i = h+i, i+1 {
			entry := table[h&tableMask]
			if entry == 0 {
				break
			}
			if entry>>compressMaximumCodeBits == lookupKey {
				currentCode = entry & (1<<compressMaximumCodeBits - 1)
				continue ProcessBytes
			}
		}

		// If not, write the code for the current prefix that is
		// part of our dictionary.
		pendingOutput |= currentCode << pendingBits
		pendingBits += highestCodeBits
		if pendingBits >= 32 {
			compressed = binary.LittleEndian.AppendUint32(compressed, uint32(pendingOutput))
			pendingOutput >>= 32
			pendingBits -= 32
		}

		// Allocate a new code for the current prefix with the
		// next symbol appended to it. If this crosses a power
		// of 2 boundary, use the opportunity to check that our
		// hash table is still big enough. If not, grow it.
		highestCode++
		if newCodeBits := bits.Len64(highestCode); newCodeBits != highestCodeBits {
			if newCodeBits > compressMaximumCodeBits {
				return nil, status.Errorf(codes.InvalidArgument, "Data requires more than %d codes to compress", 1<<compressMaximumCodeBits)
			}
			if desiredSize := int(highestCode) * 4; len(table) < desiredSize {
				newTable := make([]uint64, desiredSize)
				newTableMask := uint64(len(newTable) - 1)
				for _, entry := range table {
					key := entry >> compressMaximumCodeBits
					for h, i := key>>8^key, uint64(1); ; h, i = h+i, i+1 {
						newEntry := &newTable[h&newTableMask]
						if *newEntry == 0 {
							*newEntry = entry
							break
						}
					}
				}
				table = newTable
				tableMask = newTableMask
			}
			highestCodeBits = newCodeBits
		}

		// Insert the current prefix with the next symbol
		// appended to it into the hash table, so that future
		// occurrences can be abbreviated.
		for h, i := lookupHash, uint64(1); ; h, i = h+i, i+1 {
			entry := &table[h&tableMask]
			if *entry == 0 {
				*entry = lookupKey<<compressMaximumCodeBits | highestCode
				break
			}
		}
		currentCode = uint64(nextSymbol)
	}

	pendingOutput |= uint64(currentCode) << pendingBits
	pendingBits += highestCodeBits

	// Flush any trailing bytes of output.
	var trailer [8]byte
	binary.LittleEndian.PutUint64(trailer[:], pendingOutput)
	compressed = append(compressed, trailer[:(pendingBits+7)/8]...)
	return compressed, nil
}

// MaybeCompress compresses data, but discards the results if the
// compressed copy is larger than the original. In that case the
// uncompressed version is returned, prefixed with a decompressed size
// of 0 bytes, indicating that data is not compressed.
func MaybeCompress(uncompressed []byte) ([]byte, error) {
	compressed, err := Compress(uncompressed)
	if err != nil {
		return nil, err
	}
	if len(compressed) > len(uncompressed) {
		return append([]byte{0x80}, uncompressed...), nil
	}
	return compressed, nil
}
