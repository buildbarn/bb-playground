package simplelzw_test

import (
	"testing"

	"github.com/buildbarn/bb-playground/pkg/compress/simplelzw"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDecompress(t *testing.T) {
	t.Run("NotCompressed", func(t *testing.T) {
		// If the header indicates a size of zero bytes, the
		// decompressed data is stored in literal form.
		decompressed, err := simplelzw.Decompress(
			[]byte{
				0x80,
				'H',
				'e',
				'l',
				'l',
				'o',
			},
			/* maximumSizeBytes = */ 5,
		)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), decompressed)
	})

	t.Run("NotCompressedTooLarge", func(t *testing.T) {
		_, err := simplelzw.Decompress(
			[]byte{
				0x80,
				'H',
				'e',
				'l',
				'l',
				'o',
			},
			/* maximumSizeBytes = */ 2,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Decompressed data is 5 bytes in size, which exceeds the permitted maximum of 2 bytes"), err)
	})

	t.Run("TrailingBytes", func(t *testing.T) {
		// Compression output should be padded to the nearest
		// byte. It's not permitted to append unused bytes at
		// the end that are too small to fit any more codes.
		_, err := simplelzw.Decompress(
			[]byte{
				0x81,
				'A',
				0x00,
			},
			/* maximumSizeBytes = */ 100,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Compressed input contains 1 unnecessary trailing bytes"), err)
	})

	t.Run("AtMaximum", func(t *testing.T) {
		// Attempt to decompress 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 = 36
		// 'A' characters.
		decompressed, err := simplelzw.Decompress(
			[]byte{
				0x80 + 36,
				'A',
				256 & 0xff,
				(256>>8 | 257<<1) & 0xff,
				(257>>7 | 258<<2) & 0xff,
				(258>>6 | 259<<3) & 0xff,
				(259>>5 | 260<<4) & 0xff,
				(260>>4 | 261<<5) & 0xff,
				(261>>3 | 262<<6) & 0xff,
				262 >> 2,
			},
			/* maximumSizeBytes = */ 36,
		)
		require.NoError(t, err)
		require.Equal(t, []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"), decompressed)
	})

	t.Run("PastMaximum", func(t *testing.T) {
		// Attempt to decompress more data than is permitted by
		// the caller. This should cause a failure.
		_, err := simplelzw.Decompress(
			[]byte{
				0x80 + 36,
				'A',
				256 & 0xff,
				(256>>8 | 257<<1) & 0xff,
				(257>>7 | 258<<2) & 0xff,
				(258>>6 | 259<<3) & 0xff,
				(259>>5 | 260<<4) & 0xff,
				(260>>4 | 261<<5) & 0xff,
				(261>>3 | 262<<6) & 0xff,
				262 >> 2,
			},
			/* maximumSizeBytes = */ 35,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Decompressed data is 36 bytes in size, which exceeds the permitted maximum of 35 bytes"), err)
	})

	t.Run("MismatchingSizeTooLong", func(t *testing.T) {
		_, err := simplelzw.Decompress(
			[]byte{
				0x80 + 35,
				'A',
				256 & 0xff,
				(256>>8 | 257<<1) & 0xff,
				(257>>7 | 258<<2) & 0xff,
				(258>>6 | 259<<3) & 0xff,
				(259>>5 | 260<<4) & 0xff,
				(260>>4 | 261<<5) & 0xff,
				(261>>3 | 262<<6) & 0xff,
				262 >> 2,
			},
			/* maximumSizeBytes = */ 35,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Size of decompressed output exceeds size in header of 35 bytes, with 7 of 8 codes processed"), err)
	})

	t.Run("MismatchingSizeTooShort", func(t *testing.T) {
		_, err := simplelzw.Decompress(
			[]byte{
				0x80 + 37,
				'A',
				256 & 0xff,
				(256>>8 | 257<<1) & 0xff,
				(257>>7 | 258<<2) & 0xff,
				(258>>6 | 259<<3) & 0xff,
				(259>>5 | 260<<4) & 0xff,
				(260>>4 | 261<<5) & 0xff,
				(261>>3 | 262<<6) & 0xff,
				262 >> 2,
			},
			/* maximumSizeBytes = */ 37,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Size of decompressed output is 36 bytes, which differs from size in header of 37 bytes"), err)
	})

	t.Run("TrailingNonZeroBits", func(t *testing.T) {
		// There may be bits in the final byte that are unused.
		// Those should be set to zero.
		_, err := simplelzw.Decompress(
			[]byte{
				0x82,
				'A',
				'B',
				0xf0,
			},
			/* maximumSizeBytes = */ 100,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Trailing bits are not zero"), err)
	})
}
