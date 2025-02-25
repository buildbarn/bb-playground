package simplelzw_test

import (
	"crypto/rand"
	"testing"

	"github.com/buildbarn/bonanza/pkg/compress/simplelzw"
	"github.com/stretchr/testify/require"
)

func TestCompress(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		compressed, err := simplelzw.Compress(nil)
		require.NoError(t, err)
		require.Empty(t, compressed)
	})

	t.Run("SingleByte", func(t *testing.T) {
		for i := 0; i < 256; i++ {
			compressed, err := simplelzw.Compress([]byte{byte(i)})
			require.NoError(t, err)
			require.Equal(
				t,
				[]byte{0x81, byte(i)},
				compressed,
			)
		}
	})

	t.Run("ABCDEF", func(t *testing.T) {
		compressed, err := simplelzw.Compress([]byte("ABCDEF"))
		require.NoError(t, err)
		require.Equal(
			t,
			[]byte{
				0x86,
				'A',
				'B',
				('C' << 1) & 0xff,
				('C'>>7 | 'D'<<2) & 0xff,
				('D'>>6 | 'E'<<3) & 0xff,
				('E'>>5 | 'F'<<4) & 0xff,
				'F' >> 4,
			},
			compressed,
		)
	})

	t.Run("RandomCompressDecompress", func(t *testing.T) {
		original := make([]byte, 10000)
		for length := 0; length < len(original); length++ {
			n, err := rand.Read(original[:length])
			require.NoError(t, err)
			require.Equal(t, length, n)

			compressed, err := simplelzw.Compress(original[:length])
			require.NoError(t, err)

			decompressed, err := simplelzw.Decompress(compressed, uint32(length))
			require.NoError(t, err)
			require.Equal(t, original[:length], decompressed)
		}
	})
}
