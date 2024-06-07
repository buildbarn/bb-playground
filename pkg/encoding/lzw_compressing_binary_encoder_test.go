package encoding_test

import (
	"crypto/rand"
	"testing"

	"github.com/buildbarn/bb-playground/pkg/encoding"
	"github.com/stretchr/testify/require"
)

func TestLZWCompressingBinaryEncoder(t *testing.T) {
	binaryEncoder := encoding.NewLZWCompressingBinaryEncoder(1 << 20)

	t.Run("EncodeBinary", func(t *testing.T) {
		t.Run("Empty", func(t *testing.T) {
			encodedData, err := binaryEncoder.EncodeBinary(nil)
			require.NoError(t, err)
			require.Empty(t, encodedData)
		})
	})

	t.Run("DecodeBinary", func(t *testing.T) {
		t.Run("Empty", func(t *testing.T) {
			decodedData, err := binaryEncoder.DecodeBinary(nil)
			require.NoError(t, err)
			require.Empty(t, decodedData)
		})
	})

	t.Run("RandomEncodeDecode", func(t *testing.T) {
		original := make([]byte, 10000)
		for length := 0; length < len(original); length++ {
			n, err := rand.Read(original[:length])
			require.NoError(t, err)
			require.Equal(t, length, n)

			encoded, err := binaryEncoder.EncodeBinary(original[:length])
			require.NoError(t, err)

			decoded, err := binaryEncoder.DecodeBinary(encoded)
			require.NoError(t, err)
			require.Equal(t, original[:length], decoded)
		}
	})
}
