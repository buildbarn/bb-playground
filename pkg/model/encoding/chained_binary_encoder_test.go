package encoding_test

import (
	"testing"

	"github.com/buildbarn/bb-playground/pkg/model/encoding"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestChainedBinaryEncoder(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("Zero", func(t *testing.T) {
		// If no encoders are provided, the resulting chained
		// encoder should act as the identity function.
		binaryEncoder := encoding.NewChainedBinaryEncoder(nil)

		t.Run("Encode", func(t *testing.T) {
			encodedData, err := binaryEncoder.EncodeBinary([]byte("Hello"))
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), encodedData)
		})

		t.Run("Decode", func(t *testing.T) {
			decodedData, err := binaryEncoder.DecodeBinary([]byte("Hello"))
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), decodedData)
		})
	})

	t.Run("One", func(t *testing.T) {
		binaryEncoder1 := NewMockBinaryEncoder(ctrl)
		binaryEncoder := encoding.NewChainedBinaryEncoder([]encoding.BinaryEncoder{
			binaryEncoder1,
		})

		t.Run("Encode", func(t *testing.T) {
			binaryEncoder1.EXPECT().EncodeBinary([]byte("Hello")).
				Return([]byte("World"), nil)

			encodedData, err := binaryEncoder.EncodeBinary([]byte("Hello"))
			require.NoError(t, err)
			require.Equal(t, []byte("World"), encodedData)
		})

		t.Run("Decode", func(t *testing.T) {
			binaryEncoder1.EXPECT().DecodeBinary([]byte("World")).
				Return([]byte("Hello"), nil)

			decodedData, err := binaryEncoder.DecodeBinary([]byte("World"))
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), decodedData)
		})
	})

	t.Run("Two", func(t *testing.T) {
		binaryEncoder1 := NewMockBinaryEncoder(ctrl)
		binaryEncoder2 := NewMockBinaryEncoder(ctrl)
		binaryEncoder := encoding.NewChainedBinaryEncoder([]encoding.BinaryEncoder{
			binaryEncoder1,
			binaryEncoder2,
		})

		t.Run("Encode", func(t *testing.T) {
			// When encoding, the encoders should be applied
			// from first to last (e.g., compress and
			// encrypt).
			gomock.InOrder(
				binaryEncoder1.EXPECT().EncodeBinary([]byte("Foo")).
					Return([]byte("Bar"), nil),
				binaryEncoder2.EXPECT().EncodeBinary([]byte("Bar")).
					Return([]byte("Baz"), nil),
			)

			encodedData, err := binaryEncoder.EncodeBinary([]byte("Foo"))
			require.NoError(t, err)
			require.Equal(t, []byte("Baz"), encodedData)
		})

		t.Run("Decode", func(t *testing.T) {
			// When decoding, the encoders should be applied
			// the other way around (e.g., decrypt and
			// decompress).
			gomock.InOrder(
				binaryEncoder2.EXPECT().DecodeBinary([]byte("Baz")).
					Return([]byte("Bar"), nil),
				binaryEncoder1.EXPECT().DecodeBinary([]byte("Bar")).
					Return([]byte("Foo"), nil),
			)

			decodedData, err := binaryEncoder.DecodeBinary([]byte("Baz"))
			require.NoError(t, err)
			require.Equal(t, []byte("Foo"), decodedData)
		})
	})
}
