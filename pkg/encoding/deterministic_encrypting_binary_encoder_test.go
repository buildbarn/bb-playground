package encoding_test

import (
	"crypto/aes"
	"crypto/rand"
	"testing"

	"github.com/buildbarn/bb-playground/pkg/encoding"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDeterministicEncryptingBinaryEncoder(t *testing.T) {
	blockCipher, err := aes.NewCipher([]byte{
		0x10, 0xc2, 0xfe, 0xfd, 0x4b, 0x11, 0x43, 0x9b,
		0x1e, 0x73, 0xda, 0x12, 0xef, 0x53, 0xd1, 0x31,
		0xf7, 0x7e, 0x6c, 0xb8, 0x2f, 0xdf, 0x79, 0xb0,
		0x90, 0x6b, 0x23, 0x3c, 0x4a, 0x32, 0xa0, 0x14,
	})
	require.NoError(t, err)
	binaryEncoder := encoding.NewDeterministicEncryptingBinaryEncoder(blockCipher)

	t.Run("EncodeBinary", func(t *testing.T) {
		t.Run("Empty", func(t *testing.T) {
			encodedData, err := binaryEncoder.EncodeBinary(nil)
			require.NoError(t, err)
			require.Empty(t, encodedData)
		})

		t.Run("HelloWorld", func(t *testing.T) {
			encodedData, err := binaryEncoder.EncodeBinary([]byte("Hello world"))
			require.NoError(t, err)
			require.Equal(t, []byte{
				// Initialization vector.
				0x10, 0x23, 0x7d, 0x7d, 0xe1, 0x80, 0x2e, 0xe0,
				0x7c, 0x0c, 0xbe, 0x21, 0x53, 0x9d, 0x7e, 0xde,
				// Encrypted payload.
				0x58, 0x46, 0x11, 0x11, 0x8e, 0xa0, 0x59, 0x8f,
				0x0e, 0x60, 0xda,
				// Padding.
				0xa1,
			}, encodedData)
		})
	})

	t.Run("DecodeBinary", func(t *testing.T) {
		t.Run("Empty", func(t *testing.T) {
			decodedData, err := binaryEncoder.DecodeBinary(nil)
			require.NoError(t, err)
			require.Empty(t, decodedData)
		})

		t.Run("HelloWorld", func(t *testing.T) {
			decodedData, err := binaryEncoder.DecodeBinary([]byte{
				// Initialization vector.
				0x10, 0x23, 0x7d, 0x7d, 0xe1, 0x80, 0x2e, 0xe0,
				0x7c, 0x0c, 0xbe, 0x21, 0x53, 0x9d, 0x7e, 0xde,
				// Encrypted payload.
				0x58, 0x46, 0x11, 0x11, 0x8e, 0xa0, 0x59, 0x8f,
				0x0e, 0x60, 0xda,
				// Padding.
				0xa1,
			})
			require.NoError(t, err)
			require.Equal(t, []byte("Hello world"), decodedData)
		})

		t.Run("TooShort", func(t *testing.T) {
			_, err := binaryEncoder.DecodeBinary([]byte("Hello"))
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Encoded data is 5 bytes in size, which is too small hold an encrypted initialization vector of 16 bytes and a payload"), err)
		})

		t.Run("BadPadding", func(t *testing.T) {
			_, err := binaryEncoder.DecodeBinary([]byte{
				// Initialization vector.
				0x3a, 0xd1, 0xc5, 0xdc, 0xd0, 0x85, 0xf3, 0xd1,
				0xac, 0x1e, 0xaf, 0xd1, 0xe3, 0x92, 0x0d, 0x92,
				// Padding.
				0x60, 0x94, 0x5b, 0x87, 0x83, 0x5d, 0xeb, 0xd2,
				0x11, 0x4a, 0x9e, 0xa3, 0x0c, 0xf3, 0x4d, 0xba,
			})
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Padding contains invalid byte with value 40"), err)
		})

		t.Run("TooMuchPadding", func(t *testing.T) {
			// Additional check to ensure that other
			// implementations encode data the same way.
			// Using different amounts of padding may
			// introduce information leakage.
			_, err := binaryEncoder.DecodeBinary([]byte{
				// Initialization vector.
				0x10, 0x23, 0x7d, 0x7d, 0xe1, 0x80, 0x2e, 0xe0,
				0x7c, 0x0c, 0xbe, 0x21, 0x53, 0x9d, 0x7e, 0xde,
				// Encrypted payload.
				0x58, 0x46, 0x11, 0x11, 0x8e, 0xa0, 0x59, 0x8f,
				0x0e, 0x60, 0xda,
				// Padding.
				0xa1, 0x53,
			})
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Encoded data is 29 bytes in size, while 28 bytes were expected for an initialization vector of 16 bytes and a payload of 11 bytes"), err)
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
