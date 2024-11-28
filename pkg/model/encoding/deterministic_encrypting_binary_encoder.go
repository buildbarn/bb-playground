package encoding

import (
	"crypto/cipher"
	"crypto/sha256"
	"math/bits"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type deterministicEncryptingBinaryEncoder struct {
	blockCipher    cipher.Block
	blockSizeBytes int
}

// NewDeterministicEncryptingBinaryEncoder creates a BinaryEncoder that
// is capable of encrypting and decrypting data. The encryption process
// is deterministic, in that encrypting the same data twice results in
// the same encoded version of the data. It does not use Authenticating
// Encryption (AE), meaning that other facilities need to be present to
// ensure integrity of data.
func NewDeterministicEncryptingBinaryEncoder(blockCipher cipher.Block) BinaryEncoder {
	return &deterministicEncryptingBinaryEncoder{
		blockCipher:    blockCipher,
		blockSizeBytes: blockCipher.BlockSize(),
	}
}

// getEncodedSizeBytes computes the size of the encrypted output, with
// padding in place. Because we use Counter (CTR) mode, we don't need
// any padding to encrypt the data itself. However, adding it reduces
// information leakage by obfuscating the original size.
//
// Use the same structure as Padded Uniform Random Blobs (PURBs), where
// the length is rounded up to a floating point number whose mantissa is
// no longer than its exponent.
//
// More details: Reducing Metadata Leakage from Encrypted Files and
// Communication with PURBs, Algorithm 1 "PADMÉ".
// https://petsymposium.org/popets/2019/popets-2019-0056.pdf
func (be *deterministicEncryptingBinaryEncoder) getEncodedSizeBytes(dataSizeBytes int) int {
	unpaddedSizeBytes := be.blockSizeBytes + dataSizeBytes
	e := bits.Len(uint(unpaddedSizeBytes)) - 1
	bitsToClear := e - bits.Len(uint(e))
	return (unpaddedSizeBytes>>bitsToClear + 1) << bitsToClear
}

func (be *deterministicEncryptingBinaryEncoder) EncodeBinary(in []byte) ([]byte, error) {
	if len(in) == 0 {
		return []byte{}, nil
	}

	// Pick an initialization vector. Because this has to work
	// deterministically, hash the input. Encrypt it, so that the
	// hash itself isn't revealed. That would allow fingerprinting
	// of objects, even if the key is changed.
	ivHash := sha256.Sum256(in)
	out := make([]byte, be.getEncodedSizeBytes(len(in)))
	iv := out[:be.blockSizeBytes]
	be.blockCipher.Encrypt(iv, ivHash[:be.blockSizeBytes])

	outPayload := out[be.blockSizeBytes:]
	stream := cipher.NewCTR(be.blockCipher, iv)
	stream.XORKeyStream(outPayload, in)

	outPadding := outPayload[len(in):]
	outPadding[0] = 0x80
	stream.XORKeyStream(outPadding, outPadding)
	return out, nil
}

func (be *deterministicEncryptingBinaryEncoder) DecodeBinary(in []byte) ([]byte, error) {
	if len(in) == 0 {
		return []byte{}, nil
	}
	if len(in) <= be.blockSizeBytes {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"Encoded data is %d bytes in size, which is too small hold an encrypted initialization vector of %d bytes and a payload",
			len(in),
			be.blockSizeBytes,
		)
	}

	// Decrypt the data, using the initialization vector that is
	// stored before it.
	encryptedData := in[be.blockSizeBytes:]
	out := make([]byte, len(encryptedData))
	stream := cipher.NewCTR(be.blockCipher, in[:be.blockSizeBytes])
	stream.XORKeyStream(out, encryptedData)

	// Remove trailing padding.
	for l := len(out) - 1; l > 0; l-- {
		switch out[l] {
		case 0x00:
		case 0x80:
			out = out[:l]
			if encodedSizeBytes := be.getEncodedSizeBytes(len(out)); len(in) != encodedSizeBytes {
				return nil, status.Errorf(
					codes.InvalidArgument,
					"Encoded data is %d bytes in size, while %d bytes were expected for an initialization vector of %d bytes and a payload of %d bytes",
					len(in),
					encodedSizeBytes,
					be.blockSizeBytes,
					len(out),
				)
			}
			return out, nil
		default:
			return nil, status.Errorf(codes.InvalidArgument, "Padding contains invalid byte with value %d", int(out[l]))
		}
	}
	return nil, status.Error(codes.InvalidArgument, "No data remains after removing padding")
}
