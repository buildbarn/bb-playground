package encoding

import (
	"crypto/aes"

	"github.com/buildbarn/bb-playground/pkg/proto/configuration/encoding"
	"github.com/buildbarn/bb-storage/pkg/util"
)

func NewBinaryEncoderFromConfiguration(configurations []*encoding.BinaryEncoderConfiguration, maximumDecodedSizeBytes uint32) (BinaryEncoder, error) {
	encoders := make([]BinaryEncoder, 0, len(configurations))
	for _, configuration := range configurations {
		switch encoderConfiguration := configuration.Encoder.(type) {
		case *encoding.BinaryEncoderConfiguration_LzwCompressing:
			encoders = append(
				encoders,
				NewLZWCompressingBinaryEncoder(maximumDecodedSizeBytes),
			)
		case *encoding.BinaryEncoderConfiguration_DeterministicEncrypting:
			encryptionKey, err := aes.NewCipher(encoderConfiguration.DeterministicEncrypting.EncryptionKey)
			if err != nil {
				return nil, util.StatusWrap(err, "Invalid encryption key")
			}
			encoders = append(
				encoders,
				NewDeterministicEncryptingBinaryEncoder(encryptionKey),
			)
		}
	}
	return NewChainedBinaryEncoder(encoders), nil
}
