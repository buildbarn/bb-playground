package encoding

type chainedBinaryEncoder struct {
	encoders []BinaryEncoder
}

// NewChainedBinaryEncoder creates a BinaryEncoder that is capable of
// applying multiple encoding/decoding steps. It can be used to, for
// example, apply both compression and encryption.
func NewChainedBinaryEncoder(encoders []BinaryEncoder) BinaryEncoder {
	if len(encoders) == 1 {
		return encoders[0]
	}
	return &chainedBinaryEncoder{
		encoders: encoders,
	}
}

func (be *chainedBinaryEncoder) EncodeBinary(in []byte) ([]byte, error) {
	// Invoke encoders in forward order.
	for _, encoder := range be.encoders {
		var err error
		in, err = encoder.EncodeBinary(in)
		if err != nil {
			return nil, err
		}
	}
	return in, nil
}

func (be *chainedBinaryEncoder) DecodeBinary(in []byte) ([]byte, error) {
	// Invoke decoders the other way around.
	for i := len(be.encoders); i > 0; i-- {
		var err error
		in, err = be.encoders[i-1].DecodeBinary(in)
		if err != nil {
			return nil, err
		}
	}
	return in, nil
}
