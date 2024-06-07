package encoding

// BinaryEncoder can be used to encode binary data. Examples of encoding
// steps include compression and encryption. These encoding steps must
// be reversible.
//
// Many applications give a special meaning to empty data (e.g., the
// default value of bytes fields in a Protobuf message being). Because
// of that, implementations of BinaryEncoder should ensure that empty
// data should remain empty when encoded.
type BinaryEncoder interface {
	EncodeBinary(in []byte) ([]byte, error)
	DecodeBinary(in []byte) ([]byte, error)
}
