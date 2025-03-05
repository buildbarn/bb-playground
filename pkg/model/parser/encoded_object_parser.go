package parser

import (
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_encoding "github.com/buildbarn/bonanza/pkg/model/encoding"
)

type encodedObjectParser[TReference any] struct {
	encoder model_encoding.BinaryEncoder
}

func NewEncodedObjectParser[
	TReference any,
](encoder model_encoding.BinaryEncoder) ObjectParser[TReference, model_core.Message[[]byte, TReference]] {
	return &encodedObjectParser[TReference]{
		encoder: encoder,
	}
}

func (p *encodedObjectParser[TReference]) ParseObject(in model_core.Message[[]byte, TReference]) (model_core.Message[[]byte, TReference], int, error) {
	decoded, err := p.encoder.DecodeBinary(in.Message)
	if err != nil {
		return model_core.Message[[]byte, TReference]{}, 0, nil
	}
	return model_core.NewMessage(decoded, in.OutgoingReferences), len(decoded), nil
}
