package parser

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RawObjectParserReference interface {
	GetSizeBytes() int
}

type rawObjectParser[TReference RawObjectParserReference] struct{}

func NewRawObjectParser[TReference RawObjectParserReference]() ObjectParser[TReference, []byte] {
	return &rawObjectParser[TReference]{}
}

func (p *rawObjectParser[TReference]) ParseObject(ctx context.Context, reference TReference, outgoingReferences object.OutgoingReferences[object.LocalReference], data []byte) ([]byte, int, error) {
	if degree := outgoingReferences.GetDegree(); degree > 0 {
		return nil, 0, status.Errorf(codes.InvalidArgument, "Object has a degree of %d, while zero was expected", degree)
	}
	return data, reference.GetSizeBytes(), nil
}
