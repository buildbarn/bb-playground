package parser

import (
	model_core "github.com/buildbarn/bonanza/pkg/model/core"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rawObjectParser[TReference any] struct{}

func NewRawObjectParser[TReference any]() ObjectParser[TReference, []byte] {
	return &rawObjectParser[TReference]{}
}

func (p *rawObjectParser[TReference]) ParseObject(in model_core.Message[[]byte, TReference]) ([]byte, int, error) {
	if degree := in.OutgoingReferences.GetDegree(); degree > 0 {
		return nil, 0, status.Errorf(codes.InvalidArgument, "Object has a degree of %d, while zero was expected", degree)
	}
	return in.Message, len(in.Message), nil
}
