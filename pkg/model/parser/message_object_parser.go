package parser

import (
	"context"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

type MessageObjectParserReference interface {
	GetSizeBytes() int
}

type messageObjectParser[
	TReference MessageObjectParserReference,
	TMessage any,
	TMessagePtr interface {
		*TMessage
		proto.Message
	},
] struct{}

func NewMessageObjectParser[
	TReference MessageObjectParserReference,
	TMessage any,
	TMessagePtr interface {
		*TMessage
		proto.Message
	},
]() ObjectParser[TReference, model_core.Message[TMessagePtr]] {
	return &messageObjectParser[TReference, TMessage, TMessagePtr]{}
}

func (p *messageObjectParser[TReference, TMessage, TMessagePtr]) ParseObject(ctx context.Context, reference TReference, outgoingReferences object.OutgoingReferences, data []byte) (model_core.Message[TMessagePtr], int, error) {
	var message TMessage
	if err := proto.Unmarshal(data, TMessagePtr(&message)); err != nil {
		return model_core.Message[TMessagePtr]{}, 0, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to unmarshal message")
	}
	return model_core.Message[TMessagePtr]{
		Message:            &message,
		OutgoingReferences: outgoingReferences.GetOutgoingReferencesList(),
	}, reference.GetSizeBytes(), nil
}
