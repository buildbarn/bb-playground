package parser

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/util"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/object"

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
]() ObjectParser[TReference, model_core.Message[TMessagePtr, object.OutgoingReferences[object.LocalReference]]] {
	return &messageObjectParser[TReference, TMessage, TMessagePtr]{}
}

func (p *messageObjectParser[TReference, TMessage, TMessagePtr]) ParseObject(ctx context.Context, reference TReference, outgoingReferences object.OutgoingReferences[object.LocalReference], data []byte) (model_core.Message[TMessagePtr, object.OutgoingReferences[object.LocalReference]], int, error) {
	var message TMessage
	if err := proto.Unmarshal(data, TMessagePtr(&message)); err != nil {
		return model_core.Message[TMessagePtr, object.OutgoingReferences[object.LocalReference]]{}, 0, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to unmarshal message")
	}
	return model_core.Message[TMessagePtr, object.OutgoingReferences[object.LocalReference]]{
		Message:            &message,
		OutgoingReferences: outgoingReferences.GetOutgoingReferencesList(),
	}, reference.GetSizeBytes(), nil
}
