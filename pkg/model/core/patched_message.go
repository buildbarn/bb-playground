package core

import (
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

// PatchedMessage is a tuple for storing a Protobuf message that
// contains model_core_pb.Reference messages, and the associated
// ReferenceMessagePatcher that can be used to assign indices to these
// references.
type PatchedMessage[TMessage proto.Message, TMetadata any] struct {
	Message TMessage
	Patcher *ReferenceMessagePatcher[TMetadata]
}

func NewPatchedMessageFromExisting[TMessage proto.Message, TMetadata any](
	existing Message[TMessage],
	createMetadata func(reference object.LocalReference) TMetadata,
) (PatchedMessage[TMessage, TMetadata], error) {
	clonedMessage := proto.Clone(existing.Message)
	patcher := NewReferenceMessagePatcher[TMetadata]()
	if err := patcher.addReferenceMessagesRecursively(
		clonedMessage.ProtoReflect(),
		existing.OutgoingReferences,
		createMetadata,
	); err != nil {
		return PatchedMessage[TMessage, TMetadata]{}, err
	}
	return PatchedMessage[TMessage, TMetadata]{
		Message: clonedMessage.(TMessage),
		Patcher: patcher,
	}, nil
}

func (m *PatchedMessage[T, TMetadata]) IsSet() bool {
	return m.Patcher != nil
}
