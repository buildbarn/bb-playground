package core

import (
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

// PatchedMessage is a tuple for storing a Protobuf message that
// contains model_core_pb.Reference messages, and the associated
// ReferenceMessagePatcher that can be used to assign indices to these
// references.
type PatchedMessage[TMessage, TMetadata any] struct {
	Message TMessage
	Patcher *ReferenceMessagePatcher[TMetadata]
}

func NewPatchedMessageFromExisting[TMessage proto.Message, TMetadata any](
	existing Message[TMessage],
	createMetadata func(reference object.LocalReference) TMetadata,
) PatchedMessage[TMessage, TMetadata] {
	clonedMessage := proto.Clone(existing.Message)
	patcher := NewReferenceMessagePatcher[TMetadata]()
	patcher.addReferenceMessagesRecursively(
		clonedMessage.ProtoReflect(),
		existing.OutgoingReferences,
		createMetadata,
	)
	return PatchedMessage[TMessage, TMetadata]{
		Message: clonedMessage.(TMessage),
		Patcher: patcher,
	}
}

func (m PatchedMessage[T, TMetadata]) IsSet() bool {
	return m.Patcher != nil
}

func (m *PatchedMessage[T, TMetadata]) Clear() {
	*m = PatchedMessage[T, TMetadata]{}
}
