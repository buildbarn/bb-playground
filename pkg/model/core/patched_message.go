package core

import (
	model_encoding "github.com/buildbarn/bonanza/pkg/model/encoding"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

// PatchedMessage is a tuple for storing a Protobuf message that
// contains model_core_pb.Reference messages, and the associated
// ReferenceMessagePatcher that can be used to assign indices to these
// references.
type PatchedMessage[TMessage any, TMetadata ReferenceMetadata] struct {
	Message TMessage
	Patcher *ReferenceMessagePatcher[TMetadata]
}

func NewPatchedMessage[TMessage any, TMetadata ReferenceMetadata](
	message TMessage,
	patcher *ReferenceMessagePatcher[TMetadata],
) PatchedMessage[TMessage, TMetadata] {
	return PatchedMessage[TMessage, TMetadata]{
		Message: message,
		Patcher: patcher,
	}
}

func NewPatchedMessageFromExisting[
	TMessage any,
	TMetadata ReferenceMetadata,
	TMessagePtr interface {
		*TMessage
		proto.Message
	},
](
	existing Message[TMessagePtr],
	createMetadata ReferenceMetadataCreator[TMetadata],
) PatchedMessage[TMessagePtr, TMetadata] {
	patcher := NewReferenceMessagePatcher[TMetadata]()
	if existing.Message == nil || existing.OutgoingReferences.GetDegree() == 0 {
		return PatchedMessage[TMessagePtr, TMetadata]{
			Message: existing.Message,
			Patcher: patcher,
		}
	}

	clonedMessage := proto.Clone(existing.Message)
	patcher.addReferenceMessagesRecursively(
		clonedMessage.ProtoReflect(),
		existing.OutgoingReferences,
		createMetadata,
	)
	return PatchedMessage[TMessagePtr, TMetadata]{
		Message: clonedMessage.(TMessagePtr),
		Patcher: patcher,
	}
}

// NewSimplePatchedMessage is a helper function for creating instances
// of PatchedMessage for messages that don't contain any references.
func NewSimplePatchedMessage[TMetadata ReferenceMetadata, TMessage any](v TMessage) PatchedMessage[TMessage, TMetadata] {
	return PatchedMessage[TMessage, TMetadata]{
		Message: v,
		Patcher: NewReferenceMessagePatcher[TMetadata](),
	}
}

func (m PatchedMessage[T, TMetadata]) IsSet() bool {
	return m.Patcher != nil
}

func (m *PatchedMessage[T, TMetadata]) Clear() {
	*m = PatchedMessage[T, TMetadata]{}
}

func (m *PatchedMessage[T, TMetadata]) Discard() {
	if m.Patcher != nil {
		m.Patcher.Discard()
	}
	m.Clear()
}

// SortAndSetReferences assigns indices to outgoing references
func (m PatchedMessage[T, TMetadata]) SortAndSetReferences() (Message[T], []TMetadata) {
	references, metadata := m.Patcher.SortAndSetReferences()
	return Message[T]{
		Message:            m.Message,
		OutgoingReferences: references,
	}, metadata
}

// MarshalAndEncodePatchedMessage marshals a Protobuf message, encodes
// it, and converts it to an object that can be written to storage.
func MarshalAndEncodePatchedMessage[TMessage proto.Message, TMetadata ReferenceMetadata](
	m PatchedMessage[TMessage, TMetadata],
	referenceFormat object.ReferenceFormat,
	encoder model_encoding.BinaryEncoder,
) (CreatedObject[TMetadata], error) {
	references, metadata := m.Patcher.SortAndSetReferences()
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(m.Message)
	if err != nil {
		return CreatedObject[TMetadata]{}, err
	}
	encodedData, err := encoder.EncodeBinary(data)
	if err != nil {
		return CreatedObject[TMetadata]{}, err
	}
	contents, err := referenceFormat.NewContents(references, encodedData)
	if err != nil {
		return CreatedObject[TMetadata]{}, err
	}
	return CreatedObject[TMetadata]{
		Contents: contents,
		Metadata: metadata,
	}, nil
}
