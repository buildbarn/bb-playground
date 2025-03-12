package core

import (
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

// Message is a piece of data, typically a Protobuf message, that has
// zero or more outgoing references associated with it.
type Message[TMessage any, TReference any] struct {
	Message            TMessage
	OutgoingReferences object.OutgoingReferences[TReference]
}

// NewMessage is a helper function for creating instances of Message.
func NewMessage[TMessage, TReference any](
	m TMessage,
	outgoingReferences object.OutgoingReferences[TReference],
) Message[TMessage, TReference] {
	return Message[TMessage, TReference]{
		Message:            m,
		OutgoingReferences: outgoingReferences,
	}
}

// NewSimpleMessage is a helper function for creating instances of
// Message for messages that don't contain any references.
func NewSimpleMessage[TReference, TMessage any](m TMessage) Message[TMessage, TReference] {
	return Message[TMessage, TReference]{
		Message:            m,
		OutgoingReferences: object.OutgoingReferences[TReference](object.OutgoingReferencesList[TReference]{}),
	}
}

// NewNestedMessage is a helper function for creating instances of
// Message that refer to a message that was mebedded into another one.
func NewNestedMessage[TMessage1, TMessage2, TReference any](parent Message[TMessage1, TReference], child TMessage2) Message[TMessage2, TReference] {
	return Message[TMessage2, TReference]{
		Message:            child,
		OutgoingReferences: parent.OutgoingReferences,
	}
}

// IsSet returns true if the instance of Message is initialized.
func (m Message[TMessage, TReference]) IsSet() bool {
	return m.OutgoingReferences != nil
}

// Clear the Message, releasing the data and outgoing references that
// are associated with it.
func (m *Message[TMessage, TReference]) Clear() {
	*m = Message[TMessage, TReference]{}
}

// FlattenReference returns the actual reference that is associated with
// a given Reference Protobuf message.
func FlattenReference[TReference any](m Message[*model_core_pb.Reference, TReference]) (TReference, error) {
	index, err := GetIndexFromReferenceMessage(m.Message, m.OutgoingReferences.GetDegree())
	if err != nil {
		var badReference TReference
		return badReference, err
	}
	return m.OutgoingReferences.GetOutgoingReference(index), nil
}

func MessagesEqual[
	TMessage proto.Message,
	TReference1, TReference2 object.BasicReference,
](m1 Message[TMessage, TReference1], m2 Message[TMessage, TReference2]) bool {
	degree1, degree2 := m1.OutgoingReferences.GetDegree(), m2.OutgoingReferences.GetDegree()
	if degree1 != degree2 {
		return false
	}
	for i := 0; i < degree1; i++ {
		if m1.OutgoingReferences.GetOutgoingReference(i).GetLocalReference() != m2.OutgoingReferences.GetOutgoingReference(i).GetLocalReference() {
			return false
		}
	}
	return proto.Equal(m1.Message, m2.Message)
}
