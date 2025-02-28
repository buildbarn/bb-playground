package core

import (
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

type Message[T any] struct {
	Message            T
	OutgoingReferences object.OutgoingReferences
}

// NewSimpleMessage is a helper function for creating instances of
// Message for messages that don't contain any references.
func NewSimpleMessage[TMessage any](v TMessage) Message[TMessage] {
	return Message[TMessage]{
		Message:            v,
		OutgoingReferences: object.OutgoingReferencesList{},
	}
}

// NewNestedMessage is a helper function for creating instances of
// Message that refer to a message that was mebedded into another one.
func NewNestedMessage[T1, T2 any](parent Message[T1], child T2) Message[T2] {
	return Message[T2]{
		Message:            child,
		OutgoingReferences: parent.OutgoingReferences,
	}
}

func (m Message[T]) IsSet() bool {
	return m.OutgoingReferences != nil
}

func (m *Message[T]) Clear() {
	*m = Message[T]{}
}

// GetOutgoingReference is a utility function for obtaining an outgoing
// reference corresponding to Reference message that is a child of the
// current message.
func (m *Message[T]) GetOutgoingReference(reference *model_core_pb.Reference) (object.LocalReference, error) {
	index, err := GetIndexFromReferenceMessage(reference, m.OutgoingReferences.GetDegree())
	if err != nil {
		var badReference object.LocalReference
		return badReference, err
	}
	return m.OutgoingReferences.GetOutgoingReference(index), nil
}

func MessagesEqual[T proto.Message](m1, m2 Message[T]) bool {
	degree1, degree2 := m1.OutgoingReferences.GetDegree(), m2.OutgoingReferences.GetDegree()
	if degree1 != degree2 {
		return false
	}
	for i := 0; i < degree1; i++ {
		if m1.OutgoingReferences.GetOutgoingReference(i) != m2.OutgoingReferences.GetOutgoingReference(i) {
			return false
		}
	}
	return proto.Equal(m1.Message, m2.Message)
}
