package core

import (
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

func (m Message[T]) IsSet() bool {
	return m.OutgoingReferences != nil
}

func (m *Message[T]) Clear() {
	*m = Message[T]{}
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
