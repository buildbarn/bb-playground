package core

import (
	"github.com/buildbarn/bb-playground/pkg/storage/object"
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
