package core

import (
	"github.com/buildbarn/bb-playground/pkg/storage/object"
)

type Message[T any] struct {
	Message            T
	OutgoingReferences object.OutgoingReferences
}

func (m Message[T]) IsSet() bool {
	return m.OutgoingReferences != nil
}
