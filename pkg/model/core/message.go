package core

import (
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

type Message[T proto.Message] struct {
	Message            T
	OutgoingReferences object.OutgoingReferences
}

func (m *Message[T]) IsSet() bool {
	return m.OutgoingReferences != nil
}
