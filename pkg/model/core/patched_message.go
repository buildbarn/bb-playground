package core

import (
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
