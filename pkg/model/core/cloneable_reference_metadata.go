package core

import (
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

// CloneableReferenceMetadata is a type of ReferenceMetadata that is
// safe to place in multiple ReferenceMessagePatchers. For example,
// ReferenceMetadata that stores the contents of an object in a file may
// not be cloneable. A simple in-memory implementation like
// CreatedObject[T] is cloneable.
type CloneableReferenceMetadata interface {
	ReferenceMetadata

	IsCloneable()
}

type CloneableReference[TMetadata any] struct {
	object.LocalReference
	Metadata TMetadata
}

// PatchedMessageToCloneable converts the Protobuf message managed by
// the ReferenceMessagePatcher to a plain Message, having the originally
// provided metadata attached.
//
// This function can be used in case a PatchedMessage does not need to
// be returned/propagated immediately and/or needs to be duplicated. It
// is later possible to reobtain PatchedMessages again by calling
// NewPatchedMessageFromCloneable().
func PatchedMessageToCloneable[TMessage any, TMetadata CloneableReferenceMetadata](m PatchedMessage[TMessage, TMetadata]) Message[TMessage, CloneableReference[TMetadata]] {
	references, metadata := m.Patcher.SortAndSetReferences()
	outgoingReferences := make(object.OutgoingReferencesList[CloneableReference[TMetadata]], 0, len(metadata))
	for i, m := range metadata {
		outgoingReferences = append(outgoingReferences, CloneableReference[TMetadata]{
			LocalReference: references.GetOutgoingReference(i),
			Metadata:       m,
		})
	}
	return NewMessage(m.Message, outgoingReferences)
}

// NewPatchedMessageFromCloneable performs the inverse of
// PatchedMessageToCloneable().
func NewPatchedMessageFromCloneable[
	TMessage any,
	TMetadata ReferenceMetadata,
	TMessagePtr interface {
		*TMessage
		proto.Message
	},
](m Message[TMessagePtr, CloneableReference[TMetadata]]) PatchedMessage[TMessagePtr, TMetadata] {
	return NewPatchedMessageFromExisting(m, func(index int) TMetadata {
		return m.OutgoingReferences.GetOutgoingReference(index).Metadata
	})
}
