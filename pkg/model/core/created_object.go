package core

import (
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

// CreatedObject holds the contents of an object that was created using
// ReferenceMessagePatcher. It also holds the metadata that was provided
// to ReferenceMessagePatcher.AddReference(), provided in the same order
// as the outgoing references of the created object.
type CreatedObject[TMetadata any] struct {
	Contents *object.Contents
	Metadata []TMetadata
}

// CreatedObjectTree is CreatedObject applied recursively. Namely, it
// can hold the contents of a tree of objects that were created using
// ReferenceMessagePatcher in memory.
type CreatedObjectTree CreatedObject[CreatedObjectTree]

var _ CloneableReferenceMetadata = CreatedObjectTree{}

// Discard any resources owned by the CreatedObjectTree.
func (CreatedObjectTree) Discard() {}

// IsCloneable indicates that instances of CreatedObjectTree may safely
// be placed in multiple ReferenceMessagePatchers.
func (CreatedObjectTree) IsCloneable() {}

// CreatedObjectCapturer can be used as a factory type for reference
// metadata. Given the contents of an object and the metadata of all of
// its children, it may yield new metadata.
type CreatedObjectCapturer[TMetadata any] interface {
	CaptureCreatedObject(CreatedObject[TMetadata]) TMetadata
}

// CreatedOrExistingObjectCapturer is an extension to
// CreatedObjectCapturer, also providing a method to construct metadata
// belonging to objects that already exist in storage. This can be used
// to efficiently construct Merkle trees that are derived from ones
// created previously.
type CreatedOrExistingObjectCapturer[TReference, TMetadata any] interface {
	CreatedObjectCapturer[TMetadata]

	CaptureExistingObject(TReference) TMetadata
}

// NewPatchedMessageFromExistingCaptured is identical to
// NewPatchedMessageFromExisting, except that it automatically creates
// metadata for all references contained the message using the provided
// capturer.
func NewPatchedMessageFromExistingCaptured[
	TMessage any,
	TMetadata ReferenceMetadata,
	TMessagePtr interface {
		*TMessage
		proto.Message
	},
	TReference object.BasicReference,
](
	capturer CreatedOrExistingObjectCapturer[TReference, TMetadata],
	m Message[TMessagePtr, TReference],
) PatchedMessage[TMessagePtr, TMetadata] {
	return NewPatchedMessageFromExisting(
		m,
		func(index int) TMetadata {
			return capturer.CaptureExistingObject(
				m.OutgoingReferences.GetOutgoingReference(index),
			)
		},
	)
}
