package core

import (
	"github.com/buildbarn/bonanza/pkg/storage/object"
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
