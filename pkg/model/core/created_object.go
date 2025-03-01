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
