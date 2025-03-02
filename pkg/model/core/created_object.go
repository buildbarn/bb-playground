package core

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

var _ ReferenceMetadata = CreatedObjectTree{}

func (CreatedObjectTree) Discard() {
	// Provided to satisfy ReferenceMetadata. We intentionally make
	// this a no-op, as it is explicitly permitted to reuse
	// instances of CreatedObjectTree.
}

// ExistingCreatedObjectTree is a placeholder value for
// CreatedObjectTree that can be used to indicate that a child of a
// CreatedObject is already present in storage, and that the contents of
// this subtree are not present in memory.
var ExistingCreatedObjectTree CreatedObjectTree

type createdObjectContentsWalker struct {
	object CreatedObjectTree
}

func NewCreatedObjectContentsWalker(root CreatedObjectTree) dag.ObjectContentsWalker {
	return &createdObjectContentsWalker{
		object: root,
	}
}

func (w *createdObjectContentsWalker) GetContents(ctx context.Context) (*object.Contents, []dag.ObjectContentsWalker, error) {
	if w.object.Contents == nil {
		return nil, nil, status.Error(codes.Internal, "Contents for this object are not available for upload, as this object was expected to already exist")
	}
	walkers := make([]dag.ObjectContentsWalker, 0, len(w.object.Metadata))
	for _, child := range w.object.Metadata {
		walkers = append(walkers, NewCreatedObjectContentsWalker(child))
	}
	return w.object.Contents, walkers, nil
}

func (w *createdObjectContentsWalker) Discard() {
	// GetContents() may only be invoked once. Prevent misuse.
	w.object = CreatedObjectTree{}
}

// MapCreatedObjectsToWalkers is a helper function for creating
// ObjectContentsWalkers for all CreatedObjectTrees contained a
// ReferenceMessagePatcher.
func MapCreatedObjectsToWalkers(patcher *ReferenceMessagePatcher[CreatedObjectTree]) *ReferenceMessagePatcher[dag.ObjectContentsWalker] {
	return MapReferenceMessagePatcherMetadata(
		patcher,
		func(reference object.LocalReference, tree CreatedObjectTree) dag.ObjectContentsWalker {
			return NewCreatedObjectContentsWalker(tree)
		},
	)
}
