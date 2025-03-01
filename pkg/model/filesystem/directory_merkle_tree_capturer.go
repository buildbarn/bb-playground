package filesystem

import (
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
)

type DirectoryMerkleTreeCapturer[TDirectory, TFile any] interface {
	CaptureFileNode(TFile) TDirectory
	CaptureDirectory(createdObject model_core.CreatedObject[TDirectory]) TDirectory
	CaptureLeaves(createdObject model_core.CreatedObject[TDirectory]) TDirectory
}

type fileDiscardingDirectoryMerkleTreeCapturer struct{}

// FileDiscardingDirectoryMerkleTreeCapturer is an instance of
// DirectoryMerkleTreeCapturer that keeps any Directory and Leaves
// objects, but discards FileContents list and file chunk objects.
//
// Discarding the contents of files is typically the right approach for
// uploading directory structures with changes to only a small number of
// files. The Merkle trees of files can be recomputed if it turns out
// they still need to be uploaded.
var FileDiscardingDirectoryMerkleTreeCapturer DirectoryMerkleTreeCapturer[model_core.CreatedObjectTree, model_core.NoopReferenceMetadata] = fileDiscardingDirectoryMerkleTreeCapturer{}

func (fileDiscardingDirectoryMerkleTreeCapturer) CaptureFileNode(model_core.NoopReferenceMetadata) model_core.CreatedObjectTree {
	return model_core.CreatedObjectTree{}
}

func (fileDiscardingDirectoryMerkleTreeCapturer) CaptureDirectory(createdObject model_core.CreatedObject[model_core.CreatedObjectTree]) model_core.CreatedObjectTree {
	return model_core.CreatedObjectTree(createdObject)
}

func (fileDiscardingDirectoryMerkleTreeCapturer) CaptureLeaves(createdObject model_core.CreatedObject[model_core.CreatedObjectTree]) model_core.CreatedObjectTree {
	return model_core.CreatedObjectTree(createdObject)
}

type fileWritingDirectoryMerkleTreeCapturer struct {
	capturer *model_core.FileWritingMerkleTreeCapturer
}

// NewFileWritingDirectoryMerkleTreeCapturer creates a
// DirectoryMerkleTreeCapturer that writes all objects belonging to
// directory Merkle tree into a file.
func NewFileWritingDirectoryMerkleTreeCapturer(capturer *model_core.FileWritingMerkleTreeCapturer) DirectoryMerkleTreeCapturer[model_core.FileBackedObjectLocation, model_core.FileBackedObjectLocation] {
	return fileWritingDirectoryMerkleTreeCapturer{
		capturer: capturer,
	}
}

func (fileWritingDirectoryMerkleTreeCapturer) CaptureFileNode(metadata model_core.FileBackedObjectLocation) model_core.FileBackedObjectLocation {
	return metadata
}

func (c fileWritingDirectoryMerkleTreeCapturer) CaptureDirectory(createdObject model_core.CreatedObject[model_core.FileBackedObjectLocation]) model_core.FileBackedObjectLocation {
	return c.capturer.CaptureObject(createdObject)
}

func (c fileWritingDirectoryMerkleTreeCapturer) CaptureLeaves(createdObject model_core.CreatedObject[model_core.FileBackedObjectLocation]) model_core.FileBackedObjectLocation {
	return c.capturer.CaptureObject(createdObject)
}

type inMemoryDirectoryMerkleTreeCapturer struct{}

// InMemoryDirectoryMerkleTreeCapturer is an instance of
// DirectoryMerkleTreeCapturer that keeps all objects in memory.
var InMemoryDirectoryMerkleTreeCapturer DirectoryMerkleTreeCapturer[dag.ObjectContentsWalker, dag.ObjectContentsWalker] = inMemoryDirectoryMerkleTreeCapturer{}

func (inMemoryDirectoryMerkleTreeCapturer) CaptureFileNode(metadata dag.ObjectContentsWalker) dag.ObjectContentsWalker {
	return metadata
}

func (inMemoryDirectoryMerkleTreeCapturer) CaptureDirectory(createdObject model_core.CreatedObject[dag.ObjectContentsWalker]) dag.ObjectContentsWalker {
	return dag.NewSimpleObjectContentsWalker(createdObject.Contents, createdObject.Metadata)
}

func (inMemoryDirectoryMerkleTreeCapturer) CaptureLeaves(createdObject model_core.CreatedObject[dag.ObjectContentsWalker]) dag.ObjectContentsWalker {
	return dag.NewSimpleObjectContentsWalker(createdObject.Contents, createdObject.Metadata)
}
