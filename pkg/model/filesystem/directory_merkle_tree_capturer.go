package filesystem

import (
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
)

type DirectoryMerkleTreeCapturer[TDirectory, TFile any] interface {
	CaptureFileNode(TFile) TDirectory
	CaptureDirectory(contents *object.Contents, children []TDirectory) TDirectory
	CaptureLeaves(contents *object.Contents, children []TDirectory) TDirectory
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
var FileDiscardingDirectoryMerkleTreeCapturer DirectoryMerkleTreeCapturer[CapturedObject, model_core.NoopReferenceMetadata] = fileDiscardingDirectoryMerkleTreeCapturer{}

func (fileDiscardingDirectoryMerkleTreeCapturer) CaptureFileNode(model_core.NoopReferenceMetadata) CapturedObject {
	return CapturedObject{}
}

func (fileDiscardingDirectoryMerkleTreeCapturer) CaptureDirectory(contents *object.Contents, children []CapturedObject) CapturedObject {
	return CapturedObject{
		Contents: contents,
		Children: children,
	}
}

func (fileDiscardingDirectoryMerkleTreeCapturer) CaptureLeaves(contents *object.Contents, children []CapturedObject) CapturedObject {
	return CapturedObject{
		Contents: contents,
		Children: children,
	}
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

func (c fileWritingDirectoryMerkleTreeCapturer) CaptureDirectory(contents *object.Contents, children []model_core.FileBackedObjectLocation) model_core.FileBackedObjectLocation {
	return c.capturer.CaptureObject(contents, children)
}

func (c fileWritingDirectoryMerkleTreeCapturer) CaptureLeaves(contents *object.Contents, children []model_core.FileBackedObjectLocation) model_core.FileBackedObjectLocation {
	return c.capturer.CaptureObject(contents, children)
}

type inMemoryDirectoryMerkleTreeCapturer struct{}

// InMemoryDirectoryMerkleTreeCapturer is an instance of
// DirectoryMerkleTreeCapturer that keeps all objects in memory.
var InMemoryDirectoryMerkleTreeCapturer DirectoryMerkleTreeCapturer[dag.ObjectContentsWalker, dag.ObjectContentsWalker] = inMemoryDirectoryMerkleTreeCapturer{}

func (inMemoryDirectoryMerkleTreeCapturer) CaptureFileNode(metadata dag.ObjectContentsWalker) dag.ObjectContentsWalker {
	return metadata
}

func (inMemoryDirectoryMerkleTreeCapturer) CaptureDirectory(contents *object.Contents, children []dag.ObjectContentsWalker) dag.ObjectContentsWalker {
	return dag.NewSimpleObjectContentsWalker(contents, children)
}

func (inMemoryDirectoryMerkleTreeCapturer) CaptureLeaves(contents *object.Contents, children []dag.ObjectContentsWalker) dag.ObjectContentsWalker {
	return dag.NewSimpleObjectContentsWalker(contents, children)
}
