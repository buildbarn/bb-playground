package filesystem

import (
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
)

type DirectoryMerkleTreeCapturer[TDirectory, TFile any] interface {
	FileMerkleTreeCapturer[TFile]

	CaptureFileNode(TFile) TDirectory
	CaptureDirectory(contents *object.Contents, children []TDirectory) TDirectory
	CaptureLeaves(contents *object.Contents, children []TDirectory) TDirectory
}

type fileDiscardingDirectoryMerkleTreeCapturer struct{}

func (fileDiscardingDirectoryMerkleTreeCapturer) CaptureChunk(contents *object.Contents) model_core.NoopReferenceMetadata {
	return model_core.NoopReferenceMetadata{}
}

func (fileDiscardingDirectoryMerkleTreeCapturer) CaptureFileContentsList(contents *object.Contents, children []model_core.NoopReferenceMetadata) model_core.NoopReferenceMetadata {
	return model_core.NoopReferenceMetadata{}
}

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

// FileDiscardingDirectoryMerkleTreeCapturer is an instance of
// DirectoryMerkleTreeCapturer that keeps any Directory and Leaves
// objects, but discards FileContentsList and file chunk objects.
//
// Discarding the contents of files is typically the right approach for
// uploading directory structures with changes to only a small number of
// files. The Merkle trees of files can be recomputed if it turns out
// they still need to be uploaded.
var FileDiscardingDirectoryMerkleTreeCapturer DirectoryMerkleTreeCapturer[CapturedObject, model_core.NoopReferenceMetadata] = fileDiscardingDirectoryMerkleTreeCapturer{}
