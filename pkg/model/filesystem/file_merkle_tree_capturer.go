package filesystem

import (
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

// FileMerkleTreeCapturer is provided by callers of CreateFileMerkleTree
// to provide logic for how the resulting Merkle tree of the file should
// be captured.
//
// A no-op implementation can be used by the caller to simply compute a
// reference of the file. An implementation that actually captures the
// provided contents can be used to prepare a Merkle tree for uploading.
//
// The methods below return metadata. The metadata for the root object
// will be returned by CreateFileMerkleTree.
type FileMerkleTreeCapturer[T any] interface {
	CaptureChunk(contents *object.Contents) T
	CaptureFileContentsList(createdObject model_core.CreatedObject[T]) T
}

type noopFileMerkleTreeCapturer struct{}

// NoopFileMerkleTreeCapturer is a no-op implementation of
// FileMerkleTreeCapturer. It can be used when only a reference of a
// file needs to be computed, and there is no need to capture the
// resulting Merkle tree.
var NoopFileMerkleTreeCapturer FileMerkleTreeCapturer[model_core.NoopReferenceMetadata] = noopFileMerkleTreeCapturer{}

func (noopFileMerkleTreeCapturer) CaptureChunk(contents *object.Contents) model_core.NoopReferenceMetadata {
	return model_core.NoopReferenceMetadata{}
}

func (noopFileMerkleTreeCapturer) CaptureFileContentsList(createdObject model_core.CreatedObject[model_core.NoopReferenceMetadata]) model_core.NoopReferenceMetadata {
	return model_core.NoopReferenceMetadata{}
}

type chunkDiscardingFileMerkleTreeCapturer struct{}

// ChunkDiscardingFileMerkleTreeCapturer is an implementation of
// FileMerkleTreeCapturer that only preserves the FileContents messages
// of the Merkle tree. This can be of use when incrementally replicating
// the contents of a file. In those cases it's wasteful to store the
// full contents of a file in memory.
var ChunkDiscardingFileMerkleTreeCapturer FileMerkleTreeCapturer[model_core.CreatedObjectTree] = chunkDiscardingFileMerkleTreeCapturer{}

func (chunkDiscardingFileMerkleTreeCapturer) CaptureChunk(contents *object.Contents) model_core.CreatedObjectTree {
	return model_core.CreatedObjectTree{}
}

func (chunkDiscardingFileMerkleTreeCapturer) CaptureFileContentsList(createdObject model_core.CreatedObject[model_core.CreatedObjectTree]) model_core.CreatedObjectTree {
	o := model_core.CreatedObjectTree{
		Contents: createdObject.Contents,
	}
	if createdObject.Contents.GetReference().GetHeight() > 1 {
		o.Metadata = createdObject.Metadata
	}
	return o
}

type fileWritingFileMerkleTreeCapturer struct {
	capturer *model_core.FileWritingMerkleTreeCapturer
}

func NewFileWritingFileMerkleTreeCapturer(capturer *model_core.FileWritingMerkleTreeCapturer) FileMerkleTreeCapturer[model_core.FileBackedObjectLocation] {
	return fileWritingFileMerkleTreeCapturer{
		capturer: capturer,
	}
}

func (c fileWritingFileMerkleTreeCapturer) CaptureChunk(contents *object.Contents) model_core.FileBackedObjectLocation {
	return c.capturer.CaptureObject(model_core.CreatedObject[model_core.FileBackedObjectLocation]{Contents: contents})
}

func (c fileWritingFileMerkleTreeCapturer) CaptureFileContentsList(createdObject model_core.CreatedObject[model_core.FileBackedObjectLocation]) model_core.FileBackedObjectLocation {
	return c.capturer.CaptureObject(createdObject)
}

type FileMerkleTreeCapturerForTesting FileMerkleTreeCapturer[model_core.ReferenceMetadata]
