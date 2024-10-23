package filesystem

import (
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
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
	CaptureFileContentsList(contents *object.Contents, children []T) T
}

type noopFileMerkleTreeCapturer struct{}

func (noopFileMerkleTreeCapturer) CaptureChunk(contents *object.Contents) model_core.NoopReferenceMetadata {
	return model_core.NoopReferenceMetadata{}
}

func (noopFileMerkleTreeCapturer) CaptureFileContentsList(contents *object.Contents, children []model_core.NoopReferenceMetadata) model_core.NoopReferenceMetadata {
	return model_core.NoopReferenceMetadata{}
}

// NoopFileMerkleTreeCapturer is a no-op implementation of
// FileMerkleTreeCapturer. It can be used when only a reference of a
// file needs to be computed, and there is no need to capture the
// resulting Merkle tree.
var NoopFileMerkleTreeCapturer FileMerkleTreeCapturer[model_core.NoopReferenceMetadata] = noopFileMerkleTreeCapturer{}

type CapturedObject struct {
	Contents *object.Contents
	Children []CapturedObject
}

func (CapturedObject) Discard() {}

type chunkDiscardingFileMerkleTreeCapturer struct{}

func (chunkDiscardingFileMerkleTreeCapturer) CaptureChunk(contents *object.Contents) CapturedObject {
	return CapturedObject{}
}

func (chunkDiscardingFileMerkleTreeCapturer) CaptureFileContentsList(contents *object.Contents, children []CapturedObject) CapturedObject {
	o := CapturedObject{
		Contents: contents,
	}
	if contents.GetReference().GetHeight() > 1 {
		o.Children = children
	}
	return o
}

// ChunkDiscardingFileMerkleTreeCapturer is an implementation of
// FileMerkleTreeCapturer that only preserves the FileContentsList
// messages of the Merkle tree. This can be of use when incrementally
// replicating the contents of a file. In those cases it's wasteful to
// store the full contents of a file in memory.
var ChunkDiscardingFileMerkleTreeCapturer FileMerkleTreeCapturer[CapturedObject] = chunkDiscardingFileMerkleTreeCapturer{}

type FileMerkleTreeCapturerForTesting FileMerkleTreeCapturer[model_core.ReferenceMetadata]
