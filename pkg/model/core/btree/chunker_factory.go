package btree

import (
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"

	"google.golang.org/protobuf/proto"
)

// ChunkerFactory is a factory type for creating chunkers of individual
// levels of a B-tree.
type ChunkerFactory[TNode proto.Message, TMetadata any] interface {
	NewChunker() Chunker[TNode, TMetadata]
}

// Chunker is responsible for determining how nodes at a given level in
// the B-tree are chunked and spread out across sibling objects at the
// same level.
type Chunker[TNode proto.Message, TMetadata any] interface {
	PushSingle(node model_core.PatchedMessage[TNode, TMetadata]) error
	PopMultiple(finalize bool) model_core.PatchedMessage[[]TNode, TMetadata]
}

type (
	ChunkerFactoryForTesting ChunkerFactory[*model_filesystem_pb.FileContents, string]
	ChunkerForTesting        Chunker[*model_filesystem_pb.FileContents, string]
)
