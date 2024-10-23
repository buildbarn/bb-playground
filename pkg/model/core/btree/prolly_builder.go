package btree

import (
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"

	"google.golang.org/protobuf/proto"
)

// NewUniformProllyBuilder creates a builder of B-trees, assuming that
// leaf nodes in the tree are small and mere references to the actual
// data. For example, for files the leaf nodes in the B-tree are just
// references to the a chunk. For such trees, each object in the B-tree
// should contain at least two nodes.
func NewUniformProllyBuilder[TNode proto.Message, TMetadata model_core.ReferenceMetadata](minimumSizeBytes, maximumSizeBytes int, nodeMerger NodeMerger[TNode, TMetadata]) Builder[TNode, TMetadata] {
	return NewUniformBuilder(
		NewProllyChunkerFactory[TNode, TMetadata](
			/* minimumCount = */ 2,
			minimumSizeBytes,
			maximumSizeBytes,
		),
		nodeMerger,
	)
}

// NewSplitProllyBuilder creates a builder of B-trees, assuming that
// leaf nodes may be large. In those cases leaf objects should be
// permitted to contain a single node, if it turns out it's too large to
// store alongside its siblings.
func NewSplitProllyBuilder[TNode proto.Message, TMetadata model_core.ReferenceMetadata](minimumSizeBytes, maximumSizeBytes int, nodeMerger NodeMerger[TNode, TMetadata]) Builder[TNode, TMetadata] {
	return NewSplitBuilder(
		NewProllyChunkerFactory[TNode, TMetadata](
			/* minimumCount = */ 1,
			minimumSizeBytes,
			maximumSizeBytes,
		).NewChunker(),
		nodeMerger,
		NewUniformBuilder(
			NewProllyChunkerFactory[TNode, TMetadata](
				/* minimumCount = */ 2,
				minimumSizeBytes,
				maximumSizeBytes,
			),
			nodeMerger,
		),
	)
}
