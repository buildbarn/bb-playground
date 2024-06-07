package btree

import (
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"

	"google.golang.org/protobuf/proto"
)

// LevelBuilderFactory is a factory type for creating builders of
// individual levels of a B-tree.
type LevelBuilderFactory[TNode proto.Message, TMetadata any] interface {
	NewLevelBuilder() LevelBuilder[TNode, TMetadata]
}

// LevelBuilder is responsible for storing state for objects that are in
// the process of being constructed at a given level of the B-tree.
type LevelBuilder[TNode proto.Message, TMetadata any] interface {
	PushChild(node model_core.MessageWithReferences[TNode, TMetadata]) error
	PopParent(finalize bool) (*model_core.MessageWithReferences[TNode, TMetadata], error)
}

// Builder of B-trees.
type Builder[TNode proto.Message, TMetadata any] struct {
	levelBuilderFactory LevelBuilderFactory[TNode, TMetadata]

	rootNode *model_core.MessageWithReferences[TNode, TMetadata]
	levels   []LevelBuilder[TNode, TMetadata]
}

// NewBuilder creates a B-tree builder that is in the initial state
// (i.e., does not contain any nodes).
func NewBuilder[TNode proto.Message, TMetadata any](levelBuilderFactory LevelBuilderFactory[TNode, TMetadata]) *Builder[TNode, TMetadata] {
	return &Builder[TNode, TMetadata]{
		levelBuilderFactory: levelBuilderFactory,
	}
}

func (b *Builder[TNode, TMetadata]) pushChildAtLevel(level int, node model_core.MessageWithReferences[TNode, TMetadata]) error {
	if level == len(b.levels) {
		if b.rootNode == nil {
			// First node to be pushed at a given level.
			// This might be the new root node. Don't insert
			// it into a level builder just yet.
			b.rootNode = &node
			return nil
		}

		// Second node to be pushed at a given level. Construct
		// the new level builder and insert both nodes.
		b.levels = append(b.levels, b.levelBuilderFactory.NewLevelBuilder())
		if err := b.levels[level].PushChild(*b.rootNode); err != nil {
			return err
		}
		b.rootNode = nil
	}
	return b.levels[level].PushChild(node)
}

// PushChild inserts a new node at the very end of the B-tree.
func (b *Builder[TNode, TMetadata]) PushChild(node model_core.MessageWithReferences[TNode, TMetadata]) error {
	if err := b.pushChildAtLevel(0, node); err != nil {
		return err
	}

	// See if there are any new parent nodes that we can propagate upward.
	for childLevel := 0; childLevel < len(b.levels); childLevel++ {
		parentNode, err := b.levels[childLevel].PopParent(false)
		if err != nil || parentNode == nil {
			return err
		}
		parentLevel := childLevel + 1
		for {
			if err := b.pushChildAtLevel(parentLevel, *parentNode); err != nil {
				return err
			}
			parentNode, err = b.levels[childLevel].PopParent(false)
			if err != nil {
				return err
			}
			if parentNode == nil {
				break
			}
		}
	}
	return nil
}

// Finalize the B-tree by returning its root node.
func (b *Builder[TNode, TMetadata]) Finalize() (node *model_core.MessageWithReferences[TNode, TMetadata], err error) {
	// Drain parent nodes at each level until a single node root remains.
	for childLevel := 0; childLevel < len(b.levels); childLevel++ {
		parentLevel := childLevel + 1
		for {
			parentNode, err := b.levels[childLevel].PopParent(true)
			if err != nil {
				return nil, err
			}
			if parentNode == nil {
				break
			}
			if err := b.pushChildAtLevel(parentLevel, *parentNode); err != nil {
				return nil, err
			}
		}
	}
	return b.rootNode, nil
}

type (
	LevelBuilderForTesting        LevelBuilder[*model_filesystem_pb.FileContents, string]
	LevelBuilderFactoryForTesting LevelBuilderFactory[*model_filesystem_pb.FileContents, string]
)
