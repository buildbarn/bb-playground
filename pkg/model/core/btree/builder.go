package btree

import (
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"

	"google.golang.org/protobuf/proto"
)

// Builder of B-trees.
type Builder[TNode proto.Message, TMetadata any] struct {
	chunkerFactory ChunkerFactory[TNode, TMetadata]
	nodeMerger     NodeMerger[TNode, TMetadata]

	rootChildren model_core.PatchedMessage[[]TNode, TMetadata]
	levels       []Chunker[TNode, TMetadata]
}

// NewBuilder creates a B-tree builder that is in the initial state
// (i.e., does not contain any nodes).
func NewBuilder[TNode proto.Message, TMetadata any](chunkerFactory ChunkerFactory[TNode, TMetadata], nodeMerger NodeMerger[TNode, TMetadata]) *Builder[TNode, TMetadata] {
	return &Builder[TNode, TMetadata]{
		chunkerFactory: chunkerFactory,
		nodeMerger:     nodeMerger,
	}
}

func (b *Builder[TNode, TMetadata]) pushChildrenToParent(level int, children model_core.PatchedMessage[[]TNode, TMetadata]) error {
	if level == len(b.levels) {
		if !b.rootChildren.IsSet() {
			// First node to be pushed at a given level.
			// This might be the new root node. Don't insert
			// it into the chunker just yet.
			b.rootChildren = children
			return nil
		}

		// Second node to be pushed at a given level. Construct
		// a new chunker and insert both nodes.
		b.levels = append(b.levels, b.chunkerFactory.NewChunker())
		rootNode, err := b.nodeMerger(b.rootChildren)
		if err != nil {
			return err
		}
		if err := b.levels[level].PushSingle(rootNode); err != nil {
			return err
		}
		b.rootChildren.Clear()
	}

	node, err := b.nodeMerger(children)
	if err != nil {
		return err
	}
	return b.levels[level].PushSingle(node)
}

// PushChild inserts a new node at the very end of the B-tree.
func (b *Builder[TNode, TMetadata]) PushChild(node model_core.PatchedMessage[TNode, TMetadata]) error {
	if len(b.levels) == 0 {
		if !b.rootChildren.IsSet() {
			// Very first node to be pushed into the tree.
			// The resulting B-tree may be a list consisting
			// of a single element.
			b.rootChildren = model_core.PatchedMessage[[]TNode, TMetadata]{
				Message: []TNode{node.Message},
				Patcher: node.Patcher,
			}
			return nil
		}

		// Second node to be pushed into the tree. Construct a
		// new chunker and insert both nodes.
		b.levels = append(b.levels, b.chunkerFactory.NewChunker())
		if err := b.levels[0].PushSingle(model_core.PatchedMessage[TNode, TMetadata]{
			Message: b.rootChildren.Message[0],
			Patcher: b.rootChildren.Patcher,
		}); err != nil {
			return err
		}
		b.rootChildren.Clear()
	}
	if err := b.levels[0].PushSingle(node); err != nil {
		return err
	}

	// See if there are any new parent nodes that we can propagate upward.
	for childLevel := 0; childLevel < len(b.levels); childLevel++ {
		children := b.levels[childLevel].PopMultiple(false)
		if !children.IsSet() {
			return nil
		}
		parentLevel := childLevel + 1
		for {
			if err := b.pushChildrenToParent(parentLevel, children); err != nil {
				return err
			}
			children = b.levels[childLevel].PopMultiple(false)
			if !children.IsSet() {
				break
			}
		}
	}
	return nil
}

// Drain parent nodes at each level until a single node root remains.
func (b *Builder[TNode, TMetadata]) drain() error {
	for childLevel := 0; childLevel < len(b.levels); childLevel++ {
		parentLevel := childLevel + 1
		for {
			children := b.levels[childLevel].PopMultiple(true)
			if !children.IsSet() {
				break
			}
			if err := b.pushChildrenToParent(parentLevel, children); err != nil {
				return err
			}
		}
	}
	return nil
}

// FinalizeList finalizes the B-tree by returning the list of nodes to
// be contained in the root node. If the B-tree contains no entries, an
// empty list is returned.
func (b *Builder[TNode, TMetadata]) FinalizeList() (model_core.PatchedMessage[[]TNode, TMetadata], error) {
	if err := b.drain(); err != nil {
		return model_core.PatchedMessage[[]TNode, TMetadata]{}, err
	}
	if b.rootChildren.IsSet() {
		return b.rootChildren, nil
	}
	return model_core.PatchedMessage[[]TNode, TMetadata]{
		Patcher: model_core.NewReferenceMessagePatcher[TMetadata](),
	}, nil
}

// FinalizeSingle finalizes the B-tree by returning the root node. If
// the B-tree contains no entries, nothing is returned.
func (b *Builder[TNode, TMetadata]) FinalizeSingle() (model_core.PatchedMessage[TNode, TMetadata], error) {
	if err := b.drain(); err != nil {
		return model_core.PatchedMessage[TNode, TMetadata]{}, err
	}
	switch len(b.rootChildren.Message) {
	case 0:
		return model_core.PatchedMessage[TNode, TMetadata]{}, nil
	case 1:
		return model_core.PatchedMessage[TNode, TMetadata]{
			Message: b.rootChildren.Message[0],
			Patcher: b.rootChildren.Patcher,
		}, nil
	default:
		return b.nodeMerger(b.rootChildren)
	}
}
