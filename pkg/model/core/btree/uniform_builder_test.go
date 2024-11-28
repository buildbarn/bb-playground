package btree_test

import (
	"testing"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestUniformBuilder(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("EmptyTree", func(t *testing.T) {
		chunkerFactory := NewMockChunkerFactoryForTesting(ctrl)
		nodeMerger := NewMockNodeMergerForTesting(ctrl)
		builder := btree.NewUniformBuilder[*model_filesystem_pb.FileContents, model_core.ReferenceMetadata](chunkerFactory, nodeMerger.Call)

		rootNode, err := builder.FinalizeSingle()
		require.NoError(t, err)
		require.False(t, rootNode.IsSet())
	})

	t.Run("SingleNodeTree", func(t *testing.T) {
		chunkerFactory := NewMockChunkerFactoryForTesting(ctrl)
		nodeMerger := NewMockNodeMergerForTesting(ctrl)
		builder := btree.NewUniformBuilder[*model_filesystem_pb.FileContents, model_core.ReferenceMetadata](chunkerFactory, nodeMerger.Call)

		patcher := model_core.NewReferenceMessagePatcher[model_core.ReferenceMetadata]()
		metadata := NewMockReferenceMetadata(ctrl)
		node := model_core.NewPatchedMessage(
			&model_filesystem_pb.FileContents{
				Reference: patcher.AddReference(
					object.MustNewSHA256V1LocalReference("8e81422ce5470c6fde1f2455d2eb0eb0eec4d6352eada7c36f99c8182dd3a1df", 42, 0, 0, 0),
					metadata,
				),
				TotalSizeBytes: 42,
			},
			patcher,
		)
		require.NoError(t, builder.PushChild(node))

		rootNode, err := builder.FinalizeSingle()
		require.NoError(t, err)
		require.Equal(t, node, rootNode)
	})

	t.Run("TwoNodeTree", func(t *testing.T) {
		chunkerFactory := NewMockChunkerFactoryForTesting(ctrl)
		nodeMerger := NewMockNodeMergerForTesting(ctrl)
		builder := btree.NewUniformBuilder[*model_filesystem_pb.FileContents, model_core.ReferenceMetadata](chunkerFactory, nodeMerger.Call)

		// Pushing the first node should only cause it to be stored.
		patcher1 := model_core.NewReferenceMessagePatcher[model_core.ReferenceMetadata]()
		metadata1 := NewMockReferenceMetadata(ctrl)
		fileContents1 := &model_filesystem_pb.FileContents{
			Reference: patcher1.AddReference(
				object.MustNewSHA256V1LocalReference("8e81422ce5470c6fde1f2455d2eb0eb0eec4d6352eada7c36f99c8182dd3a1df", 42, 0, 0, 0),
				metadata1,
			),
			TotalSizeBytes: 42,
		}
		node1 := model_core.NewPatchedMessage(fileContents1, patcher1)
		require.NoError(t, builder.PushChild(node1))

		// Pushing the second node should cause a new level to
		// be created.
		patcher2 := model_core.NewReferenceMessagePatcher[model_core.ReferenceMetadata]()
		metadata2 := NewMockReferenceMetadata(ctrl)
		node2 := model_core.NewPatchedMessage(
			&model_filesystem_pb.FileContents{
				Reference: patcher2.AddReference(
					object.MustNewSHA256V1LocalReference("8a5aae1152fcf85722d50b557e8462c92d0fe02e34f17aae9e70c389d4d0c140", 51, 0, 0, 0),
					metadata2,
				),
				TotalSizeBytes: 51,
			},
			patcher2,
		)
		chunker := NewMockChunkerForTesting(ctrl)
		chunkerFactory.EXPECT().NewChunker().Return(chunker)
		chunker.EXPECT().PushSingle(node1)
		chunker.EXPECT().PushSingle(node2)
		chunker.EXPECT().PopMultiple(false)

		require.NoError(t, builder.PushChild(node2))

		// Finalizing the tree should cause the two-node level
		// to be finalized as well. The resulting parent node
		// should be returned as the root of the tree.
		patcher1.Merge(patcher2)
		nodes := model_core.NewPatchedMessage(
			[]*model_filesystem_pb.FileContents{
				node1.Message,
				node2.Message,
			},
			patcher1,
		)
		chunker.EXPECT().PopMultiple(true).Return(nodes)
		chunker.EXPECT().PopMultiple(true)
		patcher3 := model_core.NewReferenceMessagePatcher[model_core.ReferenceMetadata]()
		metadata3 := NewMockReferenceMetadata(ctrl)
		node3 := model_core.NewPatchedMessage(
			&model_filesystem_pb.FileContents{
				Reference: patcher3.AddReference(
					object.MustNewSHA256V1LocalReference("4a552ba6f6bbd650497185ec68791ba2749364f493b17cbd318d6a53a2fd48eb", 100, 1, 2, 0),
					metadata3,
				),
				TotalSizeBytes: 93,
			},
			patcher3,
		)
		nodeMerger.EXPECT().Call(nodes).
			DoAndReturn(func(model_core.PatchedMessage[[]*model_filesystem_pb.FileContents, model_core.ReferenceMetadata]) (model_core.PatchedMessage[*model_filesystem_pb.FileContents, model_core.ReferenceMetadata], error) {
				return node3, nil
			})

		rootNode, err := builder.FinalizeSingle()
		require.NoError(t, err)
		require.Equal(t, node3, rootNode)
	})
}
