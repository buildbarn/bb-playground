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

func TestBuilder(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("EmptyTree", func(t *testing.T) {
		levelBuilderFactory := NewMockLevelBuilderFactoryForTesting(ctrl)
		builder := btree.NewBuilder[*model_filesystem_pb.FileContents, string](levelBuilderFactory)

		rootNode, err := builder.Finalize()
		require.NoError(t, err)
		require.Nil(t, rootNode)
	})

	t.Run("SingleNodeTree", func(t *testing.T) {
		levelBuilderFactory := NewMockLevelBuilderFactoryForTesting(ctrl)
		builder := btree.NewBuilder[*model_filesystem_pb.FileContents, string](levelBuilderFactory)

		patcher := model_core.NewReferenceMessagePatcher[string]()
		node := model_core.MessageWithReferences[*model_filesystem_pb.FileContents, string]{
			Message: &model_filesystem_pb.FileContents{
				Reference: patcher.AddReference(
					object.MustNewSHA256V1LocalReference("8e81422ce5470c6fde1f2455d2eb0eb0eec4d6352eada7c36f99c8182dd3a1df", 42, 0, 0, 0),
					"Hello",
				),
				TotalSizeBytes: 42,
			},
			Patcher: patcher,
		}
		require.NoError(t, builder.PushChild(node))

		rootNode, err := builder.Finalize()
		require.NoError(t, err)
		require.Equal(t, node, *rootNode)
	})

	t.Run("TwoNodeTree", func(t *testing.T) {
		levelBuilderFactory := NewMockLevelBuilderFactoryForTesting(ctrl)
		builder := btree.NewBuilder[*model_filesystem_pb.FileContents, string](levelBuilderFactory)

		// Pushing the first node should only cause it to be stored.
		patcher1 := model_core.NewReferenceMessagePatcher[string]()
		fileContents1 := &model_filesystem_pb.FileContents{
			Reference: patcher1.AddReference(
				object.MustNewSHA256V1LocalReference("8e81422ce5470c6fde1f2455d2eb0eb0eec4d6352eada7c36f99c8182dd3a1df", 42, 0, 0, 0),
				"Hello",
			),
			TotalSizeBytes: 42,
		}
		node1 := model_core.MessageWithReferences[*model_filesystem_pb.FileContents, string]{
			Message: fileContents1,
			Patcher: patcher1,
		}
		require.NoError(t, builder.PushChild(node1))

		// Pushing the second node should cause a new level to
		// be created.
		patcher2 := model_core.NewReferenceMessagePatcher[string]()
		node2 := model_core.MessageWithReferences[*model_filesystem_pb.FileContents, string]{
			Message: &model_filesystem_pb.FileContents{
				Reference: patcher2.AddReference(
					object.MustNewSHA256V1LocalReference("8a5aae1152fcf85722d50b557e8462c92d0fe02e34f17aae9e70c389d4d0c140", 51, 0, 0, 0),
					"World",
				),
				TotalSizeBytes: 51,
			},
			Patcher: patcher2,
		}
		levelBuilder := NewMockLevelBuilderForTesting(ctrl)
		levelBuilderFactory.EXPECT().NewLevelBuilder().Return(levelBuilder)
		levelBuilder.EXPECT().PushChild(node1)
		levelBuilder.EXPECT().PushChild(node2)
		levelBuilder.EXPECT().PopParent(false)

		require.NoError(t, builder.PushChild(node2))

		// Finalizing the tree should cause the two-node level
		// to be finalized as well. The resulting parent node
		// should be returned as the root of the tree.
		patcher3 := model_core.NewReferenceMessagePatcher[string]()
		node3 := model_core.MessageWithReferences[*model_filesystem_pb.FileContents, string]{
			Message: &model_filesystem_pb.FileContents{
				Reference: patcher3.AddReference(
					object.MustNewSHA256V1LocalReference("4a552ba6f6bbd650497185ec68791ba2749364f493b17cbd318d6a53a2fd48eb", 100, 1, 2, 0),
					"HelloWorld",
				),
				TotalSizeBytes: 93,
			},
			Patcher: patcher3,
		}
		levelBuilder.EXPECT().PopParent(true).Return(&node3, nil)
		levelBuilder.EXPECT().PopParent(true)

		rootNode, err := builder.Finalize()
		require.NoError(t, err)
		require.Equal(t, node3, *rootNode)
	})
}
