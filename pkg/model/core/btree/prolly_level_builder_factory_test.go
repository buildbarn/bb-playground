package btree_test

import (
	"fmt"
	"testing"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	object_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestProllyLevelBuilderFactory(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("Empty", func(t *testing.T) {
		// If we don't push any children into the level builder,
		// PopParent() should always return a nil node, even
		// when finalizing.
		encoder := NewMockBinaryEncoder(ctrl)
		parentNodeComputer := NewMockParentNodeComputerForTesting(ctrl)
		levelBuilderFactory := btree.NewProllyLevelBuilderFactory(
			/* minimumCount = */ 2,
			/* minimumSizeBytes = */ 1024,
			/* maximumSizeBytes = */ 4*1024,
			encoder,
			object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
			parentNodeComputer.Call,
		)
		levelBuilder := levelBuilderFactory.NewLevelBuilder()

		node, err := levelBuilder.PopParent(false)
		require.NoError(t, err)
		require.Nil(t, node)

		node, err = levelBuilder.PopParent(true)
		require.NoError(t, err)
		require.Nil(t, node)
	})

	t.Run("TinyNodes", func(t *testing.T) {
		// Test that minimumCount is respected when creating
		// nodes. We insert 10 nodes. These should be spread out
		// across 3 objects containing [3, 3, 4] nodes.
		encoder := NewMockBinaryEncoder(ctrl)
		parentNodeComputer := NewMockParentNodeComputerForTesting(ctrl)
		levelBuilderFactory := btree.NewProllyLevelBuilderFactory(
			/* minimumCount = */ 3,
			/* minimumSizeBytes = */ 1,
			/* maximumSizeBytes = */ 2,
			encoder,
			object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
			parentNodeComputer.Call,
		)
		levelBuilder := levelBuilderFactory.NewLevelBuilder()

		for i := 1000; i < 1010; i++ {
			patcher := model_core.NewReferenceMessagePatcher[string]()
			require.NoError(t, levelBuilder.PushChild(model_core.PatchedMessage[*model_filesystem_pb.FileContents, string]{
				Message: &model_filesystem_pb.FileContents{
					TotalSizeBytes: uint64(i),
					Reference: patcher.AddReference(
						object.MustNewSHA256V1LocalReference("5b2484693d5051be0fae63f4f862ce606cdc30ffbcd8a8a44b5b1b226b459262", uint32(i), 0, 0, 0),
						fmt.Sprintf("Child node %d", i),
					),
				},
				Patcher: patcher,
			}))
		}

		// The first call to PopParent() should construct an
		// object for the first three nodes.
		parentPatcher1 := model_core.NewReferenceMessagePatcher[string]()
		parentNode1 := model_core.PatchedMessage[*model_filesystem_pb.FileContents, string]{
			Message: &model_filesystem_pb.FileContents{
				TotalSizeBytes: 3003,
				Reference: parentPatcher1.AddReference(
					object.MustNewSHA256V1LocalReference("01969bfecedb8a71e173d229eea93c09923c634e1ea5b4b889d7c30cdb057994", 400, 1, 3, 0),
					"Parent node 1",
				),
			},
			Patcher: parentPatcher1,
		}
		encoder.EXPECT().EncodeBinary(gomock.Any()).
			DoAndReturn(func(in []byte) ([]byte, error) { return in, nil })
		parentNodeComputer.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			[]string{
				"Child node 1000",
				"Child node 1001",
				"Child node 1002",
			},
		).Return(parentNode1, nil)

		node, err := levelBuilder.PopParent(false)
		require.NoError(t, err)
		require.Equal(t, parentNode1, *node)

		// The second call to PopParent() should construct an
		// object for the next three nodes.
		parentPatcher2 := model_core.NewReferenceMessagePatcher[string]()
		parentNode2 := model_core.PatchedMessage[*model_filesystem_pb.FileContents, string]{
			Message: &model_filesystem_pb.FileContents{
				TotalSizeBytes: 3012,
				Reference: parentPatcher2.AddReference(
					object.MustNewSHA256V1LocalReference("ea77ffec4249175e05f445d4c44258162bbcf94a17291121c63fd694e46581ec", 400, 1, 3, 0),
					"Parent node 2",
				),
			},
			Patcher: parentPatcher2,
		}
		encoder.EXPECT().EncodeBinary(gomock.Any()).
			DoAndReturn(func(in []byte) ([]byte, error) { return in, nil })
		parentNodeComputer.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			[]string{
				"Child node 1003",
				"Child node 1004",
				"Child node 1005",
			},
		).Return(parentNode2, nil)

		node, err = levelBuilder.PopParent(false)
		require.NoError(t, err)
		require.Equal(t, parentNode2, *node)

		// The third call to PopParent() should not return
		// anything, as we have 4 nodes remaining. As
		// minimumCount is set to 3, we either have to return
		// all 4 nodes, or at least 2 nodes to be pushed.
		node, err = levelBuilder.PopParent(false)
		require.NoError(t, err)
		require.Nil(t, node)

		// If finalization is requested, the final 4 nodes
		// should be returned as part of a single message, as
		// there is no way to split them.
		parentPatcher3 := model_core.NewReferenceMessagePatcher[string]()
		parentNode3 := model_core.PatchedMessage[*model_filesystem_pb.FileContents, string]{
			Message: &model_filesystem_pb.FileContents{
				TotalSizeBytes: 4030,
				Reference: parentPatcher3.AddReference(
					object.MustNewSHA256V1LocalReference("d91f69312cc7453a3bb65804d493225c2006ec6eff12a47681277db29fbdbf62", 400, 1, 4, 0),
					"Parent node 3",
				),
			},
			Patcher: parentPatcher3,
		}
		encoder.EXPECT().EncodeBinary(gomock.Any()).
			DoAndReturn(func(in []byte) ([]byte, error) { return in, nil })
		parentNodeComputer.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			[]string{
				"Child node 1006",
				"Child node 1007",
				"Child node 1008",
				"Child node 1009",
			},
		).Return(parentNode3, nil)

		node, err = levelBuilder.PopParent(true)
		require.NoError(t, err)
		require.Equal(t, parentNode3, *node)

		// Once all nodes have been processed, PopParent()
		// should no longer do anything, even if finalization is
		// requested.
		node, err = levelBuilder.PopParent(true)
		require.NoError(t, err)
		require.Nil(t, node)
	})
}
