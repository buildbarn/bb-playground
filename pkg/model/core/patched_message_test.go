package core_test

import (
	"testing"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_core_pb "github.com/buildbarn/bb-playground/pkg/proto/model/core"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewPatchedMessageFromExisting(t *testing.T) {
	t.Run("Example", func(t *testing.T) {
		m1, err := model_core.NewPatchedMessageFromExisting(
			model_core.Message[*model_filesystem_pb.FileNode]{
				Message: &model_filesystem_pb.FileNode{
					Name: "a",
					Properties: &model_filesystem_pb.FileProperties{
						Contents: &model_filesystem_pb.FileContents{
							Reference: &model_core_pb.Reference{
								Index: 2,
							},
							TotalSizeBytes: 23,
						},
					},
				},
				OutgoingReferences: object.OutgoingReferencesList{
					object.MustNewSHA256V1LocalReference("31233528b0ccc08d56724b2f132154967a89c4fb79de65fc65e3eeb42d9f89e4", 4828, 0, 0, 0),
					object.MustNewSHA256V1LocalReference("46d71098267fa33992257c061ba8fc48017e2bcac8f9ac3be8853c8337ec896e", 58511, 0, 0, 0),
					object.MustNewSHA256V1LocalReference("e1d1549332e44eddf28662dda4ca1aae36c3dcd597cd63b3c69737f88afd75d5", 213, 0, 0, 0),
				},
			},
			func(reference object.LocalReference) int {
				require.Equal(t, object.MustNewSHA256V1LocalReference("46d71098267fa33992257c061ba8fc48017e2bcac8f9ac3be8853c8337ec896e", 58511, 0, 0, 0), reference)
				return 123
			},
		)
		require.NoError(t, err)

		references, metadata := m1.Patcher.SortAndSetReferences()
		require.Equal(t, object.OutgoingReferencesList{object.MustNewSHA256V1LocalReference("46d71098267fa33992257c061ba8fc48017e2bcac8f9ac3be8853c8337ec896e", 58511, 0, 0, 0)}, references)
		require.Equal(t, []int{123}, metadata)

		testutil.RequireEqualProto(t, &model_filesystem_pb.FileNode{
			Name: "a",
			Properties: &model_filesystem_pb.FileProperties{
				Contents: &model_filesystem_pb.FileContents{
					Reference: &model_core_pb.Reference{
						Index: 1,
					},
					TotalSizeBytes: 23,
				},
			},
		}, m1.Message)
	})
}
