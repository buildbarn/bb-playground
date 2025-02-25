package filesystem_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/testutil"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/object"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.uber.org/mock/gomock"
)

func TestDirectoryClusterObjectParser(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	leavesParser := NewMockLeavesParsedObjectReaderForTesting(ctrl)
	objectParser := model_filesystem.NewDirectoryClusterObjectParser[object.LocalReference](leavesParser)

	t.Run("InvalidMessage", func(t *testing.T) {
		_, _, err := objectParser.ParseObject(
			ctx,
			object.MustNewSHA256V1LocalReference("4c817b0522489e65d00d339d41b4e2b2ec2081be15d1775195be822dd9a9c7f0", 28, 0, 0, 0),
			object.OutgoingReferencesList{},
			[]byte("Not a valid Protobuf message"),
		)
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to parse directory: "), err)
	})

	t.Run("DirectoryWithLeaves", func(t *testing.T) {
		leaves := &model_filesystem_pb.Leaves{
			Files: []*model_filesystem_pb.FileNode{{
				Name: "hello.txt",
				Properties: &model_filesystem_pb.FileProperties{
					Contents: &model_filesystem_pb.FileContents{
						Reference: &model_core_pb.Reference{
							Index: 1,
						},
						TotalSizeBytes: 42,
					},
				},
			}},
			Symlinks: []*model_filesystem_pb.SymlinkNode{{
				Name:   "world.txt",
				Target: "hello.txt",
			}},
		}
		data, err := proto.Marshal(&model_filesystem_pb.Directory{
			Leaves: &model_filesystem_pb.Directory_LeavesInline{
				LeavesInline: leaves,
			},
		})
		require.NoError(t, err)
		cluster, sizeBytes, err := objectParser.ParseObject(
			ctx,
			object.MustNewSHA256V1LocalReference("30f668110e6f5fb8b19705f8f1c124ac338a0b2e7b8d7ff98160619db1b737c4", 100, 1, 1, 0),
			object.OutgoingReferencesList{
				object.MustNewSHA256V1LocalReference("b6902c0677c346fd8fff1580fc7b8f01989729195dc3cce74c5b7984008ec8f5", 42, 0, 0, 0),
			},
			data,
		)
		require.NoError(t, err)
		require.Len(t, cluster, 1)
		require.Empty(t, cluster[0].Directories)
		testutil.RequireEqualProto(t, leaves, cluster[0].Leaves.Message)
		require.Equal(t, object.OutgoingReferencesList{
			object.MustNewSHA256V1LocalReference("b6902c0677c346fd8fff1580fc7b8f01989729195dc3cce74c5b7984008ec8f5", 42, 0, 0, 0),
		}, cluster[0].Leaves.OutgoingReferences)
		require.Equal(t, 100, sizeBytes)
	})

	// TODO: Add more testing coverage.
}
