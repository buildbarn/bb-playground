package grpc_test

import (
	"context"
	"testing"

	object_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	tag_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/tag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/tag/grpc"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.uber.org/mock/gomock"
)

func TestGRPCUpdater(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := NewMockUpdaterClient(ctrl)
	updater := grpc.NewGRPCUpdater(client)

	t.Run("BackendFailure", func(t *testing.T) {
		// Errors reported by the backend should be propagated.
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		client.EXPECT().UpdateTag(ctx, testutil.EqProto(t, &tag_pb.UpdateTagRequest{
			Namespace: &object_pb.Namespace{
				InstanceName:    "hello/world",
				ReferenceFormat: object_pb.ReferenceFormat_SHA256_V1,
			},
			Tag: tag,
			Reference: []byte{
				// SHA-256 hash.
				0x75, 0x1a, 0x11, 0x12, 0xa7, 0xb0, 0x3c, 0x95,
				0x42, 0x66, 0x71, 0xe6, 0x8d, 0xc7, 0x83, 0x38,
				0x24, 0x4b, 0xd2, 0xbd, 0x91, 0x1a, 0xbe, 0x8b,
				0xcd, 0x60, 0xf8, 0x63, 0xb1, 0xfb, 0xe2, 0xc0,
				// Size in bytes.
				0xd7, 0x84, 0x01,
				// Height.
				0x07,
				// Degree.
				0x28, 0x00,
				// Maximum parents total size in bytes.
				0x77, 0x43,
			},
			Lease:     []byte{1, 2, 3},
			Overwrite: true,
		})).Return(nil, status.Error(codes.Unavailable, "Server offline"))

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unavailable, "Server offline"),
			updater.UpdateTag(
				ctx,
				tag,
				object.MustNewSHA256V1GlobalReference("hello/world", "751a1112a7b03c95426671e68dc78338244bd2bd911abe8bcd60f863b1fbe2c0", 99543, 7, 40, 375665),
				[]byte{1, 2, 3},
				/* overwrite = */ true,
			),
		)
	})

	t.Run("Success", func(t *testing.T) {
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		client.EXPECT().UpdateTag(ctx, testutil.EqProto(t, &tag_pb.UpdateTagRequest{
			Namespace: &object_pb.Namespace{
				InstanceName:    "hello/world",
				ReferenceFormat: object_pb.ReferenceFormat_SHA256_V1,
			},
			Tag: tag,
			Reference: []byte{
				// SHA-256 hash.
				0x91, 0x59, 0x32, 0xae, 0x3e, 0x36, 0xa2, 0xf3,
				0x95, 0xf6, 0xb0, 0x33, 0xdb, 0x69, 0x4d, 0x1d,
				0x31, 0xc1, 0x28, 0xf9, 0x4d, 0x08, 0xf2, 0x02,
				0x2d, 0x7c, 0x4b, 0x4f, 0x33, 0x6b, 0xc5, 0x41,
				// Size in bytes.
				0xd7, 0x84, 0x01,
				// Height.
				0x07,
				// Degree.
				0x28, 0x00,
				// Maximum parents total size in bytes.
				0x77, 0x43,
			},
			Lease:     []byte{1, 2, 3},
			Overwrite: false,
		})).Return(&emptypb.Empty{}, nil)

		require.NoError(t, updater.UpdateTag(
			ctx,
			tag,
			object.MustNewSHA256V1GlobalReference("hello/world", "915932ae3e36a2f395f6b033db694d1d31c128f94d08f2022d7c4b4f336bc541", 99543, 7, 40, 375665),
			[]byte{1, 2, 3},
			/* overwrite = */ false,
		))
	})
}
