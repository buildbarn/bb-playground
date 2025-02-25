package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/testutil"
	object_pb "github.com/buildbarn/bonanza/pkg/proto/storage/object"
	tag_pb "github.com/buildbarn/bonanza/pkg/proto/storage/tag"
	"github.com/buildbarn/bonanza/pkg/storage/object"
	"github.com/buildbarn/bonanza/pkg/storage/tag/grpc"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.uber.org/mock/gomock"
)

func TestGRPCResolver(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := NewMockResolverClient(ctrl)
	updater := grpc.NewGRPCResolver(client)

	t.Run("BackendFailure", func(t *testing.T) {
		// Errors reported by the backend should be propagated.
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		client.EXPECT().ResolveTag(ctx, testutil.EqProto(t, &tag_pb.ResolveTagRequest{
			Namespace: &object_pb.Namespace{
				InstanceName:    "hello/world",
				ReferenceFormat: object_pb.ReferenceFormat_SHA256_V1,
			},
			Tag: tag,
		})).Return(nil, status.Error(codes.Unavailable, "Server offline"))

		_, _, err = updater.ResolveTag(
			ctx,
			object.MustNewNamespace(&object_pb.Namespace{
				InstanceName:    "hello/world",
				ReferenceFormat: object_pb.ReferenceFormat_SHA256_V1,
			}),
			tag,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server offline"), err)
	})

	t.Run("InvalidReference", func(t *testing.T) {
		// If the server returns a malformed reference, an
		// internal server error should be returned.
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		client.EXPECT().ResolveTag(ctx, testutil.EqProto(t, &tag_pb.ResolveTagRequest{
			Namespace: &object_pb.Namespace{
				InstanceName:    "hello/world",
				ReferenceFormat: object_pb.ReferenceFormat_SHA256_V1,
			},
			Tag: tag,
		})).Return(&tag_pb.ResolveTagResponse{
			Reference: []byte{1, 2, 3},
			Complete:  false,
		}, nil)

		_, _, err = updater.ResolveTag(
			ctx,
			object.MustNewNamespace(&object_pb.Namespace{
				InstanceName:    "hello/world",
				ReferenceFormat: object_pb.ReferenceFormat_SHA256_V1,
			}),
			tag,
		)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Server returned an invalid reference: Reference is 3 bytes in size, while SHA256_V1 references are 40 bytes in size"), err)
	})

	t.Run("Success", func(t *testing.T) {
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		client.EXPECT().ResolveTag(ctx, testutil.EqProto(t, &tag_pb.ResolveTagRequest{
			Namespace: &object_pb.Namespace{
				InstanceName:    "hello/world",
				ReferenceFormat: object_pb.ReferenceFormat_SHA256_V1,
			},
			Tag: tag,
		})).Return(&tag_pb.ResolveTagResponse{
			Reference: []byte{
				// SHA-256 hash.
				0x1e, 0xf1, 0x27, 0x20, 0x45, 0xcc, 0xe7, 0xd3,
				0x80, 0x71, 0xc2, 0x87, 0x74, 0x2e, 0xdf, 0x54,
				0xef, 0x0c, 0x01, 0x6e, 0x78, 0x35, 0x2d, 0x2b,
				0xc8, 0xba, 0x11, 0x69, 0x14, 0x14, 0xc1, 0x50,
				// Size in bytes.
				0xd7, 0x84, 0x01,
				// Height.
				0x07,
				// Degree.
				0x28, 0x00,
				// Maximum parents total size in bytes.
				0x77, 0x43,
			},
			Complete: true,
		}, nil)

		reference, complete, err := updater.ResolveTag(
			ctx,
			object.MustNewNamespace(&object_pb.Namespace{
				InstanceName:    "hello/world",
				ReferenceFormat: object_pb.ReferenceFormat_SHA256_V1,
			}),
			tag,
		)
		require.NoError(t, err)
		require.Equal(t, object.MustNewSHA256V1LocalReference("1ef1272045cce7d38071c287742edf54ef0c016e78352d2bc8ba11691414c150", 99543, 7, 40, 375665), reference)
		require.True(t, complete)
	})
}
