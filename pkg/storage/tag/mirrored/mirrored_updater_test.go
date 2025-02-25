package mirrored_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bonanza/pkg/storage/object"
	object_mirrored "github.com/buildbarn/bonanza/pkg/storage/object/mirrored"
	tag_mirrored "github.com/buildbarn/bonanza/pkg/storage/tag/mirrored"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.uber.org/mock/gomock"
)

func TestMirroredUpdater(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	replicaA := NewMockUpdaterForTesting(ctrl)
	replicaB := NewMockUpdaterForTesting(ctrl)
	updater := tag_mirrored.NewMirroredUpdater(replicaA, replicaB)

	t.Run("FailureReplicaA", func(t *testing.T) {
		// If updating the tag only succeeds for one of the
		// replicas, the error message should be propagated.
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		replicaA.EXPECT().
			UpdateTag(
				gomock.Any(),
				tag,
				object.MustNewSHA256V1GlobalReference("hello/world", "8ed6814114c216e75bef25dcba0d6b6c9600f2d49f07e3ae970697effae188d1", 595814, 58, 12, 7883322),
				"Lease A",
				/* overwrite = */ true,
			).
			Return(status.Error(codes.PermissionDenied, "User is not permitted to update tags"))
		replicaB.EXPECT().
			UpdateTag(
				gomock.Any(),
				tag,
				object.MustNewSHA256V1GlobalReference("hello/world", "8ed6814114c216e75bef25dcba0d6b6c9600f2d49f07e3ae970697effae188d1", 595814, 58, 12, 7883322),
				"Lease B",
				/* overwrite = */ true,
			)

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.PermissionDenied, "Replica A: User is not permitted to update tags"),
			updater.UpdateTag(
				ctx,
				tag,
				object.MustNewSHA256V1GlobalReference("hello/world", "8ed6814114c216e75bef25dcba0d6b6c9600f2d49f07e3ae970697effae188d1", 595814, 58, 12, 7883322),
				object_mirrored.MirroredLease[any, any]{
					LeaseA: "Lease A",
					LeaseB: "Lease B",
				},
				/* overwrite = */ true,
			),
		)
	})

	t.Run("FailureReplicaB", func(t *testing.T) {
		// If both replicas return an error, the error returned
		// by the first replica that failed should be returned.
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		replicaA.EXPECT().
			UpdateTag(
				gomock.Any(),
				tag,
				object.MustNewSHA256V1GlobalReference("hello/world", "abcba052c8c920fd06461136e1ab188d02a2302fde2b25fb8dc4b638203a6b9b", 595814, 58, 12, 7883322),
				"Lease A",
				/* overwrite = */ false,
			).
			Do(func(ctx context.Context, tag *anypb.Any, reference object.GlobalReference, lease any, overwrite bool) {
				<-ctx.Done()
			}).
			Return(status.Error(codes.Canceled, "Request canceled"))
		replicaB.EXPECT().
			UpdateTag(
				gomock.Any(),
				tag,
				object.MustNewSHA256V1GlobalReference("hello/world", "abcba052c8c920fd06461136e1ab188d02a2302fde2b25fb8dc4b638203a6b9b", 595814, 58, 12, 7883322),
				"Lease B",
				/* overwrite = */ false,
			).
			Return(status.Error(codes.Unavailable, "Server offline"))

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unavailable, "Replica B: Server offline"),
			updater.UpdateTag(
				ctx,
				tag,
				object.MustNewSHA256V1GlobalReference("hello/world", "abcba052c8c920fd06461136e1ab188d02a2302fde2b25fb8dc4b638203a6b9b", 595814, 58, 12, 7883322),
				object_mirrored.MirroredLease[any, any]{
					LeaseA: "Lease A",
					LeaseB: "Lease B",
				},
				/* overwrite = */ false,
			),
		)
	})

	t.Run("Success", func(t *testing.T) {
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		replicaA.EXPECT().
			UpdateTag(
				gomock.Any(),
				tag,
				object.MustNewSHA256V1GlobalReference("hello/world", "e1d6fa62850d20d505ef2ae9383588d61cc8ec400b9b4a3f9203c9df3ad25729", 595814, 58, 12, 7883322),
				"Lease A",
				/* overwrite = */ false,
			)
		replicaB.EXPECT().
			UpdateTag(
				gomock.Any(),
				tag,
				object.MustNewSHA256V1GlobalReference("hello/world", "e1d6fa62850d20d505ef2ae9383588d61cc8ec400b9b4a3f9203c9df3ad25729", 595814, 58, 12, 7883322),
				"Lease B",
				/* overwrite = */ false,
			)

		require.NoError(t, updater.UpdateTag(
			ctx,
			tag,
			object.MustNewSHA256V1GlobalReference("hello/world", "e1d6fa62850d20d505ef2ae9383588d61cc8ec400b9b4a3f9203c9df3ad25729", 595814, 58, 12, 7883322),
			object_mirrored.MirroredLease[any, any]{
				LeaseA: "Lease A",
				LeaseB: "Lease B",
			},
			/* overwrite = */ false,
		))
	})
}
