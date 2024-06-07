package leasemarshaling_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/tag/leasemarshaling"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.uber.org/mock/gomock"
)

func TestLeaseMarshalingUpdater(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseUpdater := NewMockUpdaterForTesting(ctrl)
	marshaler := NewMockLeaseMarshalerForTesting(ctrl)
	updater := leasemarshaling.NewLeaseMarshalingUpdater(baseUpdater, marshaler)

	t.Run("EmptyLease", func(t *testing.T) {
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		baseUpdater.EXPECT().UpdateTag(
			ctx,
			tag,
			object.MustNewSHA256V1GlobalReference("hello/world", "d257cd70c5b9987aed8f48cafcda62e8b8f3883a258f86cd6c85b731f675f9a2", 595814, 58, 12, 7883322),
			nil,
			/* overwrite = */ false,
		)

		require.NoError(t, updater.UpdateTag(
			ctx,
			tag,
			object.MustNewSHA256V1GlobalReference("hello/world", "d257cd70c5b9987aed8f48cafcda62e8b8f3883a258f86cd6c85b731f675f9a2", 595814, 58, 12, 7883322),
			nil,
			/* overwrite = */ false,
		))
	})

	t.Run("InvalidLease", func(t *testing.T) {
		marshaler.EXPECT().UnmarshalLease([]byte{1, 2, 3}).Return(nil, status.Error(codes.InvalidArgument, "Incorrect length"))

		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Invalid lease: Incorrect length"),
			updater.UpdateTag(
				ctx,
				tag,
				object.MustNewSHA256V1GlobalReference("hello/world", "4c237d99a0dcc601ef5e27f5e0616309631ad6651e4ad2bec3c7aa1466572e90", 595814, 58, 12, 7883322),
				[]byte{1, 2, 3},
				/* overwrite = */ true,
			),
		)
	})

	t.Run("ValidLease", func(t *testing.T) {
		marshaler.EXPECT().UnmarshalLease([]byte{4, 5, 6}).Return(42, nil)
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		baseUpdater.EXPECT().UpdateTag(
			ctx,
			tag,
			object.MustNewSHA256V1GlobalReference("hello/world", "b5ef7edee8fa4ca2de6f3517f2234a591050ea1d5f01eb099752a31b874a4f5b", 595814, 58, 12, 7883322),
			42,
			/* overwrite = */ true,
		)

		require.NoError(t, updater.UpdateTag(
			ctx,
			tag,
			object.MustNewSHA256V1GlobalReference("hello/world", "b5ef7edee8fa4ca2de6f3517f2234a591050ea1d5f01eb099752a31b874a4f5b", 595814, 58, 12, 7883322),
			[]byte{4, 5, 6},
			/* overwrite = */ true,
		))
	})
}
