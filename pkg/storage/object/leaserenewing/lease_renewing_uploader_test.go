package leaserenewing_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/testutil"
	object_pb "github.com/buildbarn/bonanza/pkg/proto/storage/object"
	"github.com/buildbarn/bonanza/pkg/storage/object"
	"github.com/buildbarn/bonanza/pkg/storage/object/leaserenewing"
	"github.com/stretchr/testify/require"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestLeaseRenewingUploader(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseUploader := NewMockUploaderForTesting(ctrl)
	uploader := leaserenewing.NewLeaseRenewingUploader(
		baseUploader,
		semaphore.NewWeighted(1),
		object.NewLimit(&object_pb.Limit{
			Count:     100,
			SizeBytes: 10000000,
		}),
	)

	// Execute these tests repeatedly, so that we can validate that
	// all resources are released properly.
	for i := 0; i <= 10; i++ {
		t.Run("InterruptProcessSingleObject", func(t *testing.T) {
			// It should be possible to interrupt
			// ProcessSingleObject() by providing a context
			// object.
			canceledCtx, cancel := context.WithCancel(ctx)
			cancel()
			uploader.ProcessSingleObject(canceledCtx)
		})

		t.Run("SkipLeafObject", func(t *testing.T) {
			// As leaf objects don't have any outgoing
			// references, this backend should pass through
			// requests in literal form.
			baseUploader.EXPECT().UploadObject(
				ctx,
				object.MustNewSHA256V1GlobalReference("hello/world", "5c2bc09048abb40e95eec67d68c3bbf53a9fcf01e581a1c77fa9068e998ef851", 488549, 0, 0, 0),
				/* contents = */ nil,
				/* childrenLeases = */ nil,
				/* wantContentsIfIncomplete = */ false,
			).Return(object.UploadObjectComplete[any]{
				Lease: "Lease",
			}, nil)

			result, err := uploader.UploadObject(
				ctx,
				object.MustNewSHA256V1GlobalReference("hello/world", "5c2bc09048abb40e95eec67d68c3bbf53a9fcf01e581a1c77fa9068e998ef851", 488549, 0, 0, 0),
				/* contents = */ nil,
				/* childrenLeases = */ nil,
				/* wantContentsIfIncomplete = */ false,
			)
			require.NoError(t, err)
			require.Equal(t, object.UploadObjectComplete[any]{
				Lease: "Lease",
			}, result)
		})

		t.Run("SkipWithChildrenLeases", func(t *testing.T) {
			// This backend should only intercept simple calls that
			// attempt to obtain a lease on an existing object. If
			// contents or leases or children are provided, the
			// request should be passed through in literal form.
			baseUploader.EXPECT().UploadObject(
				ctx,
				object.MustNewSHA256V1GlobalReference("hello/world", "132a1389ca59f99461dba1f307cd7b6bb2e39adbf1dfb3ae02eea2fdf87a346d", 95938, 13, 1, 958050),
				/* contents = */ nil,
				[]any{"Lease 1"},
				/* wantContentsIfIncomplete = */ false,
			).Return(object.UploadObjectComplete[any]{
				Lease: "Lease 2",
			}, nil)

			result, err := uploader.UploadObject(
				ctx,
				object.MustNewSHA256V1GlobalReference("hello/world", "132a1389ca59f99461dba1f307cd7b6bb2e39adbf1dfb3ae02eea2fdf87a346d", 95938, 13, 1, 958050),
				/* contents = */ nil,
				[]any{"Lease 1"},
				/* wantContentsIfIncomplete = */ false,
			)
			require.NoError(t, err)
			require.Equal(t, object.UploadObjectComplete[any]{
				Lease: "Lease 2",
			}, result)
		})

		t.Run("SimpleComplete", func(t *testing.T) {
			// Simple case where the object exists within
			// storage and has up-to-date leases on its
			// outgoing references. The results of the
			// operation should be returned directly.
			baseUploader.EXPECT().UploadObject(
				gomock.Any(),
				object.MustNewSHA256V1GlobalReference("hello/world", "132a1389ca59f99461dba1f307cd7b6bb2e39adbf1dfb3ae02eea2fdf87a346d", 95938, 13, 1, 958050),
				/* contents = */ nil,
				/* childrenLeases = */ nil,
				/* wantContentsIfIncomplete = */ true,
			).Return(object.UploadObjectComplete[any]{
				Lease: "Lease",
			}, nil)

			go uploader.ProcessSingleObject(ctx)
			result, err := uploader.UploadObject(
				ctx,
				object.MustNewSHA256V1GlobalReference("hello/world", "132a1389ca59f99461dba1f307cd7b6bb2e39adbf1dfb3ae02eea2fdf87a346d", 95938, 13, 1, 958050),
				/* contents = */ nil,
				/* childrenLeases = */ nil,
				/* wantContentsIfIncomplete = */ false,
			)
			require.NoError(t, err)
			require.Equal(t, object.UploadObjectComplete[any]{
				Lease: "Lease",
			}, result)
		})

		t.Run("SimpleMissing", func(t *testing.T) {
			// Simple case where the object does not exist. There
			// is little that can be done to renew its leases. The
			// results should be returned directly.
			baseUploader.EXPECT().UploadObject(
				gomock.Any(),
				object.MustNewSHA256V1GlobalReference("hello/world", "3b1bbcb2f4b0cfcb8843cc96b9e6405e630faed02552e5962e1b57947e193b1d", 95938, 13, 1, 958050),
				/* contents = */ nil,
				/* childrenLeases = */ nil,
				/* wantContentsIfIncomplete = */ true,
			).Return(object.UploadObjectMissing[any]{}, nil)

			go uploader.ProcessSingleObject(ctx)
			result, err := uploader.UploadObject(
				ctx,
				object.MustNewSHA256V1GlobalReference("hello/world", "3b1bbcb2f4b0cfcb8843cc96b9e6405e630faed02552e5962e1b57947e193b1d", 95938, 13, 1, 958050),
				/* contents = */ nil,
				/* childrenLeases = */ nil,
				/* wantContentsIfIncomplete = */ false,
			)
			require.NoError(t, err)
			require.Equal(t, object.UploadObjectMissing[any]{}, result)
		})

		t.Run("IncompleteToComplete", func(t *testing.T) {
			// If an object is reported as being incomplete,
			// we should issue further UploadObject() calls
			// to obtain the leases of the children. A final
			// call to UploadObject() should be issued to
			// make the root object complete again.
			contents := object.MustNewContents(
				object_pb.ReferenceFormat_SHA256_V1,
				object.OutgoingReferencesList{
					object.MustNewSHA256V1LocalReference("348a1c1a72e1a3dece7fabff7ac7c78faa06c8994b1573c929fad20da209890d", 95938, 13, 1, 958050),
					object.MustNewSHA256V1LocalReference("4179d172c91a7487c2260c88b28b9892fc1305a904bac4e27af3b43464a08812", 774, 8, 2, 69493),
					object.MustNewSHA256V1LocalReference("48b0846063ab6897a9cd156f11c7de1c07dabfeb02e0b60f1dca1f40e6d3ca2a", 5784, 10, 10, 85439),
				},
				[]byte("Hello"),
			)
			gomock.InOrder(
				baseUploader.EXPECT().UploadObject(
					gomock.Any(),
					object.GlobalReference{
						InstanceName:   object.NewInstanceName("hello/world"),
						LocalReference: contents.GetReference(),
					},
					/* contents = */ nil,
					/* childrenLeases = */ nil,
					/* wantContentsIfIncomplete = */ true,
				).Return(object.UploadObjectIncomplete[any]{
					Contents:                     contents,
					WantOutgoingReferencesLeases: []int{1, 2},
				}, nil),
				baseUploader.EXPECT().UploadObject(
					gomock.Any(),
					object.MustNewSHA256V1GlobalReference("hello/world", "4179d172c91a7487c2260c88b28b9892fc1305a904bac4e27af3b43464a08812", 774, 8, 2, 69493),
					/* contents = */ nil,
					/* childrenLeases = */ nil,
					/* wantContentsIfIncomplete = */ true,
				).Return(object.UploadObjectMissing[any]{}, nil),
				baseUploader.EXPECT().UploadObject(
					gomock.Any(),
					object.MustNewSHA256V1GlobalReference("hello/world", "48b0846063ab6897a9cd156f11c7de1c07dabfeb02e0b60f1dca1f40e6d3ca2a", 5784, 10, 10, 85439),
					/* contents = */ nil,
					/* childrenLeases = */ nil,
					/* wantContentsIfIncomplete = */ true,
				).Return(object.UploadObjectComplete[any]{
					Lease: "Lease 1",
				}, nil),
				baseUploader.EXPECT().UploadObject(
					gomock.Any(),
					object.GlobalReference{
						InstanceName:   object.NewInstanceName("hello/world"),
						LocalReference: contents.GetReference(),
					},
					/* contents = */ nil,
					[]any{
						nil,
						nil,
						"Lease 1",
					},
					/* wantContentsIfIncomplete = */ false,
				).Return(object.UploadObjectComplete[any]{
					Lease: "Lease 2",
				}, nil),
			)

			go func() {
				for i := 0; i < 3; i++ {
					require.True(t, uploader.ProcessSingleObject(ctx))
				}
			}()
			result, err := uploader.UploadObject(
				ctx,
				object.GlobalReference{
					InstanceName:   object.NewInstanceName("hello/world"),
					LocalReference: contents.GetReference(),
				},
				/* contents = */ nil,
				/* childrenLeases = */ nil,
				/* wantContentsIfIncomplete = */ false,
			)
			require.NoError(t, err)
			require.Equal(t, object.UploadObjectComplete[any]{
				Lease: "Lease 2",
			}, result)
		})

		t.Run("InitialUploadObjectFailure", func(t *testing.T) {
			// As a single call to UploadObject() can
			// traverse a full hierarchy of objects, any
			// error messages that are propagated should
			// include the name of the object.
			baseUploader.EXPECT().UploadObject(
				gomock.Any(),
				object.MustNewSHA256V1GlobalReference("hello/world", "52ed02acd303e5b50e5b0d207113b7381c097a5bc4772ee2ac870b58d5339247", 95938, 13, 1, 958050),
				/* contents = */ nil,
				/* childrenLeases = */ nil,
				/* wantContentsIfIncomplete = */ true,
			).Return(nil, status.Error(codes.Unavailable, "Server offline"))

			go uploader.ProcessSingleObject(ctx)
			_, err := uploader.UploadObject(
				ctx,
				object.MustNewSHA256V1GlobalReference("hello/world", "52ed02acd303e5b50e5b0d207113b7381c097a5bc4772ee2ac870b58d5339247", 95938, 13, 1, 958050),
				/* contents = */ nil,
				/* childrenLeases = */ nil,
				/* wantContentsIfIncomplete = */ false,
			)
			testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Failed to obtain lease for object with reference SHA256=52ed02acd303e5b50e5b0d207113b7381c097a5bc4772ee2ac870b58d5339247:S=95938:H=13:D=1:M=958208: Server offline"), err)
		})

		t.Run("FinalUploadObjectFailure", func(t *testing.T) {
			contents := object.MustNewContents(
				object_pb.ReferenceFormat_SHA256_V1,
				object.OutgoingReferencesList{
					object.MustNewSHA256V1LocalReference("7ce38508b7b64576dc1719bed9cd450e5f521cf16913ba5ef57c353bc115d946", 59495, 13, 1, 305968),
				},
				[]byte("Hello"),
			)
			gomock.InOrder(
				baseUploader.EXPECT().UploadObject(
					gomock.Any(),
					object.GlobalReference{
						InstanceName:   object.NewInstanceName("hello/world"),
						LocalReference: contents.GetReference(),
					},
					/* contents = */ nil,
					/* childrenLeases = */ nil,
					/* wantContentsIfIncomplete = */ true,
				).Return(object.UploadObjectIncomplete[any]{
					Contents:                     contents,
					WantOutgoingReferencesLeases: []int{0},
				}, nil),
				baseUploader.EXPECT().UploadObject(
					gomock.Any(),
					object.MustNewSHA256V1GlobalReference("hello/world", "7ce38508b7b64576dc1719bed9cd450e5f521cf16913ba5ef57c353bc115d946", 59495, 13, 1, 305968),
					/* contents = */ nil,
					/* childrenLeases = */ nil,
					/* wantContentsIfIncomplete = */ true,
				).Return(object.UploadObjectComplete[any]{
					Lease: "Lease",
				}, nil),
				baseUploader.EXPECT().UploadObject(
					gomock.Any(),
					object.GlobalReference{
						InstanceName:   object.NewInstanceName("hello/world"),
						LocalReference: contents.GetReference(),
					},
					/* contents = */ nil,
					[]any{"Lease"},
					/* wantContentsIfIncomplete = */ false,
				).Return(nil, status.Error(codes.Unavailable, "Server offline")),
			)

			go func() {
				for i := 0; i < 2; i++ {
					require.True(t, uploader.ProcessSingleObject(ctx))
				}
			}()
			_, err := uploader.UploadObject(
				ctx,
				object.GlobalReference{
					InstanceName:   object.NewInstanceName("hello/world"),
					LocalReference: contents.GetReference(),
				},
				/* contents = */ nil,
				/* childrenLeases = */ nil,
				/* wantContentsIfIncomplete = */ false,
			)
			testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Failed to update leases for object with reference SHA256=953b996fde970873f7c839d13accd3a7cee159a0c4a41b3fcd32070de1ff954f:S=45:H=14:D=1:M=365568: Server offline"), err)
		})
	}
}
