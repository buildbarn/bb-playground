package mirrored_test

import (
	"context"
	"testing"

	object_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/tag/mirrored"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.uber.org/mock/gomock"
)

func TestMirroredResolver(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	replicaA := NewMockResolverForTesting(ctrl)
	replicaB := NewMockResolverForTesting(ctrl)
	resolver := mirrored.NewMirroredResolver(replicaA, replicaB)

	namespace := object.MustNewNamespace(&object_pb.Namespace{
		InstanceName:    "hello/world",
		ReferenceFormat: object_pb.ReferenceFormat_SHA256_V1,
	})

	t.Run("FailureReplicaA", func(t *testing.T) {
		// If a failure occurs resolving the tag through replica
		// A, the error should be propagated.
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		var badReference object.LocalReference
		replicaA.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(badReference, false, status.Error(codes.PermissionDenied, "User is not permitted to resolve tags"))
		replicaB.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(
				object.MustNewSHA256V1LocalReference("2572ad3fb952a78dffe4988445912245fcb4acd998750f956a3a069911aa2da6", 595814, 58, 12, 7883322),
				/* complete = */ true,
				nil,
			)

		_, _, err = resolver.ResolveTag(ctx, namespace, tag)
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Replica A: User is not permitted to resolve tags"), err)
	})

	t.Run("FailureReplicaB", func(t *testing.T) {
		// Similarly, the same should happen if a failure occurs
		// through replica B.
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		var badReference object.LocalReference
		replicaA.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Do(func(ctx context.Context, namespace object.Namespace, tag *anypb.Any) {
				<-ctx.Done()
			}).
			Return(badReference, false, status.Error(codes.Canceled, "Request canceled"))
		replicaB.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(badReference, false, status.Error(codes.PermissionDenied, "User is not permitted to resolve tags"))

		_, _, err = resolver.ResolveTag(ctx, namespace, tag)
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Replica B: User is not permitted to resolve tags"), err)
	})

	t.Run("NotFoundBoth", func(t *testing.T) {
		// If both replicas return NOT_FOUND, then there is
		// nothing meaningful we can return.
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		var badReference object.LocalReference
		replicaA.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(badReference, false, status.Error(codes.NotFound, "Tag does not exist in replica A"))
		replicaB.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(badReference, false, status.Error(codes.NotFound, "Tag does not exist in replica B"))

		_, _, err = resolver.ResolveTag(ctx, namespace, tag)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Tag not found"), err)
	})

	t.Run("NotFoundA", func(t *testing.T) {
		// If only replica A gives us a reference, we may return
		// it, but we can't announce it as being complete. Lease
		// renewing should ensure the tag gets replicated to
		// replica B.
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		var badReference object.LocalReference
		replicaA.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(badReference, false, status.Error(codes.NotFound, "Tag does not exist in replica A"))
		replicaB.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(
				object.MustNewSHA256V1LocalReference("7077118ca64ca957196c5f42537af08c7bba85171912bed4e6d8e4de428bd917", 595814, 58, 12, 7883322),
				/* complete = */ true,
				nil,
			)

		reference, complete, err := resolver.ResolveTag(ctx, namespace, tag)
		require.NoError(t, err)
		require.Equal(t, object.MustNewSHA256V1LocalReference("7077118ca64ca957196c5f42537af08c7bba85171912bed4e6d8e4de428bd917", 595814, 58, 12, 7883322), reference)
		require.False(t, complete)
	})

	t.Run("NotFoundB", func(t *testing.T) {
		// The same holds if only replica B is able to give us a
		// reference.
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		var badReference object.LocalReference
		replicaA.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(
				object.MustNewSHA256V1LocalReference("a844d50dc523d1d1e77d5dc6fae4d367561e8eae8e756b11ad51f9ab064456dd", 595814, 58, 12, 7883322),
				/* complete = */ false,
				nil,
			)
		replicaB.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(badReference, false, status.Error(codes.NotFound, "Tag does not exist in replica A"))

		reference, complete, err := resolver.ResolveTag(ctx, namespace, tag)
		require.NoError(t, err)
		require.Equal(t, object.MustNewSHA256V1LocalReference("a844d50dc523d1d1e77d5dc6fae4d367561e8eae8e756b11ad51f9ab064456dd", 595814, 58, 12, 7883322), reference)
		require.False(t, complete)
	})

	t.Run("MismatchingReferences", func(t *testing.T) {
		// If both replicas return a different reference, we
		// suppress it. This should cause the client the
		// recompute the data and write a new reference to both
		// replicas.
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		replicaA.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(
				object.MustNewSHA256V1LocalReference("a844d50dc523d1d1e77d5dc6fae4d367561e8eae8e756b11ad51f9ab064456dd", 595814, 58, 12, 7883322),
				/* complete = */ false,
				nil,
			)
		replicaB.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(
				object.MustNewSHA256V1LocalReference("6408a2db979aaaf3c66814eadcd3e9115b4fce294287a777a43089db35fe8f34", 595814, 58, 12, 7883322),
				/* complete = */ false,
				nil,
			)

		_, _, err = resolver.ResolveTag(ctx, namespace, tag)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Replica A resolves tag to object with reference SHA256=a844d50dc523d1d1e77d5dc6fae4d367561e8eae8e756b11ad51f9ab064456dd:S=595814:H=58:D=12:M=7884800, while replica B resolves tag to object with reference SHA256=6408a2db979aaaf3c66814eadcd3e9115b4fce294287a777a43089db35fe8f34:S=595814:H=58:D=12:M=7884800"), err)
	})

	t.Run("SuccessIncomplete", func(t *testing.T) {
		// If both replicas return the same reference, but one
		// of them reports it as being incomplete, we should
		// also report it as such.
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		replicaA.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(
				object.MustNewSHA256V1LocalReference("b32f1ba26b8abbe0f28be19d80f9d30a872471b0105de44583bb82ee724f9f49", 595814, 58, 12, 7883322),
				/* complete = */ true,
				nil,
			)
		replicaB.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(
				object.MustNewSHA256V1LocalReference("b32f1ba26b8abbe0f28be19d80f9d30a872471b0105de44583bb82ee724f9f49", 595814, 58, 12, 7883322),
				/* complete = */ false,
				nil,
			)

		reference, complete, err := resolver.ResolveTag(ctx, namespace, tag)
		require.NoError(t, err)
		require.Equal(t, object.MustNewSHA256V1LocalReference("b32f1ba26b8abbe0f28be19d80f9d30a872471b0105de44583bb82ee724f9f49", 595814, 58, 12, 7883322), reference)
		require.False(t, complete)
	})

	t.Run("SuccessComplete", func(t *testing.T) {
		// If both replicas return the same reference and report
		// it as being complete, we may do the same.
		tag, err := anypb.New(&emptypb.Empty{})
		require.NoError(t, err)
		replicaA.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(
				object.MustNewSHA256V1LocalReference("5b6723cc264c3d4605f359fa4e1a041f7e0fcd98763eb2b0c49bb2181a8e67f7", 595814, 58, 12, 7883322),
				/* complete = */ true,
				nil,
			)
		replicaB.EXPECT().
			ResolveTag(gomock.Any(), namespace, tag).
			Return(
				object.MustNewSHA256V1LocalReference("5b6723cc264c3d4605f359fa4e1a041f7e0fcd98763eb2b0c49bb2181a8e67f7", 595814, 58, 12, 7883322),
				/* complete = */ true,
				nil,
			)

		reference, complete, err := resolver.ResolveTag(ctx, namespace, tag)
		require.NoError(t, err)
		require.Equal(t, object.MustNewSHA256V1LocalReference("5b6723cc264c3d4605f359fa4e1a041f7e0fcd98763eb2b0c49bb2181a8e67f7", 595814, 58, 12, 7883322), reference)
		require.True(t, complete)
	})
}
