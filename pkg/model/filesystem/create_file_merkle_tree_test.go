package filesystem_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/testutil"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	object_pb "github.com/buildbarn/bonanza/pkg/proto/storage/object"
	"github.com/buildbarn/bonanza/pkg/storage/object"
	"github.com/seehuhn/mt19937"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestCreateFileMerkleTree(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	fileCreationParameters, err := model_filesystem.NewFileCreationParametersFromProto(
		&model_filesystem_pb.FileCreationParameters{
			Access:                           &model_filesystem_pb.FileAccessParameters{},
			ChunkMinimumSizeBytes:            1 << 16,
			ChunkMaximumSizeBytes:            1 << 18,
			FileContentsListMinimumSizeBytes: 1 << 12,
			FileContentsListMaximumSizeBytes: 1 << 14,
		},
		object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
	)
	require.NoError(t, err)

	t.Run("EmptyFile", func(t *testing.T) {
		// Empty files should be represented by leaving the
		// resulting FileContents message unset. There shouldn't
		// be any objects that need to be written to storage.
		capturer := NewMockFileMerkleTreeCapturerForTesting(ctrl)

		rootFileContents, err := model_filesystem.CreateFileMerkleTree(
			ctx,
			fileCreationParameters,
			bytes.NewBuffer(nil),
			capturer,
		)
		require.NoError(t, err)
		require.False(t, rootFileContents.IsSet())
	})

	t.Run("Hello", func(t *testing.T) {
		// Small files should be represented as single objects.
		// There should be no FileContents list, as those are
		// only used to join multiple objects together.
		capturer := NewMockFileMerkleTreeCapturerForTesting(ctrl)
		metadata1 := NewMockReferenceMetadata(ctrl)
		capturer.EXPECT().CaptureChunk(gomock.Any()).
			DoAndReturn(func(contents *object.Contents) model_core.ReferenceMetadata {
				require.Equal(t, object.MustNewSHA256V1LocalReference("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5, 0, 0, 0), contents.GetReference())
				return metadata1
			})

		rootFileContents, err := model_filesystem.CreateFileMerkleTree(
			ctx,
			fileCreationParameters,
			bytes.NewBufferString("Hello"),
			capturer,
		)
		require.NoError(t, err)

		references, metadata := rootFileContents.Patcher.SortAndSetReferences()
		testutil.RequireEqualProto(t, &model_filesystem_pb.FileContents{
			Reference: &model_core_pb.Reference{
				Index: 1,
			},
			TotalSizeBytes: 5,
		}, rootFileContents.Message)
		require.Equal(t, object.OutgoingReferencesList{
			object.MustNewSHA256V1LocalReference("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5, 0, 0, 0),
		}, references)
		require.Equal(t, []model_core.ReferenceMetadata{metadata1}, metadata)
	})

	t.Run("MersenneTwister1GB", func(t *testing.T) {
		// Create a Merkle tree for a 1 GB file consisting of
		// the first 1 GB of data returned by a Mersenne Twister
		// with the seed set to zero. The resulting tree should
		// have a height of two.
		twister := mt19937.New()
		twister.Seed(0)
		rootFileContents, err := model_filesystem.CreateFileMerkleTree(
			ctx,
			fileCreationParameters,
			io.LimitReader(twister, 1<<30),
			model_filesystem.NoopFileMerkleTreeCapturer,
		)
		require.NoError(t, err)

		references, _ := rootFileContents.Patcher.SortAndSetReferences()
		testutil.RequireEqualProto(t, &model_filesystem_pb.FileContents{
			Reference: &model_core_pb.Reference{
				Index: 1,
			},
			TotalSizeBytes: 1 << 30,
		}, rootFileContents.Message)
		require.Equal(t, object.OutgoingReferencesList{
			object.MustNewSHA256V1LocalReference("55147d913a3232c6bff657d445446977a4bfaf02795e08f44b35baa8babf0083", 1643, 2, 31, 15288),
		}, references)
	})
}
