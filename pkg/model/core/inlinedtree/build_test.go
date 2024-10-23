package inlinedtree_test

import (
	"testing"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/inlinedtree"
	model_core_pb "github.com/buildbarn/bb-playground/pkg/proto/model/core"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	object_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/proto"

	"go.uber.org/mock/gomock"
)

func TestBuild(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("NoCandidates", func(t *testing.T) {
		// If no candidates are provided, there is no data,
		// meaning an empty message needs to be emitted.
		encoder := NewMockBinaryEncoder(ctrl)

		output, err := inlinedtree.Build(
			inlinedtree.CandidateList[*model_filesystem_pb.Directory, model_core.ReferenceMetadata]{},
			&inlinedtree.Options{
				ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
				Encoder:          encoder,
				MaximumSizeBytes: 16 * 1024,
			},
		)
		require.NoError(t, err)

		references, metadata := output.Patcher.SortAndSetReferences()
		testutil.RequireEqualProto(t, &model_filesystem_pb.Directory{}, output.Message)
		require.Empty(t, references)
		require.Empty(t, metadata)
	})

	t.Run("SingleCandidateInlineTiny", func(t *testing.T) {
		// If the candidate is so small that it takes less space
		// to encode than an actual reference, it must be
		// inlined, even if the maximum output size does not
		// permit it.
		encoder := NewMockBinaryEncoder(ctrl)
		leaves := &model_filesystem_pb.Leaves{
			Symlinks: []*model_filesystem_pb.SymlinkNode{{
				Name:   "a",
				Target: "b",
			}},
		}
		leavesInline := &model_filesystem_pb.Directory_LeavesInline{
			LeavesInline: leaves,
		}
		parentAppender := NewMockParentAppenderForTesting(ctrl)
		metadata1 := NewMockReferenceMetadata(ctrl)
		parentAppender.EXPECT().Call(gomock.Any(), gomock.Nil(), gomock.Len(0)).
			Do(func(output model_core.PatchedMessage[*model_filesystem_pb.Directory, model_core.ReferenceMetadata], externalContents *object.Contents, externalMetadata []model_core.ReferenceMetadata) {
				output.Message.Leaves = leavesInline
			}).
			Times(2)
		parentAppender.EXPECT().Call(gomock.Any(), gomock.Not(gomock.Nil()), gomock.Len(0)).
			Do(func(output model_core.PatchedMessage[*model_filesystem_pb.Directory, model_core.ReferenceMetadata], externalContents *object.Contents, externalMetadata []model_core.ReferenceMetadata) {
				output.Message.Leaves = &model_filesystem_pb.Directory_LeavesExternal{
					LeavesExternal: output.Patcher.AddReference(externalContents.GetReference(), metadata1),
				}
			}).
			Times(1)
		metadata1.EXPECT().Discard()

		output, err := inlinedtree.Build(
			inlinedtree.CandidateList[*model_filesystem_pb.Directory, model_core.ReferenceMetadata]{{
				ExternalMessage: model_core.PatchedMessage[proto.Message, model_core.ReferenceMetadata]{
					Message: leaves,
					Patcher: model_core.NewReferenceMessagePatcher[model_core.ReferenceMetadata](),
				},
				ParentAppender: parentAppender.Call,
			}},
			&inlinedtree.Options{
				ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
				Encoder:          encoder,
				MaximumSizeBytes: 0,
			},
		)
		require.NoError(t, err)

		references, metadata := output.Patcher.SortAndSetReferences()
		testutil.RequireEqualProto(t, &model_filesystem_pb.Directory{
			Leaves: leavesInline,
		}, output.Message)
		require.Empty(t, references)
		require.Empty(t, metadata)
	})

	t.Run("SingleCandidateExternal", func(t *testing.T) {
		// If there is no space left and storing a reference
		// takes up less space than inlining the data, we should
		// emit a reference.
		encoder := NewMockBinaryEncoder(ctrl)
		encoder.EXPECT().EncodeBinary(gomock.Any()).
			DoAndReturn(func(in []byte) ([]byte, error) {
				return in, nil
			})
		leaves := &model_filesystem_pb.Leaves{
			Symlinks: []*model_filesystem_pb.SymlinkNode{{
				Name:   "This is a very long symbolic link name",
				Target: "This is a very long symbolic link target",
			}},
		}
		parentAppender := NewMockParentAppenderForTesting(ctrl)
		parentAppender.EXPECT().Call(gomock.Any(), gomock.Nil(), gomock.Len(0)).
			Do(func(output model_core.PatchedMessage[*model_filesystem_pb.Directory, model_core.ReferenceMetadata], externalContents *object.Contents, externalMetadata []model_core.ReferenceMetadata) {
				output.Message.Leaves = &model_filesystem_pb.Directory_LeavesInline{
					LeavesInline: leaves,
				}
			}).
			Times(1)
		metadata1 := NewMockReferenceMetadata(ctrl)
		parentAppender.EXPECT().Call(gomock.Any(), gomock.Not(gomock.Nil()), gomock.Len(0)).
			Do(func(output model_core.PatchedMessage[*model_filesystem_pb.Directory, model_core.ReferenceMetadata], externalContents *object.Contents, externalMetadata []model_core.ReferenceMetadata) {
				output.Message.Leaves = &model_filesystem_pb.Directory_LeavesExternal{
					LeavesExternal: output.Patcher.AddReference(externalContents.GetReference(), metadata1),
				}
			}).
			Times(2)
		metadata1.EXPECT().Discard().Times(1)

		output, err := inlinedtree.Build(
			inlinedtree.CandidateList[*model_filesystem_pb.Directory, model_core.ReferenceMetadata]{{
				ExternalMessage: model_core.PatchedMessage[proto.Message, model_core.ReferenceMetadata]{
					Message: leaves,
					Patcher: model_core.NewReferenceMessagePatcher[model_core.ReferenceMetadata](),
				},
				ParentAppender: parentAppender.Call,
			}},
			&inlinedtree.Options{
				ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
				Encoder:          encoder,
				MaximumSizeBytes: 0,
			},
		)
		require.NoError(t, err)

		references, metadata := output.Patcher.SortAndSetReferences()
		testutil.RequireEqualProto(t, &model_filesystem_pb.Directory{
			Leaves: &model_filesystem_pb.Directory_LeavesExternal{
				LeavesExternal: &model_core_pb.Reference{
					Index: 1,
				},
			},
		}, output.Message)
		require.Equal(t, object.OutgoingReferencesList{
			object.MustNewSHA256V1LocalReference("013ab9b8d7bfdce48a964249f169d6b99bb58ec55b11a7df0f7305ae8a5577df", 84, 0, 0, 0),
		}, references)
		require.Equal(t, []model_core.ReferenceMetadata{
			metadata1,
		}, metadata)
	})

	t.Run("SingleCandidateInline", func(t *testing.T) {
		// If the maximum message size if sufficiently large to
		// be able to inline the candidate, it should not store
		// the data externally.
		encoder := NewMockBinaryEncoder(ctrl)
		leaves := &model_filesystem_pb.Leaves{
			Symlinks: []*model_filesystem_pb.SymlinkNode{{
				Name:   "This is a very long symbolic link name",
				Target: "This is a very long symbolic link target",
			}},
		}
		leavesInline := &model_filesystem_pb.Directory_LeavesInline{
			LeavesInline: leaves,
		}
		parentAppender := NewMockParentAppenderForTesting(ctrl)
		parentAppender.EXPECT().Call(gomock.Any(), gomock.Nil(), gomock.Len(0)).
			Do(func(output model_core.PatchedMessage[*model_filesystem_pb.Directory, model_core.ReferenceMetadata], externalContents *object.Contents, externalMetadata []model_core.ReferenceMetadata) {
				output.Message.Leaves = leavesInline
			}).
			Times(2)
		metadata1 := NewMockReferenceMetadata(ctrl)
		parentAppender.EXPECT().Call(gomock.Any(), gomock.Not(gomock.Nil()), gomock.Len(0)).
			Do(func(output model_core.PatchedMessage[*model_filesystem_pb.Directory, model_core.ReferenceMetadata], externalContents *object.Contents, externalMetadata []model_core.ReferenceMetadata) {
				output.Message.Leaves = &model_filesystem_pb.Directory_LeavesExternal{
					LeavesExternal: output.Patcher.AddReference(externalContents.GetReference(), metadata1),
				}
			}).
			Times(1)
		metadata1.EXPECT().Discard()

		output, err := inlinedtree.Build(
			inlinedtree.CandidateList[*model_filesystem_pb.Directory, model_core.ReferenceMetadata]{{
				ExternalMessage: model_core.PatchedMessage[proto.Message, model_core.ReferenceMetadata]{
					Message: leaves,
					Patcher: model_core.NewReferenceMessagePatcher[model_core.ReferenceMetadata](),
				},
				ParentAppender: parentAppender.Call,
			}},
			&inlinedtree.Options{
				ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
				Encoder:          encoder,
				MaximumSizeBytes: 100,
			},
		)
		require.NoError(t, err)

		references, metadata := output.Patcher.SortAndSetReferences()
		testutil.RequireEqualProto(t, &model_filesystem_pb.Directory{
			Leaves: leavesInline,
		}, output.Message)
		require.Empty(t, references)
		require.Empty(t, metadata)
	})
}
