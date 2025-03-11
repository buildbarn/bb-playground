package starlark_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bonanza/pkg/label"
	"github.com/buildbarn/bonanza/pkg/model/core/inlinedtree"
	model_starlark "github.com/buildbarn/bonanza/pkg/model/starlark"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	object_pb "github.com/buildbarn/bonanza/pkg/proto/storage/object"
	"github.com/buildbarn/bonanza/pkg/storage/object"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestNewPackageGroupFromVisibility(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("private", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			packageGroup, err := model_starlark.NewPackageGroupFromVisibility(
				[]label.ResolvedLabel{
					label.MustNewResolvedLabel("@@foo+//visibility:private"),
				},
				&inlinedtree.Options{
					ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
					Encoder:          NewMockBinaryEncoder(ctrl),
					MaximumSizeBytes: 0,
				},
				NewMockCreatedObjectCapturerForTesting(ctrl),
			)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, &model_starlark_pb.PackageGroup{
				Tree: &model_starlark_pb.PackageGroup_Subpackages{},
			}, packageGroup.Message)
		})

		t.Run("Duplicate", func(t *testing.T) {
			_, err := model_starlark.NewPackageGroupFromVisibility(
				[]label.ResolvedLabel{
					label.MustNewResolvedLabel("@@foo+//visibility:private"),
					label.MustNewResolvedLabel("@@foo+//visibility:private"),
				},
				&inlinedtree.Options{
					ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
					Encoder:          NewMockBinaryEncoder(ctrl),
					MaximumSizeBytes: 0,
				},
				NewMockCreatedObjectCapturerForTesting(ctrl),
			)
			require.EqualError(t, err, "//visibility:private may not be combined with other labels")
		})
	})

	t.Run("public", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			packageGroup, err := model_starlark.NewPackageGroupFromVisibility(
				[]label.ResolvedLabel{
					label.MustNewResolvedLabel("@@foo+//visibility:public"),
				},
				&inlinedtree.Options{
					ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
					Encoder:          NewMockBinaryEncoder(ctrl),
					MaximumSizeBytes: 0,
				},
				NewMockCreatedObjectCapturerForTesting(ctrl),
			)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, &model_starlark_pb.PackageGroup{
				Tree: &model_starlark_pb.PackageGroup_Subpackages{
					IncludeSubpackages: true,
				},
			}, packageGroup.Message)
		})

		t.Run("Duplicate", func(t *testing.T) {
			_, err := model_starlark.NewPackageGroupFromVisibility(
				[]label.ResolvedLabel{
					label.MustNewResolvedLabel("@@foo+//visibility:public"),
					label.MustNewResolvedLabel("@@foo+//visibility:public"),
				},
				&inlinedtree.Options{
					ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
					Encoder:          NewMockBinaryEncoder(ctrl),
					MaximumSizeBytes: 0,
				},
				NewMockCreatedObjectCapturerForTesting(ctrl),
			)
			require.EqualError(t, err, "//visibility:public may not be combined with other labels")
		})
	})

	t.Run("Mix", func(t *testing.T) {
		objectCapturer := NewMockCreatedObjectCapturerForTesting(ctrl)
		objectCapturer.EXPECT().CaptureCreatedObject(gomock.Any()).AnyTimes()

		packageGroup, err := model_starlark.NewPackageGroupFromVisibility(
			[]label.ResolvedLabel{
				// Inclusion of the root package.
				label.MustNewResolvedLabel("@@toplevel1+//:__pkg__"),
				label.MustNewResolvedLabel("@@toplevel2+//:__subpackages__"),
				// If directories are nested, they should both
				// be represented in the resulting message.
				label.MustNewResolvedLabel("@@nested+//foo:__pkg__"),
				label.MustNewResolvedLabel("@@nested+//foo/bar:__pkg__"),
				// If a parent directory uses __subpackages__,
				// there is no need to preserve any children.
				label.MustNewResolvedLabel("@@collapse1+//foo:__subpackages__"),
				label.MustNewResolvedLabel("@@collapse1+//foo/bar:__pkg__"),
				label.MustNewResolvedLabel("@@collapse1+//foo/bar/baz:__pkg__"),
				// If the children are created before the
				// parent, the children should be removed from
				// the resulting tree.
				label.MustNewResolvedLabel("@@collapse2+//foo/bar/baz:__pkg__"),
				label.MustNewResolvedLabel("@@collapse2+//foo/bar:__pkg__"),
				label.MustNewResolvedLabel("@@collapse2+//foo:__subpackages__"),
				// If "foo" is not provided, it should still be
				// created, because "bar" should go inside it.
				label.MustNewResolvedLabel("@@skip+//foo/bar:__pkg__"),
				// As the order of package groups is irrelevant,
				// they should be deduplicated and sorted.
				label.MustNewResolvedLabel("@@packagegroups+//:group1"),
				label.MustNewResolvedLabel("@@packagegroups+//:group3"),
				label.MustNewResolvedLabel("@@packagegroups+//:group2"),
				label.MustNewResolvedLabel("@@packagegroups+//:group4"),
				label.MustNewResolvedLabel("@@packagegroups+//:group4"),
			},
			&inlinedtree.Options{
				ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
				Encoder:          NewMockBinaryEncoder(ctrl),
				MaximumSizeBytes: 1 << 20,
			},
			objectCapturer,
		)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &model_starlark_pb.PackageGroup{
			Tree: &model_starlark_pb.PackageGroup_Subpackages{
				Overrides: &model_starlark_pb.PackageGroup_Subpackages_OverridesInline{
					OverridesInline: &model_starlark_pb.PackageGroup_Subpackages_Overrides{
						Packages: []*model_starlark_pb.PackageGroup_Package{
							{
								Component: "collapse1+",
								Subpackages: &model_starlark_pb.PackageGroup_Subpackages{
									Overrides: &model_starlark_pb.PackageGroup_Subpackages_OverridesInline{
										OverridesInline: &model_starlark_pb.PackageGroup_Subpackages_Overrides{
											Packages: []*model_starlark_pb.PackageGroup_Package{
												{
													Component:      "foo",
													IncludePackage: true,
													Subpackages: &model_starlark_pb.PackageGroup_Subpackages{
														IncludeSubpackages: true,
													},
												},
											},
										},
									},
								},
							},
							{
								Component: "collapse2+",
								Subpackages: &model_starlark_pb.PackageGroup_Subpackages{
									Overrides: &model_starlark_pb.PackageGroup_Subpackages_OverridesInline{
										OverridesInline: &model_starlark_pb.PackageGroup_Subpackages_Overrides{
											Packages: []*model_starlark_pb.PackageGroup_Package{
												{
													Component:      "foo",
													IncludePackage: true,
													Subpackages: &model_starlark_pb.PackageGroup_Subpackages{
														IncludeSubpackages: true,
													},
												},
											},
										},
									},
								},
							},
							{
								Component: "nested+",
								Subpackages: &model_starlark_pb.PackageGroup_Subpackages{
									Overrides: &model_starlark_pb.PackageGroup_Subpackages_OverridesInline{
										OverridesInline: &model_starlark_pb.PackageGroup_Subpackages_Overrides{
											Packages: []*model_starlark_pb.PackageGroup_Package{
												{
													Component:      "foo",
													IncludePackage: true,
													Subpackages: &model_starlark_pb.PackageGroup_Subpackages{
														Overrides: &model_starlark_pb.PackageGroup_Subpackages_OverridesInline{
															OverridesInline: &model_starlark_pb.PackageGroup_Subpackages_Overrides{
																Packages: []*model_starlark_pb.PackageGroup_Package{
																	{
																		Component:      "bar",
																		IncludePackage: true,
																		Subpackages:    &model_starlark_pb.PackageGroup_Subpackages{},
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
							{
								Component: "skip+",
								Subpackages: &model_starlark_pb.PackageGroup_Subpackages{
									Overrides: &model_starlark_pb.PackageGroup_Subpackages_OverridesInline{
										OverridesInline: &model_starlark_pb.PackageGroup_Subpackages_Overrides{
											Packages: []*model_starlark_pb.PackageGroup_Package{
												{
													Component: "foo",
													Subpackages: &model_starlark_pb.PackageGroup_Subpackages{
														Overrides: &model_starlark_pb.PackageGroup_Subpackages_OverridesInline{
															OverridesInline: &model_starlark_pb.PackageGroup_Subpackages_Overrides{
																Packages: []*model_starlark_pb.PackageGroup_Package{
																	{
																		Component:      "bar",
																		IncludePackage: true,
																		Subpackages:    &model_starlark_pb.PackageGroup_Subpackages{},
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
							{
								Component:      "toplevel1+",
								IncludePackage: true,
								Subpackages:    &model_starlark_pb.PackageGroup_Subpackages{},
							},
							{
								Component:      "toplevel2+",
								IncludePackage: true,
								Subpackages: &model_starlark_pb.PackageGroup_Subpackages{
									IncludeSubpackages: true,
								},
							},
						},
					},
				},
			},
			IncludePackageGroups: []string{
				"@@packagegroups+//:group1",
				"@@packagegroups+//:group2",
				"@@packagegroups+//:group3",
				"@@packagegroups+//:group4",
			},
		}, packageGroup.Message)
	})
}
