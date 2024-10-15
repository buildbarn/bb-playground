package starlark_test

import (
	"testing"

	"github.com/buildbarn/bb-playground/pkg/label"
	"github.com/buildbarn/bb-playground/pkg/model/core/inlinedtree"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	object_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestNewPackageGroupFromVisibility(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("private", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			packageGroup, err := model_starlark.NewPackageGroupFromVisibility(
				[]label.CanonicalLabel{
					label.MustNewCanonicalLabel("@@foo+//visibility:private"),
				},
				&inlinedtree.Options{
					ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
					Encoder:          NewMockBinaryEncoder(ctrl),
					MaximumSizeBytes: 0,
				},
			)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, &model_starlark_pb.PackageGroup{
				Tree: &model_starlark_pb.PackageGroup_Subpackages{},
			}, packageGroup.Message)
		})

		t.Run("Duplicate", func(t *testing.T) {
			_, err := model_starlark.NewPackageGroupFromVisibility(
				[]label.CanonicalLabel{
					label.MustNewCanonicalLabel("@@foo+//visibility:private"),
					label.MustNewCanonicalLabel("@@foo+//visibility:private"),
				},
				&inlinedtree.Options{
					ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
					Encoder:          NewMockBinaryEncoder(ctrl),
					MaximumSizeBytes: 0,
				},
			)
			require.EqualError(t, err, "//visibility:private may not be combined with other labels")
		})
	})

	t.Run("public", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			packageGroup, err := model_starlark.NewPackageGroupFromVisibility(
				[]label.CanonicalLabel{
					label.MustNewCanonicalLabel("@@foo+//visibility:public"),
				},
				&inlinedtree.Options{
					ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
					Encoder:          NewMockBinaryEncoder(ctrl),
					MaximumSizeBytes: 0,
				},
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
				[]label.CanonicalLabel{
					label.MustNewCanonicalLabel("@@foo+//visibility:public"),
					label.MustNewCanonicalLabel("@@foo+//visibility:public"),
				},
				&inlinedtree.Options{
					ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
					Encoder:          NewMockBinaryEncoder(ctrl),
					MaximumSizeBytes: 0,
				},
			)
			require.EqualError(t, err, "//visibility:public may not be combined with other labels")
		})
	})

	t.Run("Mix", func(t *testing.T) {
		packageGroup, err := model_starlark.NewPackageGroupFromVisibility(
			[]label.CanonicalLabel{
				// Inclusion of the root package.
				label.MustNewCanonicalLabel("@@toplevel1+//:__pkg__"),
				label.MustNewCanonicalLabel("@@toplevel2+//:__subpackages__"),
				// If directories are nested, they should both
				// be represented in the resulting message.
				label.MustNewCanonicalLabel("@@nested+//foo:__pkg__"),
				label.MustNewCanonicalLabel("@@nested+//foo/bar:__pkg__"),
				// If a parent directory uses __subpackages__,
				// there is no need to preserve any children.
				label.MustNewCanonicalLabel("@@collapse1+//foo:__subpackages__"),
				label.MustNewCanonicalLabel("@@collapse1+//foo/bar:__pkg__"),
				label.MustNewCanonicalLabel("@@collapse1+//foo/bar/baz:__pkg__"),
				// If the children are created before the
				// parent, the children should be removed from
				// the resulting tree.
				label.MustNewCanonicalLabel("@@collapse2+//foo/bar/baz:__pkg__"),
				label.MustNewCanonicalLabel("@@collapse2+//foo/bar:__pkg__"),
				label.MustNewCanonicalLabel("@@collapse2+//foo:__subpackages__"),
				// If "foo" is not provided, it should still be
				// created, because "bar" should go inside it.
				label.MustNewCanonicalLabel("@@skip+//foo/bar:__pkg__"),
				// As the order of package groups is irrelevant,
				// they should be deduplicated and sorted.
				label.MustNewCanonicalLabel("@@packagegroups+//:group1"),
				label.MustNewCanonicalLabel("@@packagegroups+//:group3"),
				label.MustNewCanonicalLabel("@@packagegroups+//:group2"),
				label.MustNewCanonicalLabel("@@packagegroups+//:group4"),
				label.MustNewCanonicalLabel("@@packagegroups+//:group4"),
			},
			&inlinedtree.Options{
				ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
				Encoder:          NewMockBinaryEncoder(ctrl),
				MaximumSizeBytes: 1 << 20,
			},
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
