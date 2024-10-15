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

func TestParseModuleDotBazel(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("Empty", func(t *testing.T) {
		// If no calls to repo() are made, the resulting
		// attributes should be identical to the constant
		// message value we provide.
		defaultAttrs, err := model_starlark.ParseRepoDotBazel(
			"",
			label.MustNewCanonicalLabel("@@foo+//:REPO.bazel"),
			&inlinedtree.Options{
				ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
				Encoder:          NewMockBinaryEncoder(ctrl),
				MaximumSizeBytes: 0,
			},
		)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &model_starlark.DefaultInheritableAttrs, defaultAttrs.Message)
	})

	t.Run("NoArguments", func(t *testing.T) {
		// It should be valid to call repo() without any
		// arguments. In that case the returned attributes
		// should also be equal to the default.
		defaultAttrs, err := model_starlark.ParseRepoDotBazel(
			"repo()",
			label.MustNewCanonicalLabel("@@foo+//:REPO.bazel"),
			&inlinedtree.Options{
				ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
				Encoder:          NewMockBinaryEncoder(ctrl),
				MaximumSizeBytes: 0,
			},
		)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &model_starlark.DefaultInheritableAttrs, defaultAttrs.Message)
	})

	t.Run("RedundantCalls", func(t *testing.T) {
		// Calling repo() times is not permitted.
		_, err := model_starlark.ParseRepoDotBazel(
			"repo()\nrepo()",
			label.MustNewCanonicalLabel("@@foo+//:REPO.bazel"),
			&inlinedtree.Options{
				ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
				Encoder:          NewMockBinaryEncoder(ctrl),
				MaximumSizeBytes: 0,
			},
		)
		require.EqualError(t, err, "repo: function can only be invoked once")
	})

	t.Run("ApplicableLicensesAndPackageMetadata", func(t *testing.T) {
		// default_applicable_licenses is an alias of
		// default_package_metadata. It's not possible to
		// provide both arguments at once.
		_, err := model_starlark.ParseRepoDotBazel(
			`repo(
				default_applicable_licenses = ["//:license"],
				default_package_metadata = ["//:metadata"],
			)`,
			label.MustNewCanonicalLabel("@@foo+//:REPO.bazel"),
			&inlinedtree.Options{
				ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
				Encoder:          NewMockBinaryEncoder(ctrl),
				MaximumSizeBytes: 0,
			},
		)
		require.EqualError(t, err, "repo: default_applicable_licenses and default_package_metadata are mutually exclusive")
	})

	t.Run("AllArguments", func(t *testing.T) {
		// Example invocation where all supported arguments are
		// provided.
		defaultAttrs, err := model_starlark.ParseRepoDotBazel(
			`repo(
				default_deprecation = "All code in this repository is deprecated.",
				default_package_metadata = ["//:metadata"],
				default_testonly = True,
				default_visibility = [
					"//somepackage:__pkg__",
				],
			)`,
			label.MustNewCanonicalLabel("@@foo+//:REPO.bazel"),
			&inlinedtree.Options{
				ReferenceFormat:  object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1),
				Encoder:          NewMockBinaryEncoder(ctrl),
				MaximumSizeBytes: 0,
			},
		)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &model_starlark_pb.InheritableAttrs{
			Deprecation: "All code in this repository is deprecated.",
			PackageMetadata: []string{
				"@@foo+//:metadata",
			},
			Testonly: true,
			Visibility: &model_starlark_pb.PackageGroup{
				Tree: &model_starlark_pb.PackageGroup_Subpackages{
					Overrides: &model_starlark_pb.PackageGroup_Subpackages_OverridesInline{
						OverridesInline: &model_starlark_pb.PackageGroup_Subpackages_Overrides{
							Packages: []*model_starlark_pb.PackageGroup_Package{{
								Component: "foo+",
								Subpackages: &model_starlark_pb.PackageGroup_Subpackages{
									Overrides: &model_starlark_pb.PackageGroup_Subpackages_OverridesInline{
										OverridesInline: &model_starlark_pb.PackageGroup_Subpackages_Overrides{
											Packages: []*model_starlark_pb.PackageGroup_Package{
												{
													Component:      "somepackage",
													IncludePackage: true,
													Subpackages:    &model_starlark_pb.PackageGroup_Subpackages{},
												},
											},
										},
									},
								},
							}},
						},
					},
				},
			},
		}, defaultAttrs.Message)
	})
}
