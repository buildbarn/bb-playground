package starlark

import (
	"fmt"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/inlinedtree"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/starlark/unpack"

	"go.starlark.net/starlark"
)

var DefaultInheritableAttrs = model_starlark_pb.InheritableAttrs{
	Visibility: &model_starlark_pb.PackageGroup{
		Tree: &model_starlark_pb.PackageGroup_Subpackages{},
	},
}

// ParseRepoDotBazel parses a REPO.bazel file that may be stored at the
// root of a repository.
func ParseRepoDotBazel[TReference any, TMetadata model_core.CloneableReferenceMetadata](
	contents string,
	filename pg_label.CanonicalLabel,
	inlinedTreeOptions *inlinedtree.Options,
	objectCapturer model_core.CreatedObjectCapturer[TMetadata],
) (model_core.PatchedMessage[*model_starlark_pb.InheritableAttrs, TMetadata], error) {
	var defaultAttrs model_core.PatchedMessage[*model_starlark_pb.InheritableAttrs, TMetadata]
	_, err := starlark.ExecFile(
		&starlark.Thread{
			Name: "main",
			Print: func(_ *starlark.Thread, msg string) {
				// TODO: Provide logging sink.
				fmt.Println(msg)
			},
		},
		filename.String(),
		contents,
		starlark.StringDict{
			"repo": starlark.NewBuiltin("repo", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if defaultAttrs.IsSet() {
					return nil, fmt.Errorf("%s: function can only be invoked once", b.Name())
				}
				newDefaultAttrs, err := getDefaultInheritableAttrs[TReference, TMetadata](
					thread,
					b,
					args,
					kwargs,
					model_core.NewSimpleMessage[model_core.CloneableReference[TMetadata]](&DefaultInheritableAttrs),
					inlinedTreeOptions,
					objectCapturer,
				)
				if err != nil {
					return nil, err
				}
				defaultAttrs = newDefaultAttrs
				return starlark.None, nil
			}),
		},
	)
	if !defaultAttrs.IsSet() {
		defaultAttrs = model_core.NewSimplePatchedMessage[TMetadata](&DefaultInheritableAttrs)
	}
	return defaultAttrs, err
}

// getDefaultInheritableAttrs parses the arguments provided to
// REPO.bazel's repo() function or BUILD.bazel's package() function.
func getDefaultInheritableAttrs[TReference any, TMetadata model_core.CloneableReferenceMetadata](
	thread *starlark.Thread,
	b *starlark.Builtin,
	args starlark.Tuple,
	kwargs []starlark.Tuple,
	previousInheritableAttrs model_core.Message[*model_starlark_pb.InheritableAttrs, model_core.CloneableReference[TMetadata]],
	inlinedTreeOptions *inlinedtree.Options,
	objectCapturer model_core.CreatedObjectCapturer[TMetadata],
) (model_core.PatchedMessage[*model_starlark_pb.InheritableAttrs, TMetadata], error) {
	if len(args) > 0 {
		return model_core.PatchedMessage[*model_starlark_pb.InheritableAttrs, TMetadata]{}, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
	}

	var applicableLicenses []string
	deprecation := previousInheritableAttrs.Message.Deprecation
	packageMetadata := previousInheritableAttrs.Message.PackageMetadata
	testOnly := previousInheritableAttrs.Message.Testonly
	var visibility []pg_label.ResolvedLabel
	canonicalPackage := CurrentFilePackage(thread, 1)
	labelUnpackerInto := NewLabelOrStringUnpackerInto[TReference, TMetadata](canonicalPackage)
	labelStringListUnpackerInto := unpack.List(unpack.Stringer(labelUnpackerInto))
	var features []string
	if err := starlark.UnpackArgs(
		b.Name(), args, kwargs,
		"default_applicable_licenses?", unpack.Bind(thread, &applicableLicenses, labelStringListUnpackerInto),
		"default_deprecation?", unpack.Bind(thread, &deprecation, unpack.String),
		"default_package_metadata?", unpack.Bind(thread, &packageMetadata, labelStringListUnpackerInto),
		"default_testonly?", unpack.Bind(thread, &testOnly, unpack.Bool),
		"default_visibility?", unpack.Bind(thread, &visibility, unpack.List(labelUnpackerInto)),
		"features?", unpack.Bind(thread, &features, unpack.List(unpack.String)),
	); err != nil {
		return model_core.PatchedMessage[*model_starlark_pb.InheritableAttrs, TMetadata]{}, err
	}

	// default_applicable_licenses is an alias for default_package_metadata.
	if len(applicableLicenses) > 0 {
		if len(packageMetadata) > 0 {
			return model_core.PatchedMessage[*model_starlark_pb.InheritableAttrs, TMetadata]{}, fmt.Errorf("%s: default_applicable_licenses and default_package_metadata are mutually exclusive", b.Name())
		}
		packageMetadata = applicableLicenses
	}

	var visibilityPackageGroup model_core.PatchedMessage[*model_starlark_pb.PackageGroup, TMetadata]
	if len(visibility) > 0 {
		// Explicit visibility provided. Construct a new package group.
		var err error
		visibilityPackageGroup, err = NewPackageGroupFromVisibility[TMetadata](visibility, inlinedTreeOptions, objectCapturer)
		if err != nil {
			return model_core.PatchedMessage[*model_starlark_pb.InheritableAttrs, TMetadata]{}, err
		}
	} else {
		// Clone the existing visibility.
		visibilityPackageGroup = model_core.NewPatchedMessageFromCloneable(
			model_core.NewNestedMessage(previousInheritableAttrs, previousInheritableAttrs.Message.Visibility),
		)
	}

	// TODO: Also store features?
	return model_core.NewPatchedMessage(
		&model_starlark_pb.InheritableAttrs{
			Deprecation:     deprecation,
			PackageMetadata: packageMetadata,
			Testonly:        testOnly,
			Visibility:      visibilityPackageGroup.Message,
		},
		visibilityPackageGroup.Patcher,
	), nil
}
