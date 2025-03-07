package starlark

import (
	"fmt"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/inlinedtree"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
)

type TargetRegistrar[TMetadata model_core.CloneableReferenceMetadata] struct {
	// Immutable fields.
	inlinedTreeOptions *inlinedtree.Options
	objectCapturer     model_core.CreatedObjectCapturer[TMetadata]

	// Mutable fields.
	defaultInheritableAttrs    model_core.Message[*model_starlark_pb.InheritableAttrs, model_core.CloneableReference[TMetadata]]
	setDefaultInheritableAttrs bool
	targets                    map[string]model_core.PatchedMessage[*model_starlark_pb.Target_Definition, TMetadata]
}

func NewTargetRegistrar[TMetadata model_core.CloneableReferenceMetadata](inlinedTreeOptions *inlinedtree.Options, objectCapturer model_core.CreatedObjectCapturer[TMetadata], defaultInheritableAttrs model_core.Message[*model_starlark_pb.InheritableAttrs, model_core.CloneableReference[TMetadata]]) *TargetRegistrar[TMetadata] {
	return &TargetRegistrar[TMetadata]{
		inlinedTreeOptions:      inlinedTreeOptions,
		objectCapturer:          objectCapturer,
		defaultInheritableAttrs: defaultInheritableAttrs,
		targets:                 map[string]model_core.PatchedMessage[*model_starlark_pb.Target_Definition, TMetadata]{},
	}
}

func (tr *TargetRegistrar[TMetadata]) GetTargets() map[string]model_core.PatchedMessage[*model_starlark_pb.Target_Definition, TMetadata] {
	return tr.targets
}

func (tr *TargetRegistrar[TMetadata]) getVisibilityPackageGroup(visibility []pg_label.ResolvedLabel) (model_core.PatchedMessage[*model_starlark_pb.PackageGroup, TMetadata], error) {
	if len(visibility) > 0 {
		// Explicit visibility provided. Construct new package group.
		return NewPackageGroupFromVisibility[TMetadata](visibility, tr.inlinedTreeOptions, tr.objectCapturer)
	}

	// Inherit visibility from repo() in the REPO.bazel file
	// or package() in the BUILD.bazel file.
	return model_core.NewPatchedMessageFromCloneable(
		model_core.NewNestedMessage(tr.defaultInheritableAttrs, tr.defaultInheritableAttrs.Message.Visibility),
	), nil
}

func (tr *TargetRegistrar[TMetadata]) registerExplicitTarget(name string, target model_core.PatchedMessage[*model_starlark_pb.Target_Definition, TMetadata]) error {
	if tr.targets[name].IsSet() {
		return fmt.Errorf("package contains multiple targets with name %#v", name)
	}
	tr.targets[name] = target
	return nil
}

func (tr *TargetRegistrar[TMetadata]) registerImplicitTarget(name string) {
	if _, ok := tr.targets[name]; !ok {
		tr.targets[name] = model_core.PatchedMessage[*model_starlark_pb.Target_Definition, TMetadata]{}
	}
}
