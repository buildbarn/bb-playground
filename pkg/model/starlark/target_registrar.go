package starlark

import (
	"fmt"

	pg_label "github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/inlinedtree"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

type TargetRegistrar struct {
	// Immutable fields.
	inlinedTreeOptions *inlinedtree.Options

	// Mutable fields.
	defaultInheritableAttrs               model_core.Message[*model_starlark_pb.InheritableAttrs, object.OutgoingReferences[object.LocalReference]]
	createDefaultInheritableAttrsMetadata func(index int) model_core.CreatedObjectTree
	setDefaultInheritableAttrs            bool
	targets                               map[string]model_core.PatchedMessage[*model_starlark_pb.Target_Definition, model_core.CreatedObjectTree]
}

func NewTargetRegistrar(inlinedTreeOptions *inlinedtree.Options, defaultInheritableAttrs model_core.Message[*model_starlark_pb.InheritableAttrs, object.OutgoingReferences[object.LocalReference]]) *TargetRegistrar {
	return &TargetRegistrar{
		inlinedTreeOptions:      inlinedTreeOptions,
		defaultInheritableAttrs: defaultInheritableAttrs,
		createDefaultInheritableAttrsMetadata: func(index int) model_core.CreatedObjectTree {
			return model_core.ExistingCreatedObjectTree
		},
		targets: map[string]model_core.PatchedMessage[*model_starlark_pb.Target_Definition, model_core.CreatedObjectTree]{},
	}
}

func (tr *TargetRegistrar) GetTargets() map[string]model_core.PatchedMessage[*model_starlark_pb.Target_Definition, model_core.CreatedObjectTree] {
	return tr.targets
}

func (tr *TargetRegistrar) getVisibilityPackageGroup(visibility []pg_label.ResolvedLabel) (model_core.PatchedMessage[*model_starlark_pb.PackageGroup, model_core.CreatedObjectTree], error) {
	if len(visibility) > 0 {
		// Explicit visibility provided. Construct new package group.
		return NewPackageGroupFromVisibility(visibility, tr.inlinedTreeOptions)
	}

	// Inherit visibility from repo() in the REPO.bazel file
	// or package() in the BUILD.bazel file.
	return model_core.NewPatchedMessageFromExisting(
		model_core.NewNestedMessage(tr.defaultInheritableAttrs, tr.defaultInheritableAttrs.Message.Visibility),
		tr.createDefaultInheritableAttrsMetadata,
	), nil
}

func (tr *TargetRegistrar) registerExplicitTarget(name string, target model_core.PatchedMessage[*model_starlark_pb.Target_Definition, model_core.CreatedObjectTree]) error {
	if tr.targets[name].IsSet() {
		return fmt.Errorf("package contains multiple targets with name %#v", name)
	}
	tr.targets[name] = target
	return nil
}

func (tr *TargetRegistrar) registerImplicitTarget(name string) {
	if _, ok := tr.targets[name]; !ok {
		tr.targets[name] = model_core.PatchedMessage[*model_starlark_pb.Target_Definition, model_core.CreatedObjectTree]{}
	}
}
