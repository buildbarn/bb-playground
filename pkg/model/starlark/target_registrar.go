package starlark

import (
	"fmt"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/inlinedtree"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

type TargetRegistrar struct {
	// Immutable fields.
	inlinedTreeOptions *inlinedtree.Options

	// Mutable fields.
	defaultInheritableAttrs               model_core.Message[*model_starlark_pb.InheritableAttrs]
	createDefaultInheritableAttrsMetadata func(index int) dag.ObjectContentsWalker
	setDefaultInheritableAttrs            bool
	targets                               map[string]model_core.PatchedMessage[*model_starlark_pb.Target, dag.ObjectContentsWalker]
}

func NewTargetRegistrar(inlinedTreeOptions *inlinedtree.Options, defaultInheritableAttrs model_core.Message[*model_starlark_pb.InheritableAttrs]) *TargetRegistrar {
	return &TargetRegistrar{
		inlinedTreeOptions:      inlinedTreeOptions,
		defaultInheritableAttrs: defaultInheritableAttrs,
		createDefaultInheritableAttrsMetadata: func(index int) dag.ObjectContentsWalker {
			return dag.ExistingObjectContentsWalker
		},
		targets: map[string]model_core.PatchedMessage[*model_starlark_pb.Target, dag.ObjectContentsWalker]{},
	}
}

func (tr *TargetRegistrar) GetTargets() map[string]model_core.PatchedMessage[*model_starlark_pb.Target, dag.ObjectContentsWalker] {
	return tr.targets
}

func (tr *TargetRegistrar) getVisibilityPackageGroup(visibility []pg_label.CanonicalLabel) (model_core.PatchedMessage[*model_starlark_pb.PackageGroup, dag.ObjectContentsWalker], error) {
	if len(visibility) > 0 {
		// Explicit visibility provided. Construct new package group.
		return NewPackageGroupFromVisibility(visibility, tr.inlinedTreeOptions)
	}

	// Inherit visibility from repo() in the REPO.bazel file
	// or package() in the BUILD.bazel file.
	return model_core.NewPatchedMessageFromExisting(
		model_core.Message[*model_starlark_pb.PackageGroup]{
			Message:            tr.defaultInheritableAttrs.Message.Visibility,
			OutgoingReferences: tr.defaultInheritableAttrs.OutgoingReferences,
		},
		tr.createDefaultInheritableAttrsMetadata,
	), nil
}

func (tr *TargetRegistrar) registerTarget(name string, target model_core.PatchedMessage[*model_starlark_pb.Target, dag.ObjectContentsWalker]) error {
	if _, ok := tr.targets[name]; ok {
		return fmt.Errorf("package contains multiple targets with name %#v", name)
	}
	tr.targets[name] = target
	return nil
}
