package starlark

import (
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
