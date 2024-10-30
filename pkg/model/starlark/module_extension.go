package starlark

import (
	"errors"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type moduleExtension struct {
	LateNamedValue
	definition *model_starlark_pb.ModuleExtension_Definition
}

var (
	_ starlark.Value = &moduleExtension{}
	_ EncodableValue = &moduleExtension{}
)

func NewModuleExtension(identifier *pg_label.CanonicalStarlarkIdentifier, definition *model_starlark_pb.ModuleExtension_Definition) starlark.Value {
	return &moduleExtension{
		LateNamedValue: LateNamedValue{
			Identifier: identifier,
		},
		definition: definition,
	}
}

func (me *moduleExtension) String() string {
	return "<module_extension>"
}

func (me *moduleExtension) Type() string {
	return "module_extension"
}

func (me *moduleExtension) Freeze() {}

func (me *moduleExtension) Truth() starlark.Bool {
	return starlark.True
}

func (me *moduleExtension) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (me *moduleExtension) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	if me.Identifier == nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, errors.New("module extension does not have a name")
	}
	if currentIdentifier == nil || *currentIdentifier != *me.Identifier {
		// Not the canonical identifier under which this module
		// extension is known. Emit a reference.
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
			Message: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_ModuleExtension{
					ModuleExtension: &model_starlark_pb.ModuleExtension{
						Kind: &model_starlark_pb.ModuleExtension_Reference{
							Reference: me.Identifier.String(),
						},
					},
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, false, nil
	}

	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_starlark_pb.Value{
		Kind: &model_starlark_pb.Value_ModuleExtension{
			ModuleExtension: &model_starlark_pb.ModuleExtension{
				Kind: &model_starlark_pb.ModuleExtension_Definition_{
					Definition: me.definition,
				},
			},
		},
	}), false, nil
}
