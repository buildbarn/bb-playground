package starlark

import (
	"errors"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type subrule struct {
	LateNamedValue
	implementation *NamedFunction
}

var (
	_ EncodableValue = &subrule{}
	_ NamedGlobal    = &subrule{}
)

func NewSubrule(identifier *pg_label.CanonicalStarlarkIdentifier, definition *model_starlark_pb.Subrule_Definition) starlark.Value {
	return &subrule{
		LateNamedValue: LateNamedValue{
			Identifier: identifier,
		},
	}
}

func (sr *subrule) String() string {
	return "<rule>"
}

func (sr *subrule) Type() string {
	return "rule"
}

func (sr *subrule) Freeze() {}

func (sr *subrule) Truth() starlark.Bool {
	return starlark.True
}

func (sr *subrule) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (sr *subrule) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	if sr.Identifier == nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, errors.New("subrule does not have a name")
	}
	if currentIdentifier == nil || *currentIdentifier != *sr.Identifier {
		// Not the canonical identifier under which this subrule
		// is known. Emit a reference.
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
			Message: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_Subrule{
					Subrule: &model_starlark_pb.Subrule{
						Kind: &model_starlark_pb.Subrule_Reference{
							Reference: sr.Identifier.String(),
						},
					},
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, false, nil
	}

	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Subrule{
				Subrule: &model_starlark_pb.Subrule{
					Kind: &model_starlark_pb.Subrule_Definition_{
						Definition: &model_starlark_pb.Subrule_Definition{},
					},
				},
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, false, nil
}
