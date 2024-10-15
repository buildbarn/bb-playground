package starlark

import (
	"errors"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type Transition struct {
	LateNamedValue
}

var (
	_ EncodableValue = &Transition{}
	_ NamedGlobal    = &Transition{}
)

func NewTransition(identifier *pg_label.CanonicalStarlarkIdentifier, definition *model_starlark_pb.Transition_Definition) starlark.Value {
	return &Transition{
		LateNamedValue: LateNamedValue{
			Identifier: identifier,
		},
	}
}

func (t *Transition) String() string {
	return "<transition>"
}

func (t *Transition) Type() string {
	return "transition"
}

func (t *Transition) Freeze() {}

func (t *Transition) Truth() starlark.Bool {
	return starlark.True
}

func (t *Transition) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (t *Transition) EncodeValue(path map[starlark.Value]struct{}, currentIdentifier *pg_label.CanonicalStarlarkIdentifier, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	if t.Identifier == nil {
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{}, false, errors.New("transition does not have a name")
	}
	if currentIdentifier == nil || *currentIdentifier != *t.Identifier {
		// Not the canonical identifier under which this
		// transition is known. Emit a reference.
		return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
			Message: &model_starlark_pb.Value{
				Kind: &model_starlark_pb.Value_Transition{
					Transition: &model_starlark_pb.Transition{
						Kind: &model_starlark_pb.Transition_Reference{
							Reference: t.Identifier.String(),
						},
					},
				},
			},
			Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
		}, false, nil
	}

	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Transition{
				Transition: &model_starlark_pb.Transition{
					Kind: &model_starlark_pb.Transition_Definition_{
						Definition: &model_starlark_pb.Transition_Definition{},
					},
				},
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, false, nil
}
