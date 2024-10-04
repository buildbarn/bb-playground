package starlark

import (
	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type transitionValue struct {
	implementation starlark.Value
	inputs         []pg_label.CanonicalLabel
	outputs        []pg_label.CanonicalLabel
}

var _ EncodableValue = &transitionValue{}

func NewTransitionValue(implementation starlark.Value, inputs, outputs []pg_label.CanonicalLabel) starlark.Value {
	return &transitionValue{
		implementation: implementation,
		inputs:         inputs,
		outputs:        outputs,
	}
}

func (tv *transitionValue) String() string {
	return "<transition>"
}

func (tv *transitionValue) Type() string {
	return "transition"
}

func (tv *transitionValue) Freeze() {}

func (tv *transitionValue) Truth() starlark.Bool {
	return starlark.True
}

func (tv *transitionValue) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (tv *transitionValue) EncodeValue(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	// TODO!
	needsCode := false
	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Transition{
				Transition: &model_starlark_pb.TransitionValue{},
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, needsCode, nil
}
