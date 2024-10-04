package starlark

import (
	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type subruleValue struct {
	filename pg_label.CanonicalLabel
	name     string
}

var _ EncodableValue = &subruleValue{}

func NewSubruleValue(filename pg_label.CanonicalLabel, name string) starlark.Value {
	return &subruleValue{
		filename: filename,
		name:     name,
	}
}

func (sv *subruleValue) String() string {
	return "<rule>"
}

func (sv *subruleValue) Type() string {
	return "rule"
}

func (sv *subruleValue) Freeze() {}

func (sv *subruleValue) Truth() starlark.Bool {
	return starlark.True
}

func (sv *subruleValue) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (sv *subruleValue) EncodeValue(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	// TODO!
	needsCode := false
	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Subrule{
				Subrule: &model_starlark_pb.IdentifierReference{
					Filename: sv.filename.String(),
					Name:     sv.name,
				},
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, needsCode, nil
}
