package starlark

import (
	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type ruleValue struct {
	filename pg_label.CanonicalLabel
	name     string
}

var _ EncodableValue = &ruleValue{}

func NewRuleValue(filename pg_label.CanonicalLabel, name string) starlark.Value {
	return &ruleValue{
		filename: filename,
		name:     name,
	}
}

func (rv *ruleValue) String() string {
	return "<rule>"
}

func (rv *ruleValue) Type() string {
	return "rule"
}

func (rv *ruleValue) Freeze() {}

func (rv *ruleValue) Truth() starlark.Bool {
	return starlark.True
}

func (rv *ruleValue) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (rv *ruleValue) EncodeValue(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	// TODO!
	needsCode := false
	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_Rule{
				Rule: &model_starlark_pb.IdentifierReference{
					Filename: rv.filename.String(),
					Name:     rv.name,
				},
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, needsCode, nil
}
