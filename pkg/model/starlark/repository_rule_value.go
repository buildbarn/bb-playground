package starlark

import (
	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type repositoryRuleValue struct {
	filename pg_label.CanonicalLabel
	name     string
}

var _ EncodableValue = &repositoryRuleValue{}

func NewRepositoryRuleValue(filename pg_label.CanonicalLabel, name string) starlark.Value {
	return &repositoryRuleValue{
		filename: filename,
		name:     name,
	}
}

func (rrv *repositoryRuleValue) String() string {
	return "<rule>"
}

func (rrv *repositoryRuleValue) Type() string {
	return "rule"
}

func (rrv *repositoryRuleValue) Freeze() {}

func (rrv *repositoryRuleValue) Truth() starlark.Bool {
	return starlark.True
}

func (rrv *repositoryRuleValue) Hash() (uint32, error) {
	// TODO
	return 0, nil
}

func (rrv *repositoryRuleValue) EncodeValue(path map[starlark.Value]struct{}, options *ValueEncodingOptions) (model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker], bool, error) {
	// TODO!
	needsCode := false
	return model_core.PatchedMessage[*model_starlark_pb.Value, dag.ObjectContentsWalker]{
		Message: &model_starlark_pb.Value{
			Kind: &model_starlark_pb.Value_RepositoryRule{
				RepositoryRule: &model_starlark_pb.IdentifierReference{
					Filename: rrv.filename.String(),
					Name:     rrv.name,
				},
			},
		},
		Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
	}, needsCode, nil
}
