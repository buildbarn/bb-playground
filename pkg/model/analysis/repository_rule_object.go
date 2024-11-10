package analysis

import (
	"context"
	"errors"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"

	"go.starlark.net/starlark"
)

type RepositoryRule struct {
	Implementation starlark.Callable
	Attrs          AttrsDict
}

func (c *baseComputer) ComputeRepositoryRuleObjectValue(ctx context.Context, key *model_analysis_pb.RepositoryRuleObject_Key, e RepositoryRuleObjectEnvironment) (*RepositoryRule, error) {
	repositoryRuleValue := e.GetCompiledBzlFileGlobalValue(&model_analysis_pb.CompiledBzlFileGlobal_Key{
		Identifier: key.Identifier,
	})
	if !repositoryRuleValue.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}

	v, ok := repositoryRuleValue.Message.Global.GetKind().(*model_starlark_pb.Value_RepositoryRule)
	if !ok {
		return nil, errors.New("global value is not a repository rule")
	}
	d, ok := v.RepositoryRule.Kind.(*model_starlark_pb.RepositoryRule_Definition_)
	if !ok {
		return nil, errors.New("global value is not a repository rule definition")
	}

	attrs, err := c.decodeAttrsDict(ctx, model_core.Message[[]*model_starlark_pb.NamedAttr]{
		Message:            d.Definition.Attrs,
		OutgoingReferences: repositoryRuleValue.OutgoingReferences,
	})
	if err != nil {
		return nil, err
	}

	return &RepositoryRule{
		Implementation: model_starlark.NewNamedFunction(model_starlark.NewProtoNamedFunctionDefinition(
			model_core.Message[*model_starlark_pb.Function]{
				Message:            d.Definition.Implementation,
				OutgoingReferences: repositoryRuleValue.OutgoingReferences,
			},
		)),
		Attrs: attrs,
	}, nil
}
