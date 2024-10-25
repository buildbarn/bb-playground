package analysis

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeConfiguredRuleValue(ctx context.Context, key *model_analysis_pb.ConfiguredRule_Key, e ConfiguredRuleEnvironment) (PatchedConfiguredRuleValue, error) {
	ruleValue := e.GetCompiledBzlFileGlobalValue(&model_analysis_pb.CompiledBzlFileGlobal_Key{
		Identifier: key.Identifier,
	})
	if !ruleValue.IsSet() {
		return PatchedConfiguredRuleValue{}, evaluation.ErrMissingDependency
	}
	var ruleDefinition model_core.Message[*model_starlark_pb.Rule_Definition]
	switch resultType := ruleValue.Message.Result.(type) {
	case *model_analysis_pb.CompiledBzlFileGlobal_Value_Success:
		v, ok := resultType.Success.Kind.(*model_starlark_pb.Value_Rule)
		if !ok {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredRule_Value{
				Result: &model_analysis_pb.ConfiguredRule_Value_Failure{
					Failure: fmt.Sprintf("Global value %#v is not a rule", key.Identifier),
				},
			}), nil
		}
		d, ok := v.Rule.Kind.(*model_starlark_pb.Rule_Definition_)
		if !ok {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredRule_Value{
				Result: &model_analysis_pb.ConfiguredRule_Value_Failure{
					Failure: fmt.Sprintf("Global value %#v is not a rule definition", key.Identifier),
				},
			}), nil
		}
		ruleDefinition = model_core.Message[*model_starlark_pb.Rule_Definition]{
			Message:            d.Definition,
			OutgoingReferences: ruleValue.OutgoingReferences,
		}
	case *model_analysis_pb.CompiledBzlFileGlobal_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredRule_Value{
			Result: &model_analysis_pb.ConfiguredRule_Value_Failure{
				Failure: resultType.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredRule_Value{
			Result: &model_analysis_pb.ConfiguredRule_Value_Failure{
				Failure: fmt.Sprintf("Global value for rule %#v has an unknown result type", key.Identifier),
			},
		}), nil
	}

	missingResolvedToolchains := false
	for _, execGroup := range ruleDefinition.Message.ExecGroups {
		resolvedToolchains := e.GetResolvedToolchainsValue(&model_analysis_pb.ResolvedToolchains_Key{
			ExecGroup: execGroup.ExecGroup,
		})
		if !resolvedToolchains.IsSet() {
			missingResolvedToolchains = true
			continue
		}
		panic("TODO: Add resolved toolchain to configured rule")
	}
	if missingResolvedToolchains {
		return PatchedConfiguredRuleValue{}, evaluation.ErrMissingDependency
	}

	for _, attr := range ruleDefinition.Message.Attrs {
		if strings.HasPrefix(attr.Name, "_") {
			panic("TODO: Already evaluate private attributes!")
		}
	}

	patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
	return model_core.PatchedMessage[*model_analysis_pb.ConfiguredRule_Value, dag.ObjectContentsWalker]{
		Message: &model_analysis_pb.ConfiguredRule_Value{
			Result: &model_analysis_pb.ConfiguredRule_Value_Success_{
				Success: &model_analysis_pb.ConfiguredRule_Value_Success{},
			},
		},
		Patcher: patcher,
	}, nil
}

type ConfiguredRule struct{}

func (c *baseComputer) ComputeConfiguredRuleObjectValue(ctx context.Context, key *model_analysis_pb.ConfiguredRuleObject_Key, e ConfiguredRuleObjectEnvironment) (*ConfiguredRule, error) {
	configuredRuleValue := e.GetConfiguredRuleValue(&model_analysis_pb.ConfiguredRule_Key{
		Identifier: key.Identifier,
	})
	if !configuredRuleValue.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	switch resultType := configuredRuleValue.Message.Result.(type) {
	case *model_analysis_pb.ConfiguredRule_Value_Success_:
		return &ConfiguredRule{}, nil
	case *model_analysis_pb.ConfiguredRule_Value_Failure:
		return nil, errors.New(resultType.Failure)
	default:
		return nil, errors.New("Configured rule has an unknown result type")
	}
}
