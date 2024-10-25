package analysis

import (
	"context"
	"errors"
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeConfiguredTargetValue(ctx context.Context, key *model_analysis_pb.ConfiguredTarget_Key, e ConfiguredTargetEnvironment) (PatchedConfiguredTargetValue, error) {
	targetLabel, err := label.NewCanonicalLabel(key.Label)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
			Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
				Failure: fmt.Sprintf("invalid target label %#v: %s", key.Label, err),
			},
		}), nil
	}
	targetValue := e.GetTargetValue(&model_analysis_pb.Target_Key{
		Label: targetLabel.String(),
	})
	if !targetValue.IsSet() {
		return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
	}

	var targetDefinition model_core.Message[*model_starlark_pb.TargetDefinition]
	switch resultType := targetValue.Message.Result.(type) {
	case *model_analysis_pb.Target_Value_Success:
		targetDefinition = model_core.Message[*model_starlark_pb.TargetDefinition]{
			Message:            resultType.Success,
			OutgoingReferences: targetValue.OutgoingReferences,
		}
	case *model_analysis_pb.Target_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
			Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
				Failure: resultType.Failure,
			},
		}), nil
	default:
		return PatchedConfiguredTargetValue{}, errors.New("Package value has an unknown result type")
	}

	targetKind, ok := targetDefinition.Message.Kind.(*model_starlark_pb.TargetDefinition_RuleTarget)
	if !ok {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
			Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
				Failure: "Only rule targets can be configured",
			},
		}), nil
	}
	ruleTarget := targetKind.RuleTarget

	configuredRule, err := e.GetConfiguredRuleObjectValue(&model_analysis_pb.ConfiguredRuleObject_Key{
		Identifier: ruleTarget.RuleIdentifier,
	})
	if err != nil {
		return PatchedConfiguredTargetValue{}, err
	}

	panic(configuredRule)

	/*
	   	ruleValue := e.GetCompiledBzlFileGlobalValue(&model_analysis_pb.CompiledBzlFileGlobal_Key{
	   		Identifier: ruleTarget.RuleIdentifier,
	   	})

	   	if !ruleValue.IsSet() {
	   		return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
	   	}

	   var ruleDefinition model_core.Message[*model_starlark_pb.Rule_Definition]
	   switch resultType := ruleValue.Message.Result.(type) {
	   case *model_analysis_pb.CompiledBzlFileGlobal_Value_Success:

	   	v, ok := resultType.Success.Kind.(*model_starlark_pb.Value_Rule)
	   	if !ok {
	   		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
	   			Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
	   				Failure: fmt.Sprintf("Global value %#v is not a rule", ruleTarget.RuleIdentifier),
	   			},
	   		}), nil
	   	}
	   	d, ok := v.Rule.Kind.(*model_starlark_pb.Rule_Definition_)
	   	if !ok {
	   		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
	   			Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
	   				Failure: fmt.Sprintf("Global value %#v is not a rule definition", ruleTarget.RuleIdentifier),
	   			},
	   		}), nil
	   	}
	   	ruleDefinition = model_core.Message[*model_starlark_pb.Rule_Definition]{
	   		Message:            d.Definition,
	   		OutgoingReferences: ruleValue.OutgoingReferences,
	   	}

	   case *model_analysis_pb.CompiledBzlFileGlobal_Value_Failure:

	   	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
	   		Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
	   			Failure: resultType.Failure,
	   		},
	   	}), nil

	   default:

	   		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
	   			Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
	   				Failure: fmt.Sprintf("Global value for rule %#v has an unknown result type", ruleTarget.RuleIdentifier),
	   			},
	   		}), nil
	   	}

	   // Resolve the rule's implementation function.
	   // TODO: Would we want to move this into a separate function, so
	   // that the function remains cached across targets?
	   implementationIdentifier, err := label.NewCanonicalStarlarkIdentifier(ruleDefinition.Message.Implementation)

	   	if err != nil {
	   		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ConfiguredTarget_Value{
	   			Result: &model_analysis_pb.ConfiguredTarget_Value_Failure{
	   				Failure: fmt.Sprintf("Invalid canonical Starlark identifier %#v: %s", ruleDefinition.Message.Implementation, err),
	   			},
	   		}), nil
	   	}

	   implementationFunc := model_starlark.NewNamedFunction(implementationIdentifier, nil)

	   allBuiltinsModulesNames := e.GetBuiltinsModuleNamesValue(&model_analysis_pb.BuiltinsModuleNames_Key{})

	   	if !allBuiltinsModulesNames.IsSet() {
	   		return PatchedConfiguredTargetValue{}, evaluation.ErrMissingDependency
	   	}

	   v, err := starlark.Call(

	   	c.newStarlarkThread(e, allBuiltinsModulesNames.Message.BuiltinsModuleNames),
	   	implementationFunc,
	   	starlark.Tuple{
	   		starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
	   			"attr":       starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{}),
	   			"toolchains": starlark.None,
	   		}),
	   	},
	   	nil,

	   )

	   	if err != nil {
	   		if !errors.Is(err, evaluation.ErrMissingDependency) {
	   			var evalErr *starlark.EvalError
	   			if errors.As(err, &evalErr) {
	   				panic(evalErr.Backtrace())
	   			} else {
	   				panic(err.Error())
	   			}
	   		}
	   		return PatchedConfiguredTargetValue{}, err
	   	}

	   panic(v)
	*/
}
