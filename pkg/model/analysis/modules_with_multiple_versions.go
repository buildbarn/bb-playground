package analysis

import (
	"context"
	"errors"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeModulesWithMultipleVersionsValue(ctx context.Context, key *model_analysis_pb.ModulesWithMultipleVersions_Key, e ModulesWithMultipleVersionsEnvironment) (PatchedModulesWithMultipleVersionsValue, error) {
	modulesWithOverridesValue := e.GetModulesWithOverridesValue(&model_analysis_pb.ModulesWithOverrides_Key{})
	if !modulesWithOverridesValue.IsSet() {
		return PatchedModulesWithMultipleVersionsValue{}, evaluation.ErrMissingDependency
	}
	switch modulesWithOverridesResult := modulesWithOverridesValue.Message.Result.(type) {
	case *model_analysis_pb.ModulesWithOverrides_Value_Success:
		var modulesWithMultipleVersions []*model_analysis_pb.OverridesList_Module
		for _, module := range modulesWithOverridesResult.Success.Modules {
			if len(module.Versions) > 0 {
				modulesWithMultipleVersions = append(modulesWithMultipleVersions, module)
			}
		}
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModulesWithMultipleVersions_Value{
			Result: &model_analysis_pb.ModulesWithMultipleVersions_Value_Success{
				Success: &model_analysis_pb.OverridesList{
					Modules: modulesWithMultipleVersions,
				},
			},
		}), nil
	case *model_analysis_pb.ModulesWithOverrides_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModulesWithMultipleVersions_Value{
			Result: &model_analysis_pb.ModulesWithMultipleVersions_Value_Failure{
				Failure: modulesWithOverridesResult.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModulesWithMultipleVersions_Value{
			Result: &model_analysis_pb.ModulesWithMultipleVersions_Value_Failure{
				Failure: "Modules without overrides value has an unknown result type",
			},
		}), nil
	}
}

func (c *baseComputer) ComputeModulesWithMultipleVersionsObjectValue(ctx context.Context, key *model_analysis_pb.ModulesWithMultipleVersionsObject_Key, e ModulesWithMultipleVersionsObjectEnvironment) (map[label.Module]OverrideVersions, error) {
	modulesWithMultipleVersionsValue := e.GetModulesWithMultipleVersionsValue(&model_analysis_pb.ModulesWithMultipleVersions_Key{})
	if !modulesWithMultipleVersionsValue.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	switch modulesWithMultipleVersionsResult := modulesWithMultipleVersionsValue.Message.Result.(type) {
	case *model_analysis_pb.ModulesWithMultipleVersions_Value_Success:
		return parseOverridesList(modulesWithMultipleVersionsResult.Success)
	case *model_analysis_pb.ModulesWithMultipleVersions_Value_Failure:
		return nil, errors.New(modulesWithMultipleVersionsResult.Failure)
	default:
		return nil, errors.New("Modules without overrides value has an unknown result type")
	}
}
