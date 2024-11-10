package analysis

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeModulesWithOverridesValue(ctx context.Context, key *model_analysis_pb.ModulesWithOverrides_Key, e ModulesWithOverridesEnvironment) (PatchedModulesWithOverridesValue, error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return PatchedModulesWithOverridesValue{}, evaluation.ErrMissingDependency
	}

	// TODO: Check whether an override exists in the root module's
	// MODULE.bazel!
	modules := buildSpecification.Message.BuildSpecification.GetModules()
	overridesList := make([]*model_analysis_pb.OverridesListModule, 0, len(modules))
	for _, module := range modules {
		overridesList = append(overridesList, &model_analysis_pb.OverridesListModule{
			Name: module.Name,
		})
	}

	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModulesWithOverrides_Value{
		OverridesList: overridesList,
	}), nil
}
