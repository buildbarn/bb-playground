package analysis

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeRootModuleValue(ctx context.Context, key *model_analysis_pb.RootModule_Key, e RootModuleEnvironment) (PatchedRootModuleValue, error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return PatchedRootModuleValue{}, evaluation.ErrMissingDependency
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RootModule_Value{
		RootModuleName:                  buildSpecification.Message.BuildSpecification.RootModuleName,
		IgnoreRootModuleDevDependencies: buildSpecification.Message.BuildSpecification.IgnoreRootModuleDevDependencies,
	}), nil
}
