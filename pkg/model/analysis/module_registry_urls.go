package analysis

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeModuleRegistryUrlsValue(ctx context.Context, key *model_analysis_pb.ModuleRegistryUrls_Key, e ModuleRegistryUrlsEnvironment) (PatchedModuleRegistryUrlsValue, error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return PatchedModuleRegistryUrlsValue{}, evaluation.ErrMissingDependency
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRegistryUrls_Value{
		RegistryUrls: buildSpecification.Message.BuildSpecification.ModuleRegistryUrls,
	}), nil
}