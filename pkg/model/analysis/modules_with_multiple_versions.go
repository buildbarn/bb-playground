package analysis

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	"github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
)

func (c *baseComputer[TReference, TMetadata]) ComputeModulesWithMultipleVersionsValue(ctx context.Context, key *model_analysis_pb.ModulesWithMultipleVersions_Key, e ModulesWithMultipleVersionsEnvironment[TReference]) (PatchedModulesWithMultipleVersionsValue, error) {
	modulesWithOverridesValue := e.GetModulesWithOverridesValue(&model_analysis_pb.ModulesWithOverrides_Key{})
	if !modulesWithOverridesValue.IsSet() {
		return PatchedModulesWithMultipleVersionsValue{}, evaluation.ErrMissingDependency
	}
	var modulesWithMultipleVersions []*model_analysis_pb.OverridesListModule
	for _, module := range modulesWithOverridesValue.Message.OverridesList {
		if len(module.Versions) > 0 {
			modulesWithMultipleVersions = append(modulesWithMultipleVersions, module)
		}
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModulesWithMultipleVersions_Value{
		OverridesList: modulesWithMultipleVersions,
	}), nil
}

func (c *baseComputer[TReference, TMetadata]) ComputeModulesWithMultipleVersionsObjectValue(ctx context.Context, key *model_analysis_pb.ModulesWithMultipleVersionsObject_Key, e ModulesWithMultipleVersionsObjectEnvironment[TReference]) (map[label.Module]OverrideVersions, error) {
	modulesWithMultipleVersionsValue := e.GetModulesWithMultipleVersionsValue(&model_analysis_pb.ModulesWithMultipleVersions_Key{})
	if !modulesWithMultipleVersionsValue.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	return parseOverridesList(modulesWithMultipleVersionsValue.Message.OverridesList)
}
