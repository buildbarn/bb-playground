package analysis

import (
	"context"
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeCompatibleExecutionPlatformsValue(ctx context.Context, key *model_analysis_pb.CompatibleExecutionPlatforms_Key, e CompatibleExecutionPlatformsEnvironment) (PatchedCompatibleExecutionPlatformsValue, error) {
	registeredExecutionPlatforms := e.GetRegisteredExecutionPlatformsValue(&model_analysis_pb.RegisteredExecutionPlatforms_Key{})
	if !registeredExecutionPlatforms.IsSet() {
		return PatchedCompatibleExecutionPlatformsValue{}, evaluation.ErrMissingDependency
	}

	allExecutionPlatforms := registeredExecutionPlatforms.Message.ExecutionPlatforms
	var compatibleExecutionPlatforms []*model_analysis_pb.ExecutionPlatform
	for _, executionPlatform := range allExecutionPlatforms {
		if constraintsAreCompatible(executionPlatform.Constraints, key.Constraints) {
			compatibleExecutionPlatforms = append(compatibleExecutionPlatforms, executionPlatform)
		}
	}
	if len(compatibleExecutionPlatforms) == 0 {
		return PatchedCompatibleExecutionPlatformsValue{}, fmt.Errorf("none of the %d registered execution platforms are compatible with the provided constraints", len(allExecutionPlatforms))
	}

	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CompatibleExecutionPlatforms_Value{
		ExecutionPlatforms: compatibleExecutionPlatforms,
	}), nil
}
