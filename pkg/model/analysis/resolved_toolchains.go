package analysis

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"

	"google.golang.org/protobuf/encoding/protojson"
)

func (c *baseComputer) ComputeResolvedToolchainsValue(ctx context.Context, key *model_analysis_pb.ResolvedToolchains_Key, e ResolvedToolchainsEnvironment) (PatchedResolvedToolchainsValue, error) {
	registeredExecutionPlatforms := e.GetRegisteredExecutionPlatformsValue(&model_analysis_pb.RegisteredExecutionPlatforms_Key{})
	registeredToolchains := e.GetRegisteredToolchainsValue(&model_analysis_pb.RegisteredToolchains_Key{})
	if !registeredExecutionPlatforms.IsSet() || !registeredToolchains.IsSet() {
		return PatchedResolvedToolchainsValue{}, evaluation.ErrMissingDependency
	}

	panic(protojson.Format(registeredExecutionPlatforms.Message))
}
