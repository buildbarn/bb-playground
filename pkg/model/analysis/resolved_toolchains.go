package analysis

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"

	"google.golang.org/protobuf/encoding/protojson"
)

func (c *baseComputer) ComputeResolvedToolchainsValue(ctx context.Context, key *model_analysis_pb.ResolvedToolchains_Key, e ResolvedToolchainsEnvironment) (PatchedResolvedToolchainsValue, error) {
	bla := e.GetRegisteredExecutionPlatformsValue(&model_analysis_pb.RegisteredExecutionPlatforms_Key{})
	if !bla.IsSet() {
		return PatchedResolvedToolchainsValue{}, evaluation.ErrMissingDependency
	}
	panic(protojson.Format(bla.Message))
}
