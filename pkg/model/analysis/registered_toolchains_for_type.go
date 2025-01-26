package analysis

import (
	"context"
	"sort"
	"strings"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeRegisteredToolchainsForTypeValue(ctx context.Context, key *model_analysis_pb.RegisteredToolchainsForType_Key, e RegisteredToolchainsForTypeEnvironment) (PatchedRegisteredToolchainsForTypeValue, error) {
	registeredToolchainsValue := e.GetRegisteredToolchainsValue(&model_analysis_pb.RegisteredToolchains_Key{})
	if !registeredToolchainsValue.IsSet() {
		return PatchedRegisteredToolchainsForTypeValue{}, evaluation.ErrMissingDependency
	}

	registeredToolchains := registeredToolchainsValue.Message.ToolchainTypes
	if index, ok := sort.Find(
		len(registeredToolchains),
		func(i int) int { return strings.Compare(key.ToolchainType, registeredToolchains[i].ToolchainType) },
	); ok {
		// Found one or more toolchains for this type.
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredToolchainsForType_Value{
			Toolchains: registeredToolchains[index].Toolchains,
		}), nil
	}

	// No toolchains registered for this type.
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredToolchainsForType_Value{}), nil
}
