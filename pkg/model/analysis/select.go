package analysis

import (
	"context"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeSelectValue(ctx context.Context, key *model_analysis_pb.Select_Key, e SelectEnvironment) (PatchedSelectValue, error) {
	// TODO: Provide a proper implementation.
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Select_Value{}), nil
}
