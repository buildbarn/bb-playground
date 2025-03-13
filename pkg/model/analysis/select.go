package analysis

import (
	"context"

	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
)

func (c *baseComputer[TReference, TMetadata]) ComputeSelectValue(ctx context.Context, key *model_analysis_pb.Select_Key, e SelectEnvironment[TReference, TMetadata]) (PatchedSelectValue, error) {
	// TODO: Provide a proper implementation.
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.Select_Value{}), nil
}
