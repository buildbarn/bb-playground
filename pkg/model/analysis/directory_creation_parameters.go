package analysis

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
)

func (c *baseComputer[TReference, TMetadata]) ComputeDirectoryCreationParametersValue(ctx context.Context, key *model_analysis_pb.DirectoryCreationParameters_Key, e DirectoryCreationParametersEnvironment[TReference]) (PatchedDirectoryCreationParametersValue, error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return PatchedDirectoryCreationParametersValue{}, evaluation.ErrMissingDependency
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.DirectoryCreationParameters_Value{
		DirectoryCreationParameters: buildSpecification.Message.BuildSpecification.GetDirectoryCreationParameters(),
	}), nil
}

func (c *baseComputer[TReference, TMetadata]) ComputeDirectoryCreationParametersObjectValue(ctx context.Context, key *model_analysis_pb.DirectoryCreationParametersObject_Key, e DirectoryCreationParametersObjectEnvironment[TReference]) (*model_filesystem.DirectoryCreationParameters, error) {
	directoryCreationParameters := e.GetDirectoryCreationParametersValue(&model_analysis_pb.DirectoryCreationParameters_Key{})
	if !directoryCreationParameters.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	return model_filesystem.NewDirectoryCreationParametersFromProto(
		directoryCreationParameters.Message.DirectoryCreationParameters,
		c.getReferenceFormat(),
	)
}
