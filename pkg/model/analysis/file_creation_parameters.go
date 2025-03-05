package analysis

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
)

func (c *baseComputer[TReference]) ComputeFileCreationParametersValue(ctx context.Context, key *model_analysis_pb.FileCreationParameters_Key, e FileCreationParametersEnvironment[TReference]) (PatchedFileCreationParametersValue, error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return PatchedFileCreationParametersValue{}, evaluation.ErrMissingDependency
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.FileCreationParameters_Value{
		FileCreationParameters: buildSpecification.Message.BuildSpecification.GetFileCreationParameters(),
	}), nil
}

func (c *baseComputer[TReference]) ComputeFileCreationParametersObjectValue(ctx context.Context, key *model_analysis_pb.FileCreationParametersObject_Key, e FileCreationParametersObjectEnvironment[TReference]) (*model_filesystem.FileCreationParameters, error) {
	fileCreationParameters := e.GetFileCreationParametersValue(&model_analysis_pb.FileCreationParameters_Key{})
	if !fileCreationParameters.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	return model_filesystem.NewFileCreationParametersFromProto(
		fileCreationParameters.Message.FileCreationParameters,
		c.getReferenceFormat(),
	)
}
