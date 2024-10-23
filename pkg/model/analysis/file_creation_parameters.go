package analysis

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeFileCreationParametersValue(ctx context.Context, key *model_analysis_pb.FileCreationParameters_Key, e FileCreationParametersEnvironment) (PatchedFileCreationParametersValue, error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return PatchedFileCreationParametersValue{}, evaluation.ErrMissingDependency
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.FileCreationParameters_Value{
		FileCreationParameters: buildSpecification.Message.BuildSpecification.GetFileCreationParameters(),
	}), nil
}

func (c *baseComputer) ComputeFileCreationParametersObjectValue(ctx context.Context, key *model_analysis_pb.FileCreationParametersObject_Key, e FileCreationParametersObjectEnvironment) (*model_filesystem.FileCreationParameters, error) {
	fileCreationParameters := e.GetFileCreationParametersValue(&model_analysis_pb.FileCreationParameters_Key{})
	if !fileCreationParameters.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	return model_filesystem.NewFileCreationParametersFromProto(
		fileCreationParameters.Message.FileCreationParameters,
		c.buildSpecificationReference.GetReferenceFormat(),
	)
}
