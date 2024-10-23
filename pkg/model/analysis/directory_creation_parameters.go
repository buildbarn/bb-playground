package analysis

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeDirectoryCreationParametersValue(ctx context.Context, key *model_analysis_pb.DirectoryCreationParameters_Key, e DirectoryCreationParametersEnvironment) (PatchedDirectoryCreationParametersValue, error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return PatchedDirectoryCreationParametersValue{}, evaluation.ErrMissingDependency
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.DirectoryCreationParameters_Value{
		DirectoryCreationParameters: buildSpecification.Message.BuildSpecification.GetDirectoryCreationParameters(),
	}), nil
}

func (c *baseComputer) ComputeDirectoryCreationParametersObjectValue(ctx context.Context, key *model_analysis_pb.DirectoryCreationParametersObject_Key, e DirectoryCreationParametersObjectEnvironment) (*model_filesystem.DirectoryCreationParameters, error) {
	directoryCreationParameters := e.GetDirectoryCreationParametersValue(&model_analysis_pb.DirectoryCreationParameters_Key{})
	if !directoryCreationParameters.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	return model_filesystem.NewDirectoryCreationParametersFromProto(
		directoryCreationParameters.Message.DirectoryCreationParameters,
		c.buildSpecificationReference.GetReferenceFormat(),
	)
}
