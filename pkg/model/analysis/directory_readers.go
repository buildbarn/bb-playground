package analysis

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_command_pb "github.com/buildbarn/bonanza/pkg/proto/model/command"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

// DirectoryReaders contains ParsedObjectReaders that can be used to
// follow references to objects that are encoded using the directory
// access parameters that are part of the BuildSpecification.
type DirectoryReaders struct {
	Directory      model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_filesystem_pb.Directory, object.OutgoingReferences[object.LocalReference]]]
	Leaves         model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_filesystem_pb.Leaves, object.OutgoingReferences[object.LocalReference]]]
	CommandOutputs model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_command_pb.Outputs, object.OutgoingReferences[object.LocalReference]]]
}

func (c *baseComputer) ComputeDirectoryReadersValue(ctx context.Context, key *model_analysis_pb.DirectoryReaders_Key, e DirectoryReadersEnvironment) (*DirectoryReaders, error) {
	directoryAccessParametersValue := e.GetDirectoryAccessParametersValue(&model_analysis_pb.DirectoryAccessParameters_Key{})
	if !directoryAccessParametersValue.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	directoryAccessParameters, err := model_filesystem.NewDirectoryAccessParametersFromProto(
		directoryAccessParametersValue.Message.DirectoryAccessParameters,
		c.getReferenceFormat(),
	)
	if err != nil {
		return nil, err
	}

	return &DirectoryReaders{
		Directory: model_parser.NewStorageBackedParsedObjectReader(
			c.objectDownloader,
			directoryAccessParameters.GetEncoder(),
			model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Directory](),
		),
		Leaves: model_parser.NewStorageBackedParsedObjectReader(
			c.objectDownloader,
			directoryAccessParameters.GetEncoder(),
			model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Leaves](),
		),
		CommandOutputs: model_parser.NewStorageBackedParsedObjectReader(
			c.objectDownloader,
			directoryAccessParameters.GetEncoder(),
			model_parser.NewMessageObjectParser[object.LocalReference, model_command_pb.Outputs](),
		),
	}, nil
}
