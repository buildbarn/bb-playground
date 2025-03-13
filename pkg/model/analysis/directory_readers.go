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
)

// DirectoryReaders contains ParsedObjectReaders that can be used to
// follow references to objects that are encoded using the directory
// access parameters that are part of the BuildSpecification.
type DirectoryReaders[TReference any] struct {
	Directory      model_parser.ParsedObjectReader[TReference, model_core.Message[*model_filesystem_pb.Directory, TReference]]
	Leaves         model_parser.ParsedObjectReader[TReference, model_core.Message[*model_filesystem_pb.Leaves, TReference]]
	CommandOutputs model_parser.ParsedObjectReader[TReference, model_core.Message[*model_command_pb.Outputs, TReference]]
}

func (c *baseComputer[TReference, TMetadata]) ComputeDirectoryReadersValue(ctx context.Context, key *model_analysis_pb.DirectoryReaders_Key, e DirectoryReadersEnvironment[TReference, TMetadata]) (*DirectoryReaders[TReference], error) {
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

	encoderObjectParser := model_parser.NewEncodedObjectParser[TReference](directoryAccessParameters.GetEncoder())
	return &DirectoryReaders[TReference]{
		Directory: model_parser.LookupParsedObjectReader(
			c.parsedObjectPoolIngester,
			model_parser.NewChainedObjectParser(
				encoderObjectParser,
				model_parser.NewMessageObjectParser[TReference, model_filesystem_pb.Directory](),
			),
		),
		Leaves: model_parser.LookupParsedObjectReader(
			c.parsedObjectPoolIngester,
			model_parser.NewChainedObjectParser(
				encoderObjectParser,
				model_parser.NewMessageObjectParser[TReference, model_filesystem_pb.Leaves](),
			),
		),
		CommandOutputs: model_parser.LookupParsedObjectReader(
			c.parsedObjectPoolIngester,
			model_parser.NewChainedObjectParser(
				encoderObjectParser,
				model_parser.NewMessageObjectParser[TReference, model_command_pb.Outputs](),
			),
		),
	}, nil
}
