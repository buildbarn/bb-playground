package analysis

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/dereference"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_command_pb "github.com/buildbarn/bonanza/pkg/proto/model/command"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

// DirectoryDereferencers contains Dereferencers that can be used to
// follow references to objects that are encoded using the directory
// access parameters that are part of the BuildSpecification.
type DirectoryDereferencers struct {
	Directory      dereference.Dereferencer[object.OutgoingReferences, model_core.Message[*model_filesystem_pb.Directory, object.OutgoingReferences]]
	Leaves         dereference.Dereferencer[object.OutgoingReferences, model_core.Message[*model_filesystem_pb.Leaves, object.OutgoingReferences]]
	CommandOutputs dereference.Dereferencer[object.OutgoingReferences, model_core.Message[*model_command_pb.Outputs, object.OutgoingReferences]]
}

func (c *baseComputer) ComputeDirectoryDereferencersValue(ctx context.Context, key *model_analysis_pb.DirectoryDereferencers_Key, e DirectoryDereferencersEnvironment) (*DirectoryDereferencers, error) {
	directoryAccessParametersValue := e.GetDirectoryAccessParametersValue(&model_analysis_pb.DirectoryAccessParameters_Key{})
	if !directoryAccessParametersValue.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	directoryAccessParameters, err := model_filesystem.NewDirectoryAccessParametersFromProto(
		directoryAccessParametersValue.Message.DirectoryAccessParameters,
		c.buildSpecificationReference.GetReferenceFormat(),
	)
	if err != nil {
		return nil, err
	}

	return &DirectoryDereferencers{
		Directory: dereference.NewReadingDereferencer(
			model_parser.NewStorageBackedParsedObjectReader(
				c.objectDownloader,
				directoryAccessParameters.GetEncoder(),
				model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Directory](),
			),
		),
		Leaves: dereference.NewReadingDereferencer(
			model_parser.NewStorageBackedParsedObjectReader(
				c.objectDownloader,
				directoryAccessParameters.GetEncoder(),
				model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Leaves](),
			),
		),
		CommandOutputs: dereference.NewReadingDereferencer(
			model_parser.NewStorageBackedParsedObjectReader(
				c.objectDownloader,
				directoryAccessParameters.GetEncoder(),
				model_parser.NewMessageObjectParser[object.LocalReference, model_command_pb.Outputs](),
			),
		),
	}, nil
}
