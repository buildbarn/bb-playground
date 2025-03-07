package analysis

import (
	"context"
	"fmt"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
)

func (c *baseComputer[TReference, TMetadata]) ComputeFileReaderValue(ctx context.Context, key *model_analysis_pb.FileReader_Key, e FileReaderEnvironment[TReference]) (*model_filesystem.FileReader[TReference], error) {
	fileAccessParametersValue := e.GetFileAccessParametersValue(&model_analysis_pb.FileAccessParameters_Key{})
	if !fileAccessParametersValue.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	fileAccessParameters, err := model_filesystem.NewFileAccessParametersFromProto(
		fileAccessParametersValue.Message.FileAccessParameters,
		c.getReferenceFormat(),
	)
	if err != nil {
		return nil, fmt.Errorf("invalid directory access parameters: %w", err)
	}
	fileContentsListReader := model_parser.LookupParsedObjectReader(
		c.parsedObjectPoolIngester,
		model_parser.NewChainedObjectParser(
			model_parser.NewEncodedObjectParser[TReference](fileAccessParameters.GetFileContentsListEncoder()),
			model_filesystem.NewFileContentsListObjectParser[TReference](),
		),
	)
	fileChunkReader := model_parser.LookupParsedObjectReader(
		c.parsedObjectPoolIngester,
		model_parser.NewChainedObjectParser(
			model_parser.NewEncodedObjectParser[TReference](fileAccessParameters.GetChunkEncoder()),
			model_parser.NewRawObjectParser[TReference](),
		),
	)
	return model_filesystem.NewFileReader(fileContentsListReader, fileChunkReader), nil
}
