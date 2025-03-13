package analysis

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_encoding "github.com/buildbarn/bonanza/pkg/model/encoding"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
)

func (c *baseComputer[TReference, TMetadata]) ComputeCommandEncodersValue(ctx context.Context, key *model_analysis_pb.CommandEncoders_Key, e CommandEncodersEnvironment[TReference, TMetadata]) (PatchedCommandEncodersValue, error) {
	buildSpecification := e.GetBuildSpecificationValue(&model_analysis_pb.BuildSpecification_Key{})
	if !buildSpecification.IsSet() {
		return PatchedCommandEncodersValue{}, evaluation.ErrMissingDependency
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CommandEncoders_Value{
		CommandEncoders: buildSpecification.Message.BuildSpecification.GetCommandEncoders(),
	}), nil
}

func (c *baseComputer[TReference, TMetadata]) ComputeCommandEncoderObjectValue(ctx context.Context, key *model_analysis_pb.CommandEncoderObject_Key, e CommandEncoderObjectEnvironment[TReference, TMetadata]) (model_encoding.BinaryEncoder, error) {
	encoders := e.GetCommandEncodersValue(&model_analysis_pb.CommandEncoders_Key{})
	if !encoders.IsSet() {
		return nil, evaluation.ErrMissingDependency
	}
	return model_encoding.NewBinaryEncoderFromProto(
		encoders.Message.CommandEncoders,
		uint32(c.getReferenceFormat().GetMaximumObjectSizeBytes()),
	)
}
