package filesystem

import (
	"github.com/buildbarn/bb-playground/pkg/model/encoding"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// DirectoryAccessParameters contains parameters that were used when
// creating Merkle trees of directories that should also be applied when
// attempting to access its contents afterwards. Parameters include
// whether files were compressed or encrypted.
type DirectoryAccessParameters struct {
	encoder encoding.BinaryEncoder
}

// NewDirectoryAccessParametersFromProto creates an instance of
// DirectoryAccessParameters that matches the configuration stored in a
// Protobuf message. This, for example, permits a server to access files
// that were uploaded by a client.
func NewDirectoryAccessParametersFromProto(m *model_filesystem_pb.DirectoryAccessParameters, referenceFormat object.ReferenceFormat) (*DirectoryAccessParameters, error) {
	if m == nil {
		return nil, status.Error(codes.InvalidArgument, "No directory access parameters provided")
	}

	maximumObjectSizeBytes := uint32(referenceFormat.GetMaximumObjectSizeBytes())
	encoder, err := encoding.NewBinaryEncoderFromProto(m.Encoders, maximumObjectSizeBytes)
	if err != nil {
		return nil, util.StatusWrap(err, "Invalid encoder")
	}

	return &DirectoryAccessParameters{
		encoder: encoder,
	}, nil
}

func (p *DirectoryAccessParameters) DecodeDirectory(contents *object.Contents) (*model_filesystem_pb.Directory, error) {
	decodedData, err := p.encoder.DecodeBinary(contents.GetPayload())
	if err != nil {
		return nil, err
	}
	var directory model_filesystem_pb.Directory
	if err := proto.Unmarshal(decodedData, &directory); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid Protobuf message")
	}
	return &directory, nil
}

func (p *DirectoryAccessParameters) DecodeLeaves(contents *object.Contents) (*model_filesystem_pb.Leaves, error) {
	decodedData, err := p.encoder.DecodeBinary(contents.GetPayload())
	if err != nil {
		return nil, err
	}
	var leaves model_filesystem_pb.Leaves
	if err := proto.Unmarshal(decodedData, &leaves); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid Protobuf message")
	}
	return &leaves, nil
}
