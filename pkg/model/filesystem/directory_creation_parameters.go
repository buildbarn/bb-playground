package filesystem

import (
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type DirectoryCreationParameters struct {
	*DirectoryAccessParameters
	referenceFormat           object.ReferenceFormat
	directoryMaximumSizeBytes int
}

func NewDirectoryCreationParametersFromProto(m *model_filesystem_pb.DirectoryCreationParameters, referenceFormat object.ReferenceFormat) (*DirectoryCreationParameters, error) {
	if m == nil {
		return nil, status.Error(codes.InvalidArgument, "No directory creation parameters provided")
	}

	accessParameters, err := NewDirectoryAccessParametersFromProto(m.Access, referenceFormat)
	if err != nil {
		return nil, err
	}

	maximumObjectSizeBytes := uint32(referenceFormat.GetMaximumObjectSizeBytes())
	if m.DirectoryMaximumSizeBytes > maximumObjectSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Maximum size of directories is above maximum object size of %d bytes", maximumObjectSizeBytes)
	}

	return &DirectoryCreationParameters{
		DirectoryAccessParameters: accessParameters,
		referenceFormat:           referenceFormat,
		directoryMaximumSizeBytes: int(m.DirectoryMaximumSizeBytes),
	}, nil
}

var marshalOptions = proto.MarshalOptions{UseCachedSize: true}

func (p *DirectoryCreationParameters) EncodeDirectory(references []object.LocalReference, m *model_filesystem_pb.Directory) (*object.Contents, error) {
	data, err := marshalOptions.Marshal(m)
	if err != nil {
		return nil, err
	}
	encodedData, err := p.encoder.EncodeBinary(data)
	if err != nil {
		return nil, err
	}
	return p.referenceFormat.NewContents(references, encodedData)
}
