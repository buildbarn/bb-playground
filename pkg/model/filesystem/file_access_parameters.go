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

// FileAccessParameters contains parameters that were used when creating
// Merkle trees of files that should also be applied when attempting to
// access its contents afterwards. Parameters include whether files were
// compressed or encrypted.
type FileAccessParameters struct {
	chunkEncoder            encoding.BinaryEncoder
	fileContentsListEncoder encoding.BinaryEncoder
}

// NewFileAccessParametersFromProto creates an instance of
// FileAccessParameters that matches the configuration stored in a
// Protobuf message. This, for example, permits a server to access files
// that were uploaded by a client.
func NewFileAccessParametersFromProto(m *model_filesystem_pb.FileAccessParameters, referenceFormat object.ReferenceFormat) (*FileAccessParameters, error) {
	if m == nil {
		return nil, status.Error(codes.InvalidArgument, "No file access parameters provided")
	}

	maximumObjectSizeBytes := uint32(referenceFormat.GetMaximumObjectSizeBytes())
	chunkEncoder, err := encoding.NewBinaryEncoderFromProto(m.ChunkEncoders, maximumObjectSizeBytes)
	if err != nil {
		return nil, util.StatusWrap(err, "Invalid chunk encoder")
	}
	fileContentsListEncoder, err := encoding.NewBinaryEncoderFromProto(m.FileContentsListEncoders, maximumObjectSizeBytes)
	if err != nil {
		return nil, util.StatusWrap(err, "Invalid file contents list encoder")
	}

	return &FileAccessParameters{
		chunkEncoder:            chunkEncoder,
		fileContentsListEncoder: fileContentsListEncoder,
	}, nil
}

// DecodeFileContentsList extracts the FileContentsList that is stored
// in an object backed by storage.
func (p *FileAccessParameters) DecodeFileContentsList(contents *object.Contents) (*model_filesystem_pb.FileContentsList, error) {
	decodedData, err := p.fileContentsListEncoder.DecodeBinary(contents.GetPayload())
	if err != nil {
		return nil, err
	}
	var fileContentsList model_filesystem_pb.FileContentsList
	if err := proto.Unmarshal(decodedData, &fileContentsList); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid Protobuf message")
	}
	return &fileContentsList, nil
}

func (p *FileAccessParameters) GetChunkEncoder() encoding.BinaryEncoder {
	return p.chunkEncoder
}

func (p *FileAccessParameters) GetFileContentsListEncoder() encoding.BinaryEncoder {
	return p.fileContentsListEncoder
}
