package filesystem

import (
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FileCreationParameters struct {
	*FileAccessParameters
	referenceFormat                  object.ReferenceFormat
	chunkMinimumSizeBytes            int
	chunkMaximumSizeBytes            int
	fileContentsListMinimumSizeBytes int
	fileContentsListMaximumSizeBytes int
}

func NewFileCreationParametersFromProto(m *model_filesystem_pb.FileCreationParameters, referenceFormat object.ReferenceFormat) (*FileCreationParameters, error) {
	if m == nil {
		return nil, status.Error(codes.InvalidArgument, "No file creation parameters provided")
	}

	accessParameters, err := NewFileAccessParametersFromProto(m.Access, referenceFormat)
	if err != nil {
		return nil, err
	}

	// Ensure that the provided object size limits are within
	// bounds. Prevent creating objects that are tiny, as that
	// increases memory usage and running times of the content
	// defined chunking and B-tree algorithms.
	maximumObjectSizeBytes := uint32(referenceFormat.GetMaximumObjectSizeBytes())
	if limit := uint32(1024); m.ChunkMinimumSizeBytes < limit {
		return nil, status.Errorf(codes.InvalidArgument, "Minimum size of chunks is below %d bytes", limit)
	}
	if m.ChunkMaximumSizeBytes > maximumObjectSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Maximum size of chunks is above maximum object size of %d bytes", maximumObjectSizeBytes)
	}
	if m.ChunkMaximumSizeBytes < 2*m.ChunkMinimumSizeBytes {
		return nil, status.Error(codes.InvalidArgument, "Maximum size of chunks must be at least twice as large as the minimum")
	}

	if limit := uint32(1024); m.FileContentsListMinimumSizeBytes < limit {
		return nil, status.Errorf(codes.InvalidArgument, "Minimum size of file contents list is below %d bytes", limit)
	}
	if m.FileContentsListMaximumSizeBytes > maximumObjectSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Maximum size of file contents list is above maximum object size of %d bytes", maximumObjectSizeBytes)
	}
	if m.FileContentsListMaximumSizeBytes < m.FileContentsListMinimumSizeBytes {
		return nil, status.Error(codes.InvalidArgument, "Maximum size of file contents list must be at least as large as the minimum")
	}

	return &FileCreationParameters{
		FileAccessParameters:             accessParameters,
		referenceFormat:                  referenceFormat,
		chunkMinimumSizeBytes:            int(m.ChunkMinimumSizeBytes),
		chunkMaximumSizeBytes:            int(m.ChunkMaximumSizeBytes),
		fileContentsListMinimumSizeBytes: int(m.FileContentsListMinimumSizeBytes),
		fileContentsListMaximumSizeBytes: int(m.FileContentsListMaximumSizeBytes),
	}, nil
}

func (p *FileCreationParameters) EncodeChunk(data []byte) (*object.Contents, error) {
	encodedChunk, err := p.chunkEncoder.EncodeBinary(data)
	if err != nil {
		return nil, err
	}
	return p.referenceFormat.NewContents(nil, encodedChunk)
}
