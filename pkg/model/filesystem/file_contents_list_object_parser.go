package filesystem

import (
	"context"
	"math"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/parser"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// FileContentsEntry contains the properties of a part of a concatenated file.
type FileContentsEntry struct {
	EndBytes  uint64
	Reference object.LocalReference
}

// NewFileContentsEntryFromProto constructs a FileContentsListEntry
// based on the contents of a single FileContents Protobuf message,
// refering to the file as a whole.
func NewFileContentsEntryFromProto(fileContents model_core.Message[*model_filesystem_pb.FileContents], referenceFormat object.ReferenceFormat) (FileContentsEntry, error) {
	if fileContents.Message == nil {
		// File is empty, meaning that it is not backed by any
		// object. Map all of such files to the bogus reference.
		// We assume that specifying a size of zero is enough to
		// prevent the underlying file implementation from
		// loading any objects from storage.
		return FileContentsEntry{
			EndBytes:  0,
			Reference: referenceFormat.GetBogusReference(),
		}, nil
	}

	index, err := model_core.GetIndexFromReferenceMessage(
		fileContents.Message.Reference,
		fileContents.OutgoingReferences.GetDegree(),
	)
	if err != nil {
		return FileContentsEntry{}, err
	}
	return FileContentsEntry{
		EndBytes:  fileContents.Message.TotalSizeBytes,
		Reference: fileContents.OutgoingReferences.GetOutgoingReference(index),
	}, nil
}

// FileContentsList contains the properties of parts of a concatenated
// file. Parts are stored in the order in which they should be
// concatenated, with EndBytes increasing.
type FileContentsList []FileContentsEntry

// FileContentsListObjectParserReference is a constraint on the
// reference types accepted by the ObjectParser returned by
// NewFileContentsListObjectParser.
type FileContentsListObjectParserReference[T any] interface {
	GetSizeBytes() int
}

type fileContentsListObjectParser[TReference FileContentsListObjectParserReference[TReference]] struct{}

// NewFileContentsListObjectParser creates an ObjectParser that is
// capable of parsing FileContentsList messages, turning them into a
// list of entries that can be processed by FileContentsIterator.
func NewFileContentsListObjectParser[TReference FileContentsListObjectParserReference[TReference]]() parser.ObjectParser[TReference, FileContentsList] {
	return &fileContentsListObjectParser[TReference]{}
}

func (p *fileContentsListObjectParser[TReference]) ParseObject(ctx context.Context, reference TReference, outgoingReferences object.OutgoingReferences, data []byte) (FileContentsList, int, error) {
	var l model_filesystem_pb.FileContentsList
	if err := proto.Unmarshal(data, &l); err != nil {
		return nil, 0, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to parse file contents list")
	}
	if len(l.Parts) < 2 {
		return nil, 0, status.Error(codes.InvalidArgument, "File contents list contains fewer than two parts")
	}

	var endBytes uint64
	degree := outgoingReferences.GetDegree()
	fileContentsList := make(FileContentsList, 0, len(l.Parts))
	for i, part := range l.Parts {
		// Convert 'total_size_bytes' to a cumulative value, to
		// allow FileContentsIterator to perform binary searching.
		if part.TotalSizeBytes < 1 {
			return nil, 0, status.Errorf(codes.InvalidArgument, "Part at index %d does not contain any data", i)
		}
		if part.TotalSizeBytes > math.MaxUint64-endBytes {
			return nil, 0, status.Errorf(codes.InvalidArgument, "Combined size of all parts exceeds maximum file size of %d bytes", uint64(math.MaxUint64))
		}
		endBytes += part.TotalSizeBytes

		partReferenceIndex, err := model_core.GetIndexFromReferenceMessage(part.Reference, degree)
		if err != nil {
			return nil, 0, util.StatusWrapf(err, "Invalid reference index for part at index %d", i)
		}

		fileContentsList = append(fileContentsList, FileContentsEntry{
			EndBytes:  endBytes,
			Reference: outgoingReferences.GetOutgoingReference(partReferenceIndex),
		})
	}
	return fileContentsList, reference.GetSizeBytes(), nil
}
