package filesystem

import (
	"context"
	"math"

	"github.com/buildbarn/bb-storage/pkg/util"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/parser"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	reference, err := fileContents.GetOutgoingReference(fileContents.Message.Reference)
	if err != nil {
		return FileContentsEntry{}, err
	}
	return FileContentsEntry{
		EndBytes:  fileContents.Message.TotalSizeBytes,
		Reference: reference,
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
	l, sizeBytes, err := model_parser.NewMessageListObjectParser[TReference, model_filesystem_pb.FileContents]().
		ParseObject(ctx, reference, outgoingReferences, data)
	if err != nil {
		return nil, 0, err
	}
	if len(l.Message) < 2 {
		return nil, 0, status.Error(codes.InvalidArgument, "File contents list contains fewer than two parts")
	}

	var endBytes uint64
	fileContentsList := make(FileContentsList, 0, len(l.Message))
	for i, part := range l.Message {
		// Convert 'total_size_bytes' to a cumulative value, to
		// allow FileContentsIterator to perform binary searching.
		if part.TotalSizeBytes < 1 {
			return nil, 0, status.Errorf(codes.InvalidArgument, "Part at index %d does not contain any data", i)
		}
		if part.TotalSizeBytes > math.MaxUint64-endBytes {
			return nil, 0, status.Errorf(codes.InvalidArgument, "Combined size of all parts exceeds maximum file size of %d bytes", uint64(math.MaxUint64))
		}
		endBytes += part.TotalSizeBytes

		partReference, err := l.GetOutgoingReference(part.Reference)
		if err != nil {
			return nil, 0, util.StatusWrapf(err, "Invalid reference for part at index %d", i)
		}

		fileContentsList = append(fileContentsList, FileContentsEntry{
			EndBytes:  endBytes,
			Reference: partReference,
		})
	}
	return fileContentsList, sizeBytes, nil
}
