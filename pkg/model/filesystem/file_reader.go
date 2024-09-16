package filesystem

import (
	"context"

	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FileReader struct {
	fileContentsListReader model_parser.ParsedObjectReader[object.LocalReference, FileContentsList]
	fileChunkReader        model_parser.ParsedObjectReader[object.LocalReference, []byte]
}

func NewFileReader(
	fileContentsListReader model_parser.ParsedObjectReader[object.LocalReference, FileContentsList],
	fileChunkReader model_parser.ParsedObjectReader[object.LocalReference, []byte],
) *FileReader {
	return &FileReader{
		fileContentsListReader: fileContentsListReader,
		fileChunkReader:        fileChunkReader,
	}
}

func (fr *FileReader) FileReadAll(ctx context.Context, fileContents FileContentsEntry, maximumSizeBytes uint64) ([]byte, error) {
	if fileContents.EndBytes > maximumSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "File is %d bytes in size, which exceeds the permitted maximum of %d bytes", fileContents.EndBytes, maximumSizeBytes)
	}
	p := make([]byte, fileContents.EndBytes)
	if _, err := fr.FileReadAt(ctx, fileContents, p, 0); err != nil {
		return nil, err
	}
	return p, nil
}

func (fr *FileReader) FileReadAt(ctx context.Context, fileContents FileContentsEntry, p []byte, offsetBytes uint64) (int, error) {
	// TODO: Any chance we can use parallelism here to read multiple chunks?
	fileContentsIterator := NewFileContentsIterator(fileContents, offsetBytes)
	nRead := 0
	for len(p) > 0 {
		partReference, partOffsetBytes, partSizeBytes := fileContentsIterator.GetCurrentPart()
		if partReference.GetDegree() > 0 {
			fileContentsList, _, err := fr.fileContentsListReader.ReadParsedObject(ctx, partReference)
			if err != nil {
				return nRead, util.StatusWrapf(err, "Failed to read file contents list with reference %s", partReference)
			}
			if err := fileContentsIterator.PushFileContentsList(fileContentsList); err != nil {
				return nRead, util.StatusWrapf(err, "Invalid file contents list with reference %s", partReference)
			}
		} else {
			chunk, _, err := fr.fileChunkReader.ReadParsedObject(ctx, partReference)
			if err != nil {
				return nRead, util.StatusWrapf(err, "Failed to read chunk with reference %s", partReference)
			}
			if uint64(len(chunk)) != partSizeBytes {
				return nRead, status.Errorf(codes.InvalidArgument, "Chunk is %d bytes in size, while %d bytes were expected", len(chunk), partSizeBytes)
			}
			n := copy(p, chunk[partOffsetBytes:])
			p = p[n:]
			nRead += n
			fileContentsIterator.ToNextPart()
		}
	}
	return nRead, nil
}
