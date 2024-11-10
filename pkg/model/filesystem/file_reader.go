package filesystem

import (
	"context"
	"io"

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

func (fr *FileReader) readNextChunk(ctx context.Context, fileContentsIterator *FileContentsIterator) ([]byte, error) {
	for {
		partReference, partOffsetBytes, partSizeBytes := fileContentsIterator.GetCurrentPart()
		if partReference.GetDegree() == 0 {
			// Reached a chunk.
			chunk, _, err := fr.fileChunkReader.ReadParsedObject(ctx, partReference)
			if err != nil {
				return nil, util.StatusWrapf(err, "Failed to read chunk with reference %s", partReference)
			}
			if uint64(len(chunk)) != partSizeBytes {
				return nil, status.Errorf(codes.InvalidArgument, "Chunk is %d bytes in size, while %d bytes were expected", len(chunk), partSizeBytes)
			}
			fileContentsIterator.ToNextPart()
			return chunk[partOffsetBytes:], nil
		}

		// We need to push one or more file contents lists onto
		// the stack to reach a chunk.
		fileContentsList, _, err := fr.fileContentsListReader.ReadParsedObject(ctx, partReference)
		if err != nil {
			return nil, util.StatusWrapf(err, "Failed to read file contents list with reference %s", partReference)
		}
		if err := fileContentsIterator.PushFileContentsList(fileContentsList); err != nil {
			return nil, util.StatusWrapf(err, "Invalid file contents list with reference %s", partReference)
		}
	}
}

func (fr *FileReader) FileReadAt(ctx context.Context, fileContents FileContentsEntry, p []byte, offsetBytes uint64) (int, error) {
	// TODO: Any chance we can use parallelism here to read multiple chunks?
	fileContentsIterator := NewFileContentsIterator(fileContents, offsetBytes)
	nRead := 0
	for len(p) > 0 {
		chunk, err := fr.readNextChunk(ctx, &fileContentsIterator)
		if err != nil {
			return nRead, err
		}
		n := copy(p, chunk)
		p = p[n:]
		nRead += n
	}
	return nRead, nil
}

func (fr *FileReader) FileOpenRead(ctx context.Context, fileContents FileContentsEntry, offsetBytes uint64) io.Reader {
	return &sequentialFileReader{
		context:              ctx,
		fileReader:           fr,
		fileContentsIterator: NewFileContentsIterator(fileContents, offsetBytes),
		offsetBytes:          offsetBytes,
		sizeBytes:            fileContents.EndBytes,
	}
}

func (fr *FileReader) FileOpenReadAt(ctx context.Context, fileContents FileContentsEntry) io.ReaderAt {
	return &randomAccessFileReader{
		context:      ctx,
		fileReader:   fr,
		fileContents: fileContents,
	}
}

type sequentialFileReader struct {
	context              context.Context
	fileReader           *FileReader
	fileContentsIterator FileContentsIterator
	chunk                []byte
	offsetBytes          uint64
	sizeBytes            uint64
}

func (r *sequentialFileReader) Read(p []byte) (int, error) {
	nRead := 0
	for {
		// Copy data from a previously read chunk.
		n := copy(p, r.chunk)
		p = p[n:]
		r.chunk = r.chunk[n:]
		nRead += n
		if len(p) == 0 {
			return nRead, nil
		}

		// Read the next chunk if we're not at end of file.
		if r.offsetBytes >= r.sizeBytes {
			return nRead, io.EOF
		}
		chunk, err := r.fileReader.readNextChunk(r.context, &r.fileContentsIterator)
		if err != nil {
			return nRead, err
		}
		r.chunk = chunk
		r.offsetBytes += uint64(len(chunk))
	}
}

type randomAccessFileReader struct {
	context      context.Context
	fileReader   *FileReader
	fileContents FileContentsEntry
}

func (r *randomAccessFileReader) ReadAt(p []byte, offsetBytes int64) (int, error) {
	// Limit the read operation to the size of the file.
	if uint64(offsetBytes) > r.fileContents.EndBytes {
		return 0, io.EOF
	}
	remainingBytes := r.fileContents.EndBytes - uint64(offsetBytes)
	if uint64(len(p)) > remainingBytes {
		p = p[:remainingBytes]
	}

	return r.fileReader.FileReadAt(r.context, r.fileContents, p, uint64(offsetBytes))
}
