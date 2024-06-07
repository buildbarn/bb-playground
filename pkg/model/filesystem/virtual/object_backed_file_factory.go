package virtual

import (
	"bytes"
	"context"
	"io"

	"github.com/buildbarn/bb-playground/pkg/encoding/varint"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	object_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ObjectBackedFileFactory struct {
	handleAllocator        virtual.ResolvableHandleAllocator
	fileContentsListReader model_parser.ParsedObjectReader[object.LocalReference, model_filesystem.FileContentsList]
	fileChunkReader        model_parser.ParsedObjectReader[object.LocalReference, []byte]
	errorLogger            util.ErrorLogger
}

func NewObjectBackedFileFactory(handleAllocation virtual.ResolvableHandleAllocation, fileContentsListReader model_parser.ParsedObjectReader[object.LocalReference, model_filesystem.FileContentsList], fileChunkReader model_parser.ParsedObjectReader[object.LocalReference, []byte], errorLogger util.ErrorLogger) *ObjectBackedFileFactory {
	ff := &ObjectBackedFileFactory{
		fileContentsListReader: fileContentsListReader,
		fileChunkReader:        fileChunkReader,
		errorLogger:            errorLogger,
	}
	ff.handleAllocator = handleAllocation.AsResolvableAllocator(ff.resolveHandle)
	return ff
}

func (ff *ObjectBackedFileFactory) resolveHandle(r io.ByteReader) (virtual.DirectoryChild, virtual.Status) {
	referenceFormatValue, err := varint.ReadForward[object_pb.ReferenceFormat_Value](r)
	if err != nil {
		return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
	}
	referenceFormat, err := object.NewReferenceFormat(referenceFormatValue)
	if err != nil {
		return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
	}
	referenceSizeBytes := referenceFormat.GetReferenceSizeBytes()
	rawReference := make([]byte, 0, referenceSizeBytes)
	for i := 0; i < referenceSizeBytes; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
		}
		rawReference = append(rawReference, b)
	}
	reference, err := referenceFormat.NewLocalReference(rawReference)
	if err != nil {
		return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
	}

	totalSizeBytes, err := varint.ReadForward[uint64](r)
	if err != nil {
		return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
	}

	b, err := r.ReadByte()
	if err != nil {
		return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
	}
	isExecutable := b != 0x00

	return virtual.DirectoryChild{}.FromLeaf(ff.LookupFile(reference, totalSizeBytes, isExecutable)), virtual.StatusOK
}

func (ff *ObjectBackedFileFactory) LookupFile(reference object.LocalReference, totalSizeBytes uint64, isExecutable bool) virtual.Leaf {
	handle := varint.AppendForward(nil, reference.GetReferenceFormat().ToProto())
	handle = append(handle, reference.GetRawReference()...)
	handle = varint.AppendForward(handle, totalSizeBytes)
	if isExecutable {
		handle = append(handle, 0x01)
	} else {
		handle = append(handle, 0x00)
	}
	return ff.handleAllocator.New(bytes.NewBuffer(handle)).
		AsLeaf(&objectBackedFile{
			factory: ff,
			fileContents: model_filesystem.FileContentsEntry{
				EndBytes:  totalSizeBytes,
				Reference: reference,
			},
			isExecutable: isExecutable,
		})
}

type objectBackedFile struct {
	factory      *ObjectBackedFileFactory
	fileContents model_filesystem.FileContentsEntry
	isExecutable bool
}

func (f *objectBackedFile) VirtualAllocate(off, size uint64) virtual.Status {
	return virtual.StatusErrWrongType
}

func (f *objectBackedFile) VirtualClose(shareAccess virtual.ShareMask) {}

func (f *objectBackedFile) VirtualGetAttributes(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
	attributes.SetChangeID(0)
	attributes.SetFileType(filesystem.FileTypeRegularFile)
	permissions := virtual.PermissionsRead
	if f.isExecutable {
		permissions |= virtual.PermissionsWrite
	}
	attributes.SetPermissions(permissions)
	attributes.SetSizeBytes(f.fileContents.EndBytes)
}

func (f *objectBackedFile) VirtualOpenSelf(ctx context.Context, shareAccess virtual.ShareMask, options *virtual.OpenExistingOptions, requested virtual.AttributesMask, attributes *virtual.Attributes) virtual.Status {
	if shareAccess&^virtual.ShareMaskRead != 0 || options.Truncate {
		return virtual.StatusErrAccess
	}
	f.VirtualGetAttributes(ctx, requested, attributes)
	return virtual.StatusOK
}

func (f *objectBackedFile) VirtualRead(buf []byte, offsetBytes uint64) (int, bool, virtual.Status) {
	// TODO: Extend VirtualRead() to have a context.
	ctx := context.Background()
	buf, eof := virtual.BoundReadToFileSize(buf, offsetBytes, f.fileContents.EndBytes)
	nRead := 0
	ff := f.factory
	fileContentsIterator := model_filesystem.NewFileContentsIterator(f.fileContents, offsetBytes)
	for len(buf) > 0 {
		partReference, partOffsetBytes, partSizeBytes := fileContentsIterator.GetCurrentPart()
		if partReference.GetDegree() > 0 {
			fileContentsList, _, err := ff.fileContentsListReader.ReadParsedObject(ctx, partReference)
			if err != nil {
				ff.errorLogger.Log(util.StatusWrapf(err, "Failed to read file contents list with reference %s", partReference))
				return nRead, false, virtual.StatusErrIO
			}
			if err := fileContentsIterator.PushFileContentsList(fileContentsList); err != nil {
				ff.errorLogger.Log(util.StatusWrapf(err, "Invalid file contents list with reference %s", partReference))
				return nRead, false, virtual.StatusErrIO
			}
		} else {
			chunk, _, err := ff.fileChunkReader.ReadParsedObject(ctx, partReference)
			if err != nil {
				ff.errorLogger.Log(util.StatusWrapf(err, "Failed to read chunk with reference %s", partReference))
				return nRead, false, virtual.StatusErrIO
			}
			if uint64(len(chunk)) != partSizeBytes {
				ff.errorLogger.Log(status.Errorf(codes.InvalidArgument, "Chunk is %d bytes in size, while %d bytes were expected", len(chunk), partSizeBytes))
				return nRead, false, virtual.StatusErrIO
			}
			n := copy(buf, chunk[partOffsetBytes:])
			buf = buf[n:]
			nRead += n
			fileContentsIterator.ToNextPart()
		}
	}
	return nRead, eof, virtual.StatusOK
}

func (f *objectBackedFile) VirtualReadlink(ctx context.Context) ([]byte, virtual.Status) {
	return nil, virtual.StatusErrInval
}

func (f *objectBackedFile) VirtualSeek(offset uint64, regionType filesystem.RegionType) (*uint64, virtual.Status) {
	switch regionType {
	case filesystem.Data:
		if offset >= f.fileContents.EndBytes {
			return nil, virtual.StatusErrNXIO
		}
		return &offset, virtual.StatusOK
	case filesystem.Hole:
		if offset >= f.fileContents.EndBytes {
			return nil, virtual.StatusErrNXIO
		}
		return &f.fileContents.EndBytes, virtual.StatusOK
	default:
		panic("requests for other seek modes should have been intercepted")
	}
}

func (f *objectBackedFile) VirtualSetAttributes(ctx context.Context, in *virtual.Attributes, requested virtual.AttributesMask, out *virtual.Attributes) virtual.Status {
	if _, ok := in.GetPermissions(); ok {
		return virtual.StatusErrPerm
	}
	if _, ok := in.GetSizeBytes(); ok {
		return virtual.StatusErrAccess
	}
	f.VirtualGetAttributes(ctx, requested, out)
	return virtual.StatusOK
}

func (f *objectBackedFile) VirtualWrite(buf []byte, offset uint64) (int, virtual.Status) {
	panic("request to write to read-only file should have been intercepted")
}
