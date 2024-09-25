package virtual

import (
	"bytes"
	"context"
	"io"

	"github.com/buildbarn/bb-playground/pkg/encoding/varint"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	object_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type ObjectBackedFileFactory struct {
	handleAllocator virtual.ResolvableHandleAllocator
	fileReader      *model_filesystem.FileReader
	errorLogger     util.ErrorLogger
}

func NewObjectBackedFileFactory(handleAllocation virtual.ResolvableHandleAllocation, fileReader *model_filesystem.FileReader, errorLogger util.ErrorLogger) *ObjectBackedFileFactory {
	ff := &ObjectBackedFileFactory{
		fileReader:  fileReader,
		errorLogger: errorLogger,
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

	endBytes, err := varint.ReadForward[uint64](r)
	if err != nil {
		return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
	}

	b, err := r.ReadByte()
	if err != nil {
		return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
	}
	isExecutable := b != 0x00

	return virtual.DirectoryChild{}.FromLeaf(
		ff.LookupFile(
			model_filesystem.FileContentsEntry{
				EndBytes:  endBytes,
				Reference: reference,
			},
			isExecutable,
		),
	), virtual.StatusOK
}

func (ff *ObjectBackedFileFactory) LookupFile(fileContents model_filesystem.FileContentsEntry, isExecutable bool) virtual.Leaf {
	handle := varint.AppendForward(nil, fileContents.Reference.GetReferenceFormat().ToProto())
	handle = append(handle, fileContents.Reference.GetRawReference()...)
	handle = varint.AppendForward(handle, fileContents.EndBytes)
	if isExecutable {
		handle = append(handle, 0x01)
	} else {
		handle = append(handle, 0x00)
	}
	return ff.handleAllocator.New(bytes.NewBuffer(handle)).
		AsLeaf(&objectBackedFile{
			factory:      ff,
			fileContents: fileContents,
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
	buf, eof := virtual.BoundReadToFileSize(buf, offsetBytes, f.fileContents.EndBytes)
	ff := f.factory
	// TODO: Extend VirtualRead() to have a context.
	nRead, err := ff.fileReader.FileReadAt(context.Background(), f.fileContents, buf, offsetBytes)
	if err != nil {
		ff.errorLogger.Log(err)
		return nRead, false, virtual.StatusErrIO
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

func (objectBackedFile) VirtualApply(data any) bool {
	return false
}
