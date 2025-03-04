package virtual

import (
	"bytes"
	"io"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bonanza/pkg/encoding/varint"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	object_pb "github.com/buildbarn/bonanza/pkg/proto/storage/object"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

type resolvableHandleAllocatingFileFactory struct {
	base            FileFactory
	handleAllocator virtual.ResolvableHandleAllocator
}

func NewResolvableHandleAllocatingFileFactory(base FileFactory, handleAllocation virtual.ResolvableHandleAllocation) FileFactory {
	ff := &resolvableHandleAllocatingFileFactory{
		base: base,
	}
	ff.handleAllocator = handleAllocation.AsResolvableAllocator(ff.resolveHandle)
	return ff
}

func (ff *resolvableHandleAllocatingFileFactory) resolveHandle(r io.ByteReader) (virtual.DirectoryChild, virtual.Status) {
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
			model_filesystem.FileContentsEntry[object.LocalReference]{
				EndBytes:  endBytes,
				Reference: reference,
			},
			isExecutable,
		),
	), virtual.StatusOK
}

func computeFileID(fileContents model_filesystem.FileContentsEntry[object.LocalReference], isExecutable bool) io.WriterTo {
	handle := varint.AppendForward(nil, fileContents.Reference.GetReferenceFormat().ToProto())
	handle = append(handle, fileContents.Reference.GetRawReference()...)
	handle = varint.AppendForward(handle, fileContents.EndBytes)
	if isExecutable {
		handle = append(handle, 0x01)
	} else {
		handle = append(handle, 0x00)
	}
	return bytes.NewBuffer(handle)
}

func (ff *resolvableHandleAllocatingFileFactory) LookupFile(fileContents model_filesystem.FileContentsEntry[object.LocalReference], isExecutable bool) virtual.LinkableLeaf {
	return ff.handleAllocator.
		New(computeFileID(fileContents, isExecutable)).
		AsLinkableLeaf(ff.base.LookupFile(fileContents, isExecutable))
}
