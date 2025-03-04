package virtual

import (
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

type statelessHandleAllocatingFileFactory struct {
	base            FileFactory
	handleAllocator virtual.StatelessHandleAllocator
}

func NewStatelessHandleAllocatingFileFactory(base FileFactory, handleAllocation virtual.StatelessHandleAllocation) FileFactory {
	return &statelessHandleAllocatingFileFactory{
		base:            base,
		handleAllocator: handleAllocation.AsStatelessAllocator(),
	}
}

func (ff *statelessHandleAllocatingFileFactory) LookupFile(fileContents model_filesystem.FileContentsEntry[object.LocalReference], isExecutable bool) virtual.LinkableLeaf {
	return ff.handleAllocator.
		New(computeFileID(fileContents, isExecutable)).
		AsLinkableLeaf(ff.base.LookupFile(fileContents, isExecutable))
}
