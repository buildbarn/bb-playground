package virtual

import (
	"bytes"
	"context"
	"io"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/encoding/varint"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	object_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ObjectBackedDirectoryFactory struct {
	handleAllocator        virtual.ResolvableHandleAllocator
	directoryClusterReader model_parser.ParsedObjectReader[object.LocalReference, model_filesystem.DirectoryCluster]
	fileFactory            FileFactory
	errorLogger            util.ErrorLogger
}

func NewObjectBackedDirectoryFactory(handleAllocation virtual.ResolvableHandleAllocation, directoryClusterReader model_parser.ParsedObjectReader[object.LocalReference, model_filesystem.DirectoryCluster], fileFactory FileFactory, errorLogger util.ErrorLogger) *ObjectBackedDirectoryFactory {
	df := &ObjectBackedDirectoryFactory{
		directoryClusterReader: directoryClusterReader,
		fileFactory:            fileFactory,
		errorLogger:            errorLogger,
	}
	df.handleAllocator = handleAllocation.AsResolvableAllocator(df.resolveHandle)
	return df
}

func (df *ObjectBackedDirectoryFactory) resolveHandle(r io.ByteReader) (virtual.DirectoryChild, virtual.Status) {
	// Parse cluster reference.
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
	clusterReference, err := referenceFormat.NewLocalReference(rawReference)
	if err != nil {
		return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
	}

	directoryIndex, err := varint.ReadForward[uint](r)
	if err != nil {
		return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
	}

	symlinkIndex, err := varint.ReadForward[uint](r)
	if err != nil {
		return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
	}
	if symlinkIndex == 0 {
		// File handle resolves to the directory.
		directoriesCount, err := varint.ReadForward[uint32](r)
		if err != nil {
			return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
		}
		return virtual.DirectoryChild{}.FromDirectory(
			df.LookupDirectory(model_filesystem.DirectoryInfo{
				ClusterReference: clusterReference,
				DirectoryIndex:   directoryIndex,
				DirectoriesCount: directoriesCount,
			}),
		), virtual.StatusOK
	}

	// File handle resolves to a symbolic link contained in the directory.
	// TODO: Should virtual.HandleResolver provide a context?
	cluster, _, err := df.directoryClusterReader.ReadParsedObject(context.Background(), clusterReference)
	if err != nil {
		df.errorLogger.Log(util.StatusWrapf(err, "Failed to fetch directory cluster with reference %s", clusterReference))
		return virtual.DirectoryChild{}, virtual.StatusErrIO
	}
	if directoryIndex > uint(len(cluster)) {
		return virtual.DirectoryChild{}, virtual.StatusErrIO
	}
	symlinks := cluster[directoryIndex].Leaves.Message.Symlinks
	if symlinkIndex > uint(len(symlinks)) {
		return virtual.DirectoryChild{}, virtual.StatusErrIO
	}
	return virtual.DirectoryChild{}.FromLeaf(df.createSymlink(clusterReference, directoryIndex, symlinkIndex, symlinks[symlinkIndex-1].Target)), virtual.StatusOK
}

func (df *ObjectBackedDirectoryFactory) LookupDirectory(info model_filesystem.DirectoryInfo) virtual.Directory {
	handle := varint.AppendForward(nil, info.ClusterReference.GetReferenceFormat().ToProto())
	handle = append(handle, info.ClusterReference.GetRawReference()...)
	handle = varint.AppendForward(handle, info.DirectoryIndex)
	handle = varint.AppendForward(handle, 0)
	handle = varint.AppendForward(handle, info.DirectoriesCount)
	return df.handleAllocator.New(bytes.NewBuffer(handle)).
		AsStatelessDirectory(&objectBackedDirectory{
			factory: df,
			info:    info,
		})
}

func (df *ObjectBackedDirectoryFactory) createSymlink(clusterReference object.LocalReference, directoryIndex, symlinkIndex uint, target string) virtual.NativeLeaf {
	handle := varint.AppendForward(nil, clusterReference.GetReferenceFormat().ToProto())
	handle = append(handle, clusterReference.GetRawReference()...)
	handle = varint.AppendForward(handle, directoryIndex)
	handle = varint.AppendForward(handle, symlinkIndex+1)
	return df.handleAllocator.New(bytes.NewBuffer(handle)).
		AsNativeLeaf(virtual.BaseSymlinkFactory.LookupSymlink([]byte(target)))
}

type objectBackedDirectory struct {
	virtual.ReadOnlyDirectory

	factory *ObjectBackedDirectoryFactory
	info    model_filesystem.DirectoryInfo
}

func (d *objectBackedDirectory) lookupFile(fileNode *model_filesystem_pb.FileNode, leavesReferences object.OutgoingReferences) (virtual.Leaf, virtual.Status) {
	df := d.factory
	fileContents, err := model_filesystem.NewFileContentsEntryFromProto(
		model_parser.ParsedMessage[*model_filesystem_pb.FileContents]{
			Message:            fileNode.Contents,
			OutgoingReferences: leavesReferences,
		},
		d.info.ClusterReference.GetReferenceFormat(),
	)
	if err != nil {
		df.errorLogger.Log(util.StatusWrapf(err, "Invalid contents for file %#v", fileNode.Name))
		return nil, virtual.StatusErrIO
	}
	return df.fileFactory.LookupFile(
		fileContents,
		fileNode.IsExecutable,
	), virtual.StatusOK
}

func (d *objectBackedDirectory) VirtualGetAttributes(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
	attributes.SetChangeID(0)
	attributes.SetLinkCount(virtual.EmptyDirectoryLinkCount + d.info.DirectoriesCount)
	attributes.SetFileType(filesystem.FileTypeDirectory)
	attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsExecute)
	attributes.SetSizeBytes(uint64(d.info.ClusterReference.GetSizeBytes()))
}

func (d *objectBackedDirectory) getDirectory(ctx context.Context) (*model_filesystem.Directory, virtual.Status) {
	df := d.factory
	cluster, _, err := df.directoryClusterReader.ReadParsedObject(ctx, d.info.ClusterReference)
	if err != nil {
		df.errorLogger.Log(util.StatusWrapf(err, "Failed to fetch directory cluster with reference %s", d.info.ClusterReference))
		return nil, virtual.StatusErrIO
	}
	if d.info.DirectoryIndex >= uint(len(cluster)) {
		df.errorLogger.Log(status.Errorf(codes.InvalidArgument, "Directory index %d is out of range, as directory cluster with reference %s only contains %d directories", d.info.DirectoryIndex, len(cluster), d.info.ClusterReference))
		return nil, virtual.StatusErrIO
	}
	return &cluster[d.info.DirectoryIndex], virtual.StatusOK
}

func (d *objectBackedDirectory) VirtualLookup(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
	df := d.factory
	directory, s := d.getDirectory(ctx)
	if s != virtual.StatusOK {
		return virtual.DirectoryChild{}, s
	}

	// The filesystem model requires that entries stored in a
	// Directory message are sorted alphabetically. Make use of this
	// fact by performing binary searching when looking up entries.
	n := name.String()
	if i := sort.Search(
		len(directory.Directories),
		func(i int) bool { return directory.Directories[i].Name.String() >= n },
	); i < len(directory.Directories) && directory.Directories[i].Name.String() == n {
		subdirectory := &directory.Directories[i]
		child := df.LookupDirectory(subdirectory.Info)
		child.VirtualGetAttributes(ctx, requested, out)
		return virtual.DirectoryChild{}.FromDirectory(child), virtual.StatusOK
	}

	files := directory.Leaves.Message.Files
	if i := sort.Search(
		len(files),
		func(i int) bool { return files[i].Name >= n },
	); i < len(files) && files[i].Name == n {
		entry := files[i]
		child, s := d.lookupFile(entry, directory.Leaves.OutgoingReferences)
		if s != virtual.StatusOK {
			return virtual.DirectoryChild{}, virtual.StatusErrIO
		}
		child.VirtualGetAttributes(ctx, requested, out)
		return virtual.DirectoryChild{}.FromLeaf(child), virtual.StatusOK
	}

	symlinks := directory.Leaves.Message.Symlinks
	if i := sort.Search(
		len(symlinks),
		func(i int) bool { return symlinks[i].Name >= n },
	); i < len(symlinks) && symlinks[i].Name == n {
		f := df.createSymlink(d.info.ClusterReference, d.info.DirectoryIndex, uint(i), symlinks[i].Target)
		f.VirtualGetAttributes(ctx, requested, out)
		return virtual.DirectoryChild{}.FromLeaf(f), virtual.StatusOK
	}

	return virtual.DirectoryChild{}, virtual.StatusErrNoEnt
}

func (d *objectBackedDirectory) VirtualOpenChild(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, openedFileAttributes *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
	directory, s := d.getDirectory(ctx)
	if s != virtual.StatusOK {
		return nil, 0, virtual.ChangeInfo{}, s
	}

	n := name.String()
	if i := sort.Search(
		len(directory.Directories),
		func(i int) bool { return directory.Directories[i].Name.String() >= n },
	); i < len(directory.Directories) && directory.Directories[i].Name.String() == n {
		return virtual.ReadOnlyDirectoryOpenChildWrongFileType(existingOptions, virtual.StatusErrIsDir)
	}

	files := directory.Leaves.Message.Files
	if i := sort.Search(
		len(files),
		func(i int) bool { return files[i].Name >= n },
	); i < len(files) && files[i].Name == n {
		if existingOptions == nil {
			return nil, 0, virtual.ChangeInfo{}, virtual.StatusErrExist
		}

		leaf, s := d.lookupFile(files[i], directory.Leaves.OutgoingReferences)
		if s != virtual.StatusOK {
			return nil, 0, virtual.ChangeInfo{}, s
		}
		s = leaf.VirtualOpenSelf(ctx, shareAccess, existingOptions, requested, openedFileAttributes)
		return leaf, existingOptions.ToAttributesMask(), virtual.ChangeInfo{}, s
	}

	symlinks := directory.Leaves.Message.Symlinks
	if i := sort.Search(
		len(symlinks),
		func(i int) bool { return symlinks[i].Name >= n },
	); i < len(symlinks) && symlinks[i].Name == n {
		return virtual.ReadOnlyDirectoryOpenChildWrongFileType(existingOptions, virtual.StatusErrSymlink)
	}

	return virtual.ReadOnlyDirectoryOpenChildDoesntExist(createAttributes)
}

func (d *objectBackedDirectory) VirtualReadDir(ctx context.Context, firstCookie uint64, requested virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
	df := d.factory
	directory, s := d.getDirectory(ctx)
	if s != virtual.StatusOK {
		return s
	}

	i := firstCookie
	nextCookieOffset := uint64(1)

	for ; i < uint64(len(directory.Directories)); i++ {
		entry := directory.Directories[i]
		child := df.LookupDirectory(entry.Info)
		var attributes virtual.Attributes
		child.VirtualGetAttributes(ctx, requested, &attributes)
		if !reporter.ReportEntry(nextCookieOffset+i, entry.Name, virtual.DirectoryChild{}.FromDirectory(child), &attributes) {
			return virtual.StatusOK
		}
	}
	i -= uint64(len(directory.Directories))
	nextCookieOffset += uint64(len(directory.Directories))

	for ; i < uint64(len(directory.Leaves.Message.Files)); i++ {
		entry := directory.Leaves.Message.Files[i]
		name, ok := path.NewComponent(entry.Name)
		if !ok {
			df.errorLogger.Log(status.Errorf(codes.InvalidArgument, "File %#v has an invalid name", entry.Name))
			return virtual.StatusErrIO
		}
		child, s := d.lookupFile(entry, directory.Leaves.OutgoingReferences)
		if s != virtual.StatusOK {
			return s
		}
		var attributes virtual.Attributes
		child.VirtualGetAttributes(ctx, requested, &attributes)
		if !reporter.ReportEntry(nextCookieOffset+i, name, virtual.DirectoryChild{}.FromLeaf(child), &attributes) {
			return virtual.StatusOK
		}
	}
	i -= uint64(len(directory.Leaves.Message.Files))
	nextCookieOffset += uint64(len(directory.Leaves.Message.Files))

	for ; i < uint64(len(directory.Leaves.Message.Symlinks)); i++ {
		entry := directory.Leaves.Message.Symlinks[i]
		name, ok := path.NewComponent(entry.Name)
		if !ok {
			df.errorLogger.Log(status.Errorf(codes.InvalidArgument, "Symbolic link %#v has an invalid name", entry.Name))
			return virtual.StatusErrIO
		}
		child := df.createSymlink(d.info.ClusterReference, d.info.DirectoryIndex, uint(i), entry.Target)
		var attributes virtual.Attributes
		child.VirtualGetAttributes(ctx, requested, &attributes)
		if !reporter.ReportEntry(nextCookieOffset+i, name, virtual.DirectoryChild{}.FromLeaf(child), &attributes) {
			return virtual.StatusOK
		}
	}

	return virtual.StatusOK
}
