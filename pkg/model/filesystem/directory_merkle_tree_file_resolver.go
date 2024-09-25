package filesystem

import (
	"context"
	"sort"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DirectoryMerkleTreeFileResolver is an implementation of
// path.ComponentWalker that resolves the FileProperties corresponding
// to a given path. It can be used to look up files contained in a
// Merkle tree of Directory and Leaves messages.
type DirectoryMerkleTreeFileResolver struct {
	context         context.Context
	directoryReader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_filesystem_pb.Directory]]
	leavesReader    model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_filesystem_pb.Leaves]]

	currentDirectoryReference object.LocalReference
	currentDirectory          model_core.Message[*model_filesystem_pb.Directory]

	fileProperties model_core.Message[*model_filesystem_pb.FileProperties]
}

var _ path.ComponentWalker = &DirectoryMerkleTreeFileResolver{}

// NewDirectoryMerkleTreeFileResolver creates a
// DirectoryMerkleTreeFileResolver that starts resolution within a
// provided root directory.
func NewDirectoryMerkleTreeFileResolver(
	ctx context.Context,
	directoryReader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_filesystem_pb.Directory]],
	leavesReader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_filesystem_pb.Leaves]],
	rootDirectoryReference object.LocalReference,
) *DirectoryMerkleTreeFileResolver {
	return &DirectoryMerkleTreeFileResolver{
		context:                   ctx,
		directoryReader:           directoryReader,
		leavesReader:              leavesReader,
		currentDirectoryReference: rootDirectoryReference,
	}
}

func (r *DirectoryMerkleTreeFileResolver) GetFileProperties() model_core.Message[*model_filesystem_pb.FileProperties] {
	return r.fileProperties
}

func (r *DirectoryMerkleTreeFileResolver) getCurrentDirectory() (model_core.Message[*model_filesystem_pb.Directory], error) {
	if !r.currentDirectory.IsSet() {
		d, _, err := r.directoryReader.ReadParsedObject(r.context, r.currentDirectoryReference)
		if err != nil {
			return model_core.Message[*model_filesystem_pb.Directory]{}, err
		}
		r.currentDirectory = d
	}
	return r.currentDirectory, nil
}

func (r *DirectoryMerkleTreeFileResolver) getCurrentLeaves() (model_core.Message[*model_filesystem_pb.Leaves], error) {
	switch leaves := r.currentDirectory.Message.Leaves.(type) {
	case *model_filesystem_pb.Directory_LeavesExternal:
		index, err := model_core.GetIndexFromReferenceMessage(leaves.LeavesExternal, r.currentDirectory.OutgoingReferences.GetDegree())
		if err != nil {
			return model_core.Message[*model_filesystem_pb.Leaves]{}, err
		}
		l, _, err := r.leavesReader.ReadParsedObject(
			r.context,
			r.currentDirectory.OutgoingReferences.GetOutgoingReference(index),
		)
		return l, err
	case *model_filesystem_pb.Directory_LeavesInline:
		return model_core.Message[*model_filesystem_pb.Leaves]{
			Message:            leaves.LeavesInline,
			OutgoingReferences: r.currentDirectory.OutgoingReferences,
		}, nil
	default:
		return model_core.Message[*model_filesystem_pb.Leaves]{}, status.Error(codes.InvalidArgument, "Unknown leaves contents type")
	}
}

func (r *DirectoryMerkleTreeFileResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	directory, err := r.getCurrentDirectory()
	if err != nil {
		return nil, err
	}

	n := name.String()
	directories := directory.Message.Directories
	if i := sort.Search(
		len(directories),
		func(i int) bool { return directories[i].Name >= n },
	); i < len(directories) && directories[i].Name == n {
		directoryNode := directories[i]
		switch contents := directoryNode.Contents.(type) {
		case *model_filesystem_pb.DirectoryNode_ContentsExternal:
			index, err := model_core.GetIndexFromReferenceMessage(contents.ContentsExternal.Reference, r.currentDirectory.OutgoingReferences.GetDegree())
			if err != nil {
				return nil, err
			}
			r.currentDirectoryReference = r.currentDirectory.OutgoingReferences.GetOutgoingReference(index)
			r.currentDirectory = model_core.Message[*model_filesystem_pb.Directory]{}
		case *model_filesystem_pb.DirectoryNode_ContentsInline:
			r.currentDirectory.Message = contents.ContentsInline
		default:
			return nil, status.Error(codes.InvalidArgument, "Unknown directory node contents type")
		}
	}

	leaves, err := r.getCurrentLeaves()
	if err != nil {
		return nil, err
	}

	files := leaves.Message.Files
	if i := sort.Search(
		len(files),
		func(i int) bool { return files[i].Name >= n },
	); i < len(files) && files[i].Name == n {
		return nil, status.Error(codes.InvalidArgument, "Path resolves to a regular file, while a directory was expected")
	}

	symlinks := leaves.Message.Symlinks
	if i := sort.Search(
		len(symlinks),
		func(i int) bool { return symlinks[i].Name >= n },
	); i < len(symlinks) && symlinks[i].Name == n {
		return path.GotSymlink{
			Parent: path.NewRelativeScopeWalker(r),
			Target: path.UNIXFormat.NewParser(symlinks[i].Target),
		}, nil
	}

	return nil, status.Error(codes.NotFound, "Path does not exist")
}

func (r *DirectoryMerkleTreeFileResolver) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	directory, err := r.getCurrentDirectory()
	if err != nil {
		return nil, err
	}

	n := name.String()
	directories := directory.Message.Directories
	if i := sort.Search(
		len(directories),
		func(i int) bool { return directories[i].Name >= n },
	); i < len(directories) && directories[i].Name == n {
		return nil, status.Error(codes.InvalidArgument, "Path resolves to a directory, while a file was expected")
	}

	leaves, err := r.getCurrentLeaves()
	if err != nil {
		return nil, err
	}

	files := leaves.Message.Files
	if i := sort.Search(
		len(files),
		func(i int) bool { return files[i].Name >= n },
	); i < len(files) && files[i].Name == n {
		properties := files[i].Properties
		if properties == nil {
			return nil, status.Error(codes.InvalidArgument, "Path resolves to file that does not have any properties")
		}
		r.fileProperties = model_core.Message[*model_filesystem_pb.FileProperties]{
			Message:            properties,
			OutgoingReferences: leaves.OutgoingReferences,
		}
		return nil, nil
	}

	symlinks := leaves.Message.Symlinks
	if i := sort.Search(
		len(symlinks),
		func(i int) bool { return symlinks[i].Name >= n },
	); i < len(symlinks) && symlinks[i].Name == n {
		return &path.GotSymlink{
			Parent: path.NewRelativeScopeWalker(r),
			Target: path.UNIXFormat.NewParser(symlinks[i].Target),
		}, nil
	}

	return nil, status.Error(codes.NotFound, "Path does not exist")
}

func (DirectoryMerkleTreeFileResolver) OnUp() (path.ComponentWalker, error) {
	return nil, status.Error(codes.Unimplemented, "Parent directory resolution not implemented yet")
}
