package filesystem

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// DirectoryCluster is a list of all Directory messages that are
// contained in a single object in storage. Directories are stored in
// topological order, meaning that the root directory is located at
// index zero.
//
// The advantage of a DirectoryCluster over a plain Directory message is
// that it's possible to refer to individual directories using an index.
type DirectoryCluster []Directory

// Directory contained in a DirectoryCluster.
type Directory struct {
	Directory *model_filesystem_pb.Directory

	// Indices at which child directories with inline contents are
	// accessible within the same cluster. If the child directory
	// has external contents, the index is set to -1.
	ChildDirectoryIndices []int
}

type directoryClusterObjectParser[TReference any] struct{}

// NewDirectoryClusterObjectParser creates an ObjectParser that is
// capable of parsing directory objects, and exposing them in the form
// of a DirectoryCluster.
func NewDirectoryClusterObjectParser[TReference any]() model_parser.ObjectParser[TReference, model_core.Message[DirectoryCluster, TReference]] {
	return &directoryClusterObjectParser[TReference]{}
}

func (p *directoryClusterObjectParser[TReference]) ParseObject(in model_core.Message[[]byte, TReference]) (model_core.Message[DirectoryCluster, TReference], int, error) {
	var d model_filesystem_pb.Directory
	if err := proto.Unmarshal(in.Message, &d); err != nil {
		return model_core.Message[DirectoryCluster, TReference]{}, 0, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to parse directory")
	}

	// Recursively visit all Directory messages contained in the
	// object and store them in a list. This allows the caller to
	// address each directory separately.
	var cluster DirectoryCluster
	_, err := addDirectoriesToCluster(
		&cluster,
		model_core.NewMessage(&d, in.OutgoingReferences),
		nil,
	)
	if err != nil {
		return model_core.Message[DirectoryCluster, TReference]{}, 0, err
	}
	return model_core.NewNestedMessage(in, cluster), len(in.Message), nil
}

func addDirectoriesToCluster[TReference any](c *DirectoryCluster, d model_core.Message[*model_filesystem_pb.Directory, TReference], dTrace *path.Trace) (int, error) {
	directoryIndex := len(*c)
	childDirectoryIndices := make([]int, len(d.Message.Directories))
	*c = append(
		*c,
		Directory{
			Directory:             d.Message,
			ChildDirectoryIndices: childDirectoryIndices,
		},
	)

	for i, entry := range d.Message.Directories {
		switch contents := entry.Contents.(type) {
		case *model_filesystem_pb.DirectoryNode_ContentsExternal:
			childDirectoryIndices[i] = -1
		case *model_filesystem_pb.DirectoryNode_ContentsInline:
			// Subdirectory is stored in the same object.
			// Recurse into it, so that it gets its own
			// directory index.
			name, ok := path.NewComponent(entry.Name)
			if !ok {
				return 0, status.Errorf(codes.InvalidArgument, "Entry %#v in directory %#v has an invalid name", entry.Name, dTrace.GetUNIXString())
			}
			childDirectoryIndex, err := addDirectoriesToCluster(
				c,
				model_core.NewNestedMessage(d, contents.ContentsInline),
				dTrace.Append(name),
			)
			if err != nil {
				return 0, err
			}
			childDirectoryIndices[i] = childDirectoryIndex
		default:
			// Subdirectory is stored in another object.
			name, ok := path.NewComponent(entry.Name)
			if !ok {
				return 0, status.Errorf(codes.InvalidArgument, "Entry %#v in directory %#v has an invalid name", entry.Name, dTrace.GetUNIXString())
			}
			return 0, status.Errorf(codes.InvalidArgument, "Directory %#v has no contents", dTrace.Append(name).GetUNIXString())
		}
	}
	return directoryIndex, nil
}

// DirectoryGetLeaves is a helper function for obtaining the leaves
// contained in a directory.
func DirectoryGetLeaves[TReference any](
	ctx context.Context,
	reader model_parser.ParsedObjectReader[TReference, model_core.Message[*model_filesystem_pb.Leaves, TReference]],
	directory model_core.Message[*model_filesystem_pb.Directory, TReference],
) (model_core.Message[*model_filesystem_pb.Leaves, TReference], error) {
	switch leaves := directory.Message.Leaves.(type) {
	case *model_filesystem_pb.Directory_LeavesExternal:
		return model_parser.Dereference(ctx, reader, model_core.NewNestedMessage(directory, leaves.LeavesExternal.Reference))
	case *model_filesystem_pb.Directory_LeavesInline:
		return model_core.NewNestedMessage(directory, leaves.LeavesInline), nil
	default:
		return model_core.Message[*model_filesystem_pb.Leaves, TReference]{}, status.Error(codes.InvalidArgument, "Directory has no leaves")
	}
}
