package filesystem

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/parser"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// DirectoryCluster is a list of all Directory messages that are
// contained in a single object in storage. Directories are stored in
// topological order, meaning that the root directory is located at
// index zero.
type DirectoryCluster []Directory

// Directory contained in a DirectoryCluster.
type Directory struct {
	Directories []DirectoryNode
	Leaves      model_core.Message[*model_filesystem_pb.Leaves, object.OutgoingReferences[object.LocalReference]]
	Raw         model_core.Message[*model_filesystem_pb.Directory, object.OutgoingReferences[object.LocalReference]]
}

// DirectoryInfo holds all of the properties of a directory that could
// be derived from its parent directory.
type DirectoryInfo struct {
	ClusterReference object.LocalReference
	DirectoryIndex   uint
	DirectoriesCount uint32
}

// NewDirectoryInfoFromDirectoryReference creates a DirectoryInfo based
// on the contents of a DirectoryReference message.
func NewDirectoryInfoFromDirectoryReference(directoryReference model_core.Message[*model_filesystem_pb.DirectoryReference, object.OutgoingReferences[object.LocalReference]]) (DirectoryInfo, error) {
	if directoryReference.Message == nil {
		return DirectoryInfo{}, status.Error(codes.InvalidArgument, "No directory reference provided")
	}
	clusterReference, err := model_core.FlattenReference(model_core.NewNestedMessage(directoryReference, directoryReference.Message.Reference))
	if err != nil {
		return DirectoryInfo{}, err
	}
	return DirectoryInfo{
		ClusterReference: clusterReference,
		DirectoryIndex:   0,
		DirectoriesCount: directoryReference.Message.DirectoriesCount,
	}, nil
}

// DirectoryNode contains the name and properties of a directory that is
// contained within another directory.
type DirectoryNode struct {
	Name path.Component
	Info DirectoryInfo
}

// DirectoryClusterObjectParserReference is a constraint on the reference types
// accepted by the ObjectParser returned by NewDirectoryClusterObjectParser.
type DirectoryClusterObjectParserReference[T any] interface {
	GetLocalReference() object.LocalReference
	WithLocalReference(localReference object.LocalReference) T
}

type directoryClusterObjectParser[TReference DirectoryClusterObjectParserReference[TReference]] struct {
	leavesReader parser.ParsedObjectReader[TReference, model_core.Message[*model_filesystem_pb.Leaves, object.OutgoingReferences[object.LocalReference]]]
}

// NewDirectoryClusterObjectParser creates an ObjectParser that is
// capable of parsing directory objects. These directory objects may
// either be empty, contain subdirectories, or leaves.
func NewDirectoryClusterObjectParser[TReference DirectoryClusterObjectParserReference[TReference]](leavesReader parser.ParsedObjectReader[TReference, model_core.Message[*model_filesystem_pb.Leaves, object.OutgoingReferences[object.LocalReference]]]) parser.ObjectParser[TReference, DirectoryCluster] {
	return &directoryClusterObjectParser[TReference]{
		leavesReader: leavesReader,
	}
}

func (p *directoryClusterObjectParser[TReference]) ParseObject(ctx context.Context, reference TReference, outgoingReferences object.OutgoingReferences[object.LocalReference], data []byte) (DirectoryCluster, int, error) {
	var d model_filesystem_pb.Directory
	if err := proto.Unmarshal(data, &d); err != nil {
		return nil, 0, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to parse directory")
	}

	// Recursively visit all Directory messages contained in the
	// object and store them in a list. This allows the caller to
	// address each directory separately.
	var cluster DirectoryCluster
	_, externalLeavesTotalSizeBytes, err := p.addDirectoriesToCluster(
		ctx,
		&cluster,
		model_core.Message[*model_filesystem_pb.Directory, object.OutgoingReferences[object.LocalReference]]{
			Message:            &d,
			OutgoingReferences: outgoingReferences.GetOutgoingReferencesList(),
		},
		reference,
		nil,
	)
	if err != nil {
		return nil, 0, err
	}
	return cluster, reference.GetLocalReference().GetSizeBytes() + externalLeavesTotalSizeBytes, nil
}

func (p *directoryClusterObjectParser[TReference]) addDirectoriesToCluster(ctx context.Context, c *DirectoryCluster, d model_core.Message[*model_filesystem_pb.Directory, object.OutgoingReferences[object.LocalReference]], reference TReference, dTrace *path.Trace) (uint, int, error) {
	directoryIndex := uint(len(*c))
	*c = append(
		*c,
		Directory{
			Directories: make([]DirectoryNode, 0, len(d.Message.Directories)),
			Raw:         d,
		},
	)

	localReference := reference.GetLocalReference()
	externalLeavesTotalSizeBytes := 0
	switch leaves := d.Message.Leaves.(type) {
	case *model_filesystem_pb.Directory_LeavesExternal:
		leavesReference, err := model_core.FlattenReference(model_core.NewNestedMessage(d, leaves.LeavesExternal.Reference))
		if err != nil {
			return 0, 0, util.StatusWrapf(err, "Invalid reference for leaves for directory %#v", dTrace.GetUNIXString())
		}
		leavesObject, externalLeavesSizeBytes, err := p.leavesReader.ReadParsedObject(ctx, reference.WithLocalReference(leavesReference))
		if err != nil {
			return 0, 0, util.StatusWrapf(err, "Leaves for directory %#v with reference %s", dTrace.GetUNIXString(), leavesReference)
		}
		(*c)[directoryIndex].Leaves = leavesObject
		externalLeavesTotalSizeBytes += externalLeavesSizeBytes
	case *model_filesystem_pb.Directory_LeavesInline:
		(*c)[directoryIndex].Leaves = model_core.NewNestedMessage(d, leaves.LeavesInline)
	default:
		return 0, 0, status.Errorf(codes.InvalidArgument, "Directory %#v has no leaves", dTrace.GetUNIXString())
	}

	for _, entry := range d.Message.Directories {
		name, ok := path.NewComponent(entry.Name)
		if !ok {
			return 0, 0, status.Errorf(codes.InvalidArgument, "Entry %#v in directory %#v has an invalid name", entry.Name, dTrace.GetUNIXString())
		}
		switch contents := entry.Contents.(type) {
		case *model_filesystem_pb.DirectoryNode_ContentsExternal:
			// Subdirectory is stored in another object.
			// Extract its reference.
			directoryInfo, err := NewDirectoryInfoFromDirectoryReference(model_core.NewNestedMessage(d, contents.ContentsExternal))
			if err != nil {
				return 0, 0, util.StatusWrapf(err, "Failed to create info for directory %#v", dTrace.Append(name).GetUNIXString())
			}
			(*c)[directoryIndex].Directories = append(
				(*c)[directoryIndex].Directories,
				DirectoryNode{
					Name: name,
					Info: directoryInfo,
				},
			)
		case *model_filesystem_pb.DirectoryNode_ContentsInline:
			// Subdirectory is stored in the same object.
			// Recurse into it, so that it gets its own
			// directory index.
			childDirectoryIndex, childExternalLeavesTotalSizeBytes, err := p.addDirectoriesToCluster(
				ctx,
				c,
				model_core.NewNestedMessage(d, contents.ContentsInline),
				reference,
				dTrace.Append(name),
			)
			if err != nil {
				return 0, 0, err
			}
			(*c)[directoryIndex].Directories = append(
				(*c)[directoryIndex].Directories,
				DirectoryNode{
					Name: name,
					Info: DirectoryInfo{
						ClusterReference: localReference,
						DirectoryIndex:   childDirectoryIndex,
						DirectoriesCount: uint32(len((*c)[childDirectoryIndex].Directories)),
					},
				},
			)
			externalLeavesTotalSizeBytes += childExternalLeavesTotalSizeBytes
		default:
			return 0, 0, status.Errorf(codes.InvalidArgument, "Directory %#v has no contents", dTrace.Append(name).GetUNIXString())
		}
	}
	return directoryIndex, externalLeavesTotalSizeBytes, nil
}

type LeavesParsedObjectReaderForTesting parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_filesystem_pb.Leaves, object.OutgoingReferences[object.LocalReference]]]
