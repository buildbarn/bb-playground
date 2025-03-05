package virtual

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type objectBackedInitialContentsFetcherOptions struct {
	context                context.Context
	directoryClusterReader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[model_filesystem.DirectoryCluster, object.LocalReference]]
	leavesReader           model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_filesystem_pb.Leaves, object.LocalReference]]
	fileFactory            FileFactory
	symlinkFactory         virtual.SymlinkFactory
}

type objectBackedInitialContentsFetcher struct {
	options          *objectBackedInitialContentsFetcherOptions
	clusterReference object.LocalReference
	directoryIndex   int
}

func NewObjectBackedInitialContentsFetcher(
	ctx context.Context,
	directoryClusterReader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[model_filesystem.DirectoryCluster, object.LocalReference]],
	leavesReader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_filesystem_pb.Leaves, object.LocalReference]],
	fileFactory FileFactory,
	symlinkFactory virtual.SymlinkFactory,
	rootClusterReference object.LocalReference,
) virtual.InitialContentsFetcher {
	return &objectBackedInitialContentsFetcher{
		options: &objectBackedInitialContentsFetcherOptions{
			context:                ctx,
			directoryClusterReader: directoryClusterReader,
			leavesReader:           leavesReader,
			fileFactory:            fileFactory,
			symlinkFactory:         symlinkFactory,
		},
		clusterReference: rootClusterReference,
	}
}

func (icf *objectBackedInitialContentsFetcher) getDirectory() (model_core.Message[*model_filesystem.Directory, object.LocalReference], error) {
	options := icf.options
	cluster, err := options.directoryClusterReader.ReadParsedObject(options.context, icf.clusterReference)
	if err != nil {
		return model_core.Message[*model_filesystem.Directory, object.LocalReference]{}, util.StatusWrapf(err, "Failed to fetch directory cluster with reference %s", icf.clusterReference)
	}
	return model_core.NewNestedMessage(cluster, &cluster.Message[icf.directoryIndex]), nil
}

func (icf *objectBackedInitialContentsFetcher) FetchContents(fileReadMonitorFactory virtual.FileReadMonitorFactory) (map[path.Component]virtual.InitialChild, error) {
	directory, err := icf.getDirectory()
	if err != nil {
		return nil, err
	}

	options := icf.options
	leaves, err := model_filesystem.DirectoryGetLeaves(
		options.context,
		options.leavesReader,
		model_core.NewNestedMessage(directory, directory.Message.Directory),
	)
	if err != nil {
		return nil, err
	}

	// Create InitialContentsFetchers for all child directories.
	// These can yield even more InitialContentsFetchers for
	// grandchildren.
	directories := directory.Message.Directory.Directories
	files := leaves.Message.Files
	symlinks := leaves.Message.Symlinks
	children := make(map[path.Component]virtual.InitialChild, len(directories)+len(files)+len(symlinks))
	for i, entry := range directories {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return nil, util.StatusWrapf(err, "Directory %#v has na invalid name", entry.Name)
		}
		if _, ok := children[component]; ok {
			return nil, status.Errorf(codes.InvalidArgument, "Directory contains multiple children named %#v", entry.Name)
		}

		var child virtual.InitialContentsFetcher
		switch contents := entry.Contents.(type) {
		case *model_filesystem_pb.DirectoryNode_ContentsExternal:
			childReference, err := model_core.FlattenReference(model_core.NewNestedMessage(directory, contents.ContentsExternal.Reference))
			if err != nil {
				return nil, util.StatusWrapf(err, "Invalid reference for directory with name %#v", entry.Name)
			}
			child = &objectBackedInitialContentsFetcher{
				options:          options,
				clusterReference: childReference,
				directoryIndex:   0,
			}
		case *model_filesystem_pb.DirectoryNode_ContentsInline:
			child = &objectBackedInitialContentsFetcher{
				options:          options,
				clusterReference: icf.clusterReference,
				directoryIndex:   directory.Message.ChildDirectoryIndices[i],
			}
		default:
			return nil, status.Errorf(codes.InvalidArgument, "Invalid contents for directory with name %#v", entry.Name)
		}

		children[component] = virtual.InitialChild{}.FromDirectory(child)
	}

	// Ensure that leaves are properly unlinked if this method fails.
	leavesToUnlink := make([]virtual.LinkableLeaf, 0, len(files)+len(symlinks))
	defer func() {
		for _, leaf := range leavesToUnlink {
			leaf.Unlink()
		}
	}()

	// Create storage backed read-only files.
	for _, entry := range files {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return nil, util.StatusWrapf(err, "File %#v has na invalid name", entry.Name)
		}
		if _, ok := children[component]; ok {
			return nil, status.Errorf(codes.InvalidArgument, "Directory contains multiple children named %#v", entry.Name)
		}

		properties := entry.Properties
		if properties == nil {
			return nil, status.Errorf(codes.InvalidArgument, "File %#v does not have any properties", entry.Name)
		}
		fileContents, err := model_filesystem.NewFileContentsEntryFromProto(
			model_core.NewNestedMessage(leaves, properties.Contents),
		)
		if err != nil {
			return nil, util.StatusWrapf(err, "Invalid contents for file %#v", entry.Name)
		}

		leaf := options.fileFactory.LookupFile(
			fileContents,
			properties.IsExecutable,
		)
		children[component] = virtual.InitialChild{}.FromLeaf(leaf)
		leavesToUnlink = append(leavesToUnlink, leaf)
	}

	// Create symbolic links.
	for _, entry := range symlinks {
		component, ok := path.NewComponent(entry.Name)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "Symlink %#v has an invalid name", entry.Name)
		}
		if _, ok := children[component]; ok {
			return nil, status.Errorf(codes.InvalidArgument, "Directory contains multiple children named %#v", entry.Name)
		}

		leaf := options.symlinkFactory.LookupSymlink([]byte(entry.Target))
		children[component] = virtual.InitialChild{}.FromLeaf(leaf)
		leavesToUnlink = append(leavesToUnlink, leaf)
	}

	leavesToUnlink = nil
	return children, nil
}

// ApplyGetRawDirectory can be used to obtain the Directory message that
// backs a directory, assuming the contents of the directory still match
// the message.
//
// This can be used to efficiently compute a Merkle tree of a directory
// hierarchy, skipping parts of the hierarchy that have not been
// modified.
type ApplyGetRawDirectory struct {
	RawDirectory model_core.Message[*model_filesystem_pb.Directory, object.LocalReference]
	Err          error
}

func (icf *objectBackedInitialContentsFetcher) VirtualApply(data any) bool {
	switch typedData := data.(type) {
	case *ApplyGetRawDirectory:
		if directory, err := icf.getDirectory(); err == nil {
			typedData.RawDirectory = model_core.NewNestedMessage(directory, directory.Message.Directory)
		} else {
			typedData.Err = err
		}
		return true
	default:
		return false
	}
}
