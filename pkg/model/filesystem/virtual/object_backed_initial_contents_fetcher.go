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
	directoryClusterReader model_parser.ParsedObjectReader[object.LocalReference, model_filesystem.DirectoryCluster]
	fileFactory            FileFactory
	symlinkFactory         virtual.SymlinkFactory
}

type objectBackedInitialContentsFetcher struct {
	options          *objectBackedInitialContentsFetcherOptions
	clusterReference object.LocalReference
	directoryIndex   uint
}

func NewObjectBackedInitialContentsFetcher(ctx context.Context, directoryClusterReader model_parser.ParsedObjectReader[object.LocalReference, model_filesystem.DirectoryCluster], fileFactory FileFactory, symlinkFactory virtual.SymlinkFactory, rootClusterReference object.LocalReference) virtual.InitialContentsFetcher {
	return &objectBackedInitialContentsFetcher{
		options: &objectBackedInitialContentsFetcherOptions{
			context:                ctx,
			directoryClusterReader: directoryClusterReader,
			fileFactory:            fileFactory,
			symlinkFactory:         symlinkFactory,
		},
		clusterReference: rootClusterReference,
	}
}

func (icf *objectBackedInitialContentsFetcher) getDirectory() (*model_filesystem.Directory, error) {
	options := icf.options
	cluster, _, err := options.directoryClusterReader.ReadParsedObject(options.context, icf.clusterReference)
	if err != nil {
		return nil, util.StatusWrapf(err, "Failed to fetch directory cluster with reference %s", icf.clusterReference)
	}
	if icf.directoryIndex >= uint(len(cluster)) {
		return nil, status.Errorf(codes.InvalidArgument, "Directory index %d is out of range, as directory cluster with reference %s only contains %d directories", icf.directoryIndex, len(cluster), icf.clusterReference)
	}
	return &cluster[icf.directoryIndex], nil
}

func (icf *objectBackedInitialContentsFetcher) FetchContents(fileReadMonitorFactory virtual.FileReadMonitorFactory) (map[path.Component]virtual.InitialChild, error) {
	directory, err := icf.getDirectory()
	if err != nil {
		return nil, err
	}

	// Create InitialContentsFetchers for all child directories.
	// These can yield even more InitialContentsFetchers for
	// grandchildren.
	options := icf.options
	children := make(map[path.Component]virtual.InitialChild, len(directory.Directories)+len(directory.Leaves.Message.Files)+len(directory.Leaves.Message.Symlinks))
	for _, entry := range directory.Directories {
		component := entry.Name
		if _, ok := children[component]; ok {
			return nil, status.Errorf(codes.InvalidArgument, "Directory contains multiple children named %#v", entry.Name)
		}

		children[component] = virtual.InitialChild{}.FromDirectory(&objectBackedInitialContentsFetcher{
			options:          options,
			clusterReference: entry.Info.ClusterReference,
			directoryIndex:   entry.Info.DirectoryIndex,
		})
	}

	// Ensure that leaves are properly unlinked if this method fails.
	leavesToUnlink := make([]virtual.LinkableLeaf, 0, len(directory.Leaves.Message.Files)+len(directory.Leaves.Message.Symlinks))
	defer func() {
		for _, leaf := range leavesToUnlink {
			leaf.Unlink()
		}
	}()

	// Create storage backed read-only files.
	for _, entry := range directory.Leaves.Message.Files {
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
			model_core.NewNestedMessage(directory.Leaves, properties.Contents),
			icf.clusterReference.GetReferenceFormat(),
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
	for _, entry := range directory.Leaves.Message.Symlinks {
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
	RawDirectory model_core.Message[*model_filesystem_pb.Directory, object.OutgoingReferences]
	Err          error
}

func (icf *objectBackedInitialContentsFetcher) VirtualApply(data any) bool {
	switch typedData := data.(type) {
	case *ApplyGetRawDirectory:
		if directory, err := icf.getDirectory(); err == nil {
			typedData.RawDirectory = directory.Raw
		} else {
			typedData.Err = err
		}
		return true
	default:
		return false
	}
}
