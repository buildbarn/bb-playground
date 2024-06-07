package main

import (
	"context"
	"os"

	"github.com/buildbarn/bb-playground/pkg/encoding"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_filesystem_virtual "github.com/buildbarn/bb-playground/pkg/model/filesystem/virtual"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	"github.com/buildbarn/bb-playground/pkg/proto/configuration/mount_directory"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	object_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	object_grpc "github.com/buildbarn/bb-playground/pkg/storage/object/grpc"
	object_namespacemapping "github.com/buildbarn/bb-playground/pkg/storage/object/namespacemapping"
	virtual_configuration "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/configuration"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/global"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: mount_directory mount_directory.jsonnet")
		}
		var configuration mount_directory.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}
		lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global)
		if err != nil {
			return util.StatusWrap(err, "Failed to apply global configuration options")
		}

		mount, rootHandleAllocator, err := virtual_configuration.NewMountFromConfiguration(
			configuration.Mount,
			"mount_directory",
			/* rootDirectory = */ virtual_configuration.LongAttributeCaching,
			/* childDirectories = */ virtual_configuration.LongAttributeCaching,
			/* leaves = */ virtual_configuration.LongAttributeCaching,
		)
		if err != nil {
			return util.StatusWrap(err, "Failed to create virtual file system mount")
		}

		namespace, err := object.NewNamespace(configuration.Namespace)
		if err != nil {
			return util.StatusWrap(err, "Invalid namespace")
		}

		grpcClient, err := grpcClientFactory.NewClientFromConfiguration(configuration.GrpcClient)
		if err != nil {
			return util.StatusWrap(err, "Failed to create gRPC client")
		}
		downloader := object_namespacemapping.NewNamespaceAddingDownloader(
			object_grpc.NewGRPCDownloader(object_pb.NewDownloaderClient(grpcClient)),
			namespace,
		)

		maximumObjectSizeBytes := uint32(namespace.GetMaximumObjectSizeBytes())
		directoryEncoder, err := encoding.NewBinaryEncoderFromConfiguration(configuration.DirectoryEncoders, maximumObjectSizeBytes)
		if err != nil {
			return util.StatusWrap(err, "Failed to create directory encoder")
		}
		smallFileEncoder, err := encoding.NewBinaryEncoderFromConfiguration(configuration.SmallFileEncoders, maximumObjectSizeBytes)
		if err != nil {
			return util.StatusWrap(err, "Failed to create small file encoder")
		}
		concatenatedFileEncoder, err := encoding.NewBinaryEncoderFromConfiguration(configuration.ConcatenatedFileEncoders, maximumObjectSizeBytes)
		if err != nil {
			return util.StatusWrap(err, "Failed to create concatenated file encoder")
		}

		parsedObjectEvictionSet, err := eviction.NewSetFromConfiguration[model_parser.CachedParsedObjectEvictionKey[object.LocalReference]](configuration.ParsedObjectCacheReplacementPolicy)
		if err != nil {
			return util.StatusWrap(err, "Failed to create eviction set for directories")
		}
		cachedParsedObjectPool := model_parser.NewCachedParsedObjectPool(
			parsedObjectEvictionSet,
			int(configuration.ParsedObjectCacheCount),
			int(configuration.ParsedObjectCacheTotalSizeBytes),
		)

		leavesReader := model_parser.NewStorageBackedParsedObjectReader(
			downloader,
			directoryEncoder,
			model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Leaves](),
		)
		directoryClusterReader := model_parser.NewCachingParsedObjectReader(
			model_parser.NewStorageBackedParsedObjectReader(
				downloader,
				directoryEncoder,
				model_filesystem.NewDirectoryClusterObjectParser(leavesReader),
			),
			cachedParsedObjectPool,
		)
		fileContentsListReader := model_parser.NewCachingParsedObjectReader(
			model_parser.NewStorageBackedParsedObjectReader(
				downloader,
				concatenatedFileEncoder,
				model_filesystem.NewFileContentsListObjectParser[object.LocalReference](),
			),
			cachedParsedObjectPool,
		)
		fileChunkReader := model_parser.NewCachingParsedObjectReader(
			model_parser.NewStorageBackedParsedObjectReader(
				downloader,
				smallFileEncoder,
				model_parser.NewRawObjectParser[object.LocalReference](),
			),
			cachedParsedObjectPool,
		)

		rootDirectoryReference, err := namespace.NewGlobalReference(configuration.RootDirectoryReference)
		if err != nil {
			return util.StatusWrap(err, "Invalid root directory reference")
		}
		rootDirectoryLocalReference := rootDirectoryReference.GetLocalReference()

		rootDirectoryIndex := uint(0)
		rootDirectoryCluster, _, err := directoryClusterReader.ReadParsedObject(ctx, rootDirectoryLocalReference)
		if err != nil {
			return util.StatusWrap(err, "Failed to fetch contents of root directory")
		}

		fileFactory := model_filesystem_virtual.NewObjectBackedFileFactory(
			rootHandleAllocator.New(),
			fileContentsListReader,
			fileChunkReader,
			util.DefaultErrorLogger,
		)
		directoryFactory := model_filesystem_virtual.NewObjectBackedDirectoryFactory(
			rootHandleAllocator.New(),
			directoryClusterReader,
			fileFactory,
			util.DefaultErrorLogger,
		)
		rootDirectory := directoryFactory.LookupDirectory(model_filesystem.DirectoryInfo{
			ClusterReference: rootDirectoryLocalReference,
			DirectoryIndex:   rootDirectoryIndex,
			DirectoriesCount: uint32(len(rootDirectoryCluster[0].Directories)),
		})

		if err := mount.Expose(siblingsGroup, rootDirectory); err != nil {
			return util.StatusWrap(err, "Failed to expose virtual file system mount")
		}

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}
