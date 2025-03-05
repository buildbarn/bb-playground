package main

import (
	"context"
	"os"

	virtual_configuration "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/configuration"
	"github.com/buildbarn/bb-storage/pkg/global"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/model/encoding"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_filesystem_virtual "github.com/buildbarn/bonanza/pkg/model/filesystem/virtual"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	"github.com/buildbarn/bonanza/pkg/proto/configuration/mount_directory"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	object_pb "github.com/buildbarn/bonanza/pkg/proto/storage/object"
	"github.com/buildbarn/bonanza/pkg/storage/object"
	object_grpc "github.com/buildbarn/bonanza/pkg/storage/object/grpc"
	object_namespacemapping "github.com/buildbarn/bonanza/pkg/storage/object/namespacemapping"

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

		maximumObjectSizeBytes := uint32(namespace.GetMaximumObjectSizeBytes())
		directoryEncoder, err := encoding.NewBinaryEncoderFromProto(configuration.DirectoryEncoders, maximumObjectSizeBytes)
		if err != nil {
			return util.StatusWrap(err, "Failed to create directory encoder")
		}
		smallFileEncoder, err := encoding.NewBinaryEncoderFromProto(configuration.SmallFileEncoders, maximumObjectSizeBytes)
		if err != nil {
			return util.StatusWrap(err, "Failed to create small file encoder")
		}
		concatenatedFileEncoder, err := encoding.NewBinaryEncoderFromProto(configuration.ConcatenatedFileEncoders, maximumObjectSizeBytes)
		if err != nil {
			return util.StatusWrap(err, "Failed to create concatenated file encoder")
		}

		parsedObjectPool, err := model_parser.NewParsedObjectPoolFromConfiguration(configuration.ParsedObjectPool)
		if err != nil {
			return util.StatusWrap(err, "Failed to create parsed object pool")
		}
		parsedObjectFetcher := model_parser.NewParsedObjectFetcher(
			parsedObjectPool,
			object_namespacemapping.NewNamespaceAddingDownloader(
				object_grpc.NewGRPCDownloader(object_pb.NewDownloaderClient(grpcClient)),
				namespace,
			),
		)
		leavesReader := model_parser.LookupParsedObjectReader(
			parsedObjectFetcher,
			model_parser.NewChainedObjectParser(
				model_parser.NewEncodedObjectParser[object.LocalReference](directoryEncoder),
				model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Leaves](),
			),
		)
		directoryClusterReader := model_parser.LookupParsedObjectReader(
			parsedObjectFetcher,
			model_parser.NewChainedObjectParser(
				model_parser.NewEncodedObjectParser[object.LocalReference](directoryEncoder),
				model_filesystem.NewDirectoryClusterObjectParser[object.LocalReference](),
			),
		)
		fileContentsListReader := model_parser.LookupParsedObjectReader(
			parsedObjectFetcher,
			model_parser.NewChainedObjectParser(
				model_parser.NewEncodedObjectParser[object.LocalReference](concatenatedFileEncoder),
				model_filesystem.NewFileContentsListObjectParser[object.LocalReference](),
			),
		)
		fileChunkReader := model_parser.LookupParsedObjectReader(
			parsedObjectFetcher,
			model_parser.NewChainedObjectParser(
				model_parser.NewEncodedObjectParser[object.LocalReference](smallFileEncoder),
				model_parser.NewRawObjectParser[object.LocalReference](),
			),
		)

		rootDirectoryReference, err := namespace.NewGlobalReference(configuration.RootDirectoryReference)
		if err != nil {
			return util.StatusWrap(err, "Invalid root directory reference")
		}
		rootDirectoryLocalReference := rootDirectoryReference.GetLocalReference()

		rootDirectoryIndex := uint(0)
		rootDirectoryCluster, err := directoryClusterReader.ReadParsedObject(ctx, rootDirectoryLocalReference)
		if err != nil {
			return util.StatusWrap(err, "Failed to fetch contents of root directory")
		}

		fileFactory := model_filesystem_virtual.NewResolvableHandleAllocatingFileFactory(
			model_filesystem_virtual.NewObjectBackedFileFactory(
				context.Background(),
				model_filesystem.NewFileReader(
					fileContentsListReader,
					fileChunkReader,
				),
				util.DefaultErrorLogger,
			),
			rootHandleAllocator.New(),
		)
		directoryFactory := model_filesystem_virtual.NewObjectBackedDirectoryFactory(
			rootHandleAllocator.New(),
			directoryClusterReader,
			leavesReader,
			fileFactory,
			util.DefaultErrorLogger,
		)
		rootDirectory := directoryFactory.LookupDirectory(
			rootDirectoryLocalReference,
			rootDirectoryIndex,
			uint32(len(rootDirectoryCluster.Message[0].Directory.Directories)),
		)

		if err := mount.Expose(siblingsGroup, rootDirectory); err != nil {
			return util.StatusWrap(err, "Failed to expose virtual file system mount")
		}

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}
