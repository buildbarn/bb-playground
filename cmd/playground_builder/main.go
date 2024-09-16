package main

import (
	"context"
	"os"

	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/encoding"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	build_pb "github.com/buildbarn/bb-playground/pkg/proto/build"
	"github.com/buildbarn/bb-playground/pkg/proto/configuration/playground_builder"
	model_build_pb "github.com/buildbarn/bb-playground/pkg/proto/model/build"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	object_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	object_grpc "github.com/buildbarn/bb-playground/pkg/storage/object/grpc"
	object_namespacemapping "github.com/buildbarn/bb-playground/pkg/storage/object/namespacemapping"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: playground_builder playground_builder.jsonnet")
		}
		var configuration playground_builder.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}
		lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global)
		if err != nil {
			return util.StatusWrap(err, "Failed to apply global configuration options")
		}

		storageGRPCClient, err := grpcClientFactory.NewClientFromConfiguration(configuration.StorageGrpcClient)
		if err != nil {
			return util.StatusWrap(err, "Failed to create storage gRPC client")
		}
		objectDownloader := object_grpc.NewGRPCDownloader(
			object_pb.NewDownloaderClient(storageGRPCClient),
		)

		if err := bb_grpc.NewServersFromConfigurationAndServe(
			configuration.GrpcServers,
			func(s grpc.ServiceRegistrar) {
				build_pb.RegisterBuilderServer(s, &builderServer{
					objectDownloader: objectDownloader,
				})
			},
			siblingsGroup,
		); err != nil {
			return util.StatusWrap(err, "gRPC server failure")
		}

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}

type builderServer struct {
	objectDownloader object.Downloader[object.GlobalReference]
}

func (s *builderServer) PerformBuild(request *build_pb.PerformBuildRequest, server build_pb.Builder_PerformBuildServer) error {
	ctx := server.Context()

	namespace, err := object.NewNamespace(request.Namespace)
	if err != nil {
		return util.StatusWrap(err, "Invalid namespace")
	}
	buildSpecificationReference, err := namespace.NewGlobalReference(request.BuildSpecificationReference)
	if err != nil {
		return util.StatusWrap(err, "Invalid build specification reference")
	}
	buildSpecificationEncoder, err := encoding.NewBinaryEncoderFromProto(request.BuildSpecificationEncoders, uint32(namespace.ReferenceFormat.GetMaximumObjectSizeBytes()))
	if err != nil {
		return util.StatusWrap(err, "Invalid build specification encoders")
	}

	objectDownloader := object_namespacemapping.NewNamespaceAddingDownloader(s.objectDownloader, namespace)
	buildSpecificationReader := model_parser.NewStorageBackedParsedObjectReader(
		objectDownloader,
		buildSpecificationEncoder,
		model_parser.NewMessageObjectParser[object.LocalReference, model_build_pb.BuildSpecification](),
	)
	buildSpecification, _, err := buildSpecificationReader.ReadParsedObject(ctx, buildSpecificationReference.LocalReference)
	if err != nil {
		return util.StatusWrap(err, "Failed to read build specification")
	}

	buildSpecificationDegree := buildSpecificationReference.GetDegree()
	moduleRootDirectoryReferences := map[label.Module]object.LocalReference{}
	for _, module := range buildSpecification.Message.Modules {
		moduleName, err := label.NewModule(module.Name)
		if err != nil {
			return util.StatusWrapf(err, "Invalid module name %#v", module.Name)
		}
		rootDirectoryIndex, err := model_core.GetIndexFromReferenceMessage(module.RootDirectoryReference, buildSpecificationDegree)
		if err != nil {
			return util.StatusWrapf(err, "Invalid root directory reference for module %#v", module.Name)
		}
		moduleRootDirectoryReferences[moduleName] = buildSpecification.OutgoingReferences.GetOutgoingReference(rootDirectoryIndex)
	}

	rootModuleName, err := label.NewModule(buildSpecification.Message.RootModuleName)
	if err != nil {
		return util.StatusWrapf(err, "Invalid root module name %#v", buildSpecification.Message.RootModuleName)
	}
	moduleRootDirectoryReference, ok := moduleRootDirectoryReferences[rootModuleName]
	if !ok {
		return util.StatusWrapf(err, "No root directory provided for root module %#v", buildSpecification.Message.RootModuleName)
	}

	directoryParameters, err := model_filesystem.NewDirectoryCreationParametersFromProto(buildSpecification.Message.DirectoryCreationParameters, namespace.ReferenceFormat)
	if err != nil {
		return util.StatusWrap(err, "Invalid directory creation parameters")
	}
	fileParameters, err := model_filesystem.NewFileCreationParametersFromProto(buildSpecification.Message.FileCreationParameters, namespace.ReferenceFormat)
	if err != nil {
		return util.StatusWrap(err, "Invalid file creation parameters")
	}

	moduleDotBazelResolver := model_filesystem.NewDirectoryMerkleTreeFileResolver(
		ctx,
		model_parser.NewStorageBackedParsedObjectReader(
			objectDownloader,
			directoryParameters.GetEncoder(),
			model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Directory](),
		),
		model_parser.NewStorageBackedParsedObjectReader(
			objectDownloader,
			directoryParameters.GetEncoder(),
			model_parser.NewMessageObjectParser[object.LocalReference, model_filesystem_pb.Leaves](),
		),
		moduleRootDirectoryReference,
	)
	if err := path.Resolve(
		path.UNIXFormat.NewParser("MODULE.bazel"),
		path.NewLoopDetectingScopeWalker(path.NewRelativeScopeWalker(moduleDotBazelResolver)),
	); err != nil {
		return util.StatusWrapf(err, "Failed to resolve MODULE.bazel inside root module %#v", buildSpecification.Message.RootModuleName)
	}

	fileReader := model_filesystem.NewFileReader(
		model_parser.NewStorageBackedParsedObjectReader(
			objectDownloader,
			fileParameters.GetFileContentsListEncoder(),
			model_filesystem.NewFileContentsListObjectParser[object.LocalReference](),
		),
		model_parser.NewStorageBackedParsedObjectReader(
			objectDownloader,
			fileParameters.GetChunkEncoder(),
			model_parser.NewRawObjectParser[object.LocalReference](),
		),
	)

	fileNode := moduleDotBazelResolver.GetFileNode()
	fileContents, err := model_filesystem.NewFileContentsEntryFromProto(
		model_parser.ParsedMessage[*model_filesystem_pb.FileContents]{
			Message:            fileNode.Message.Contents,
			OutgoingReferences: fileNode.OutgoingReferences,
		},
		namespace.ReferenceFormat,
	)
	if err != nil {
		return util.StatusWrap(err, "Invalid file contents for MODULE.bazel")
	}
	data, err := fileReader.FileReadAll(ctx, fileContents, 1<<20)
	if err != nil {
		return util.StatusWrap(err, "Failed to read MODULE.bazel")
	}

	return status.Error(codes.Unimplemented, string(data))
}
