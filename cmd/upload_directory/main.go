package main

import (
	"context"
	"encoding/base64"
	"log"
	"os"
	"runtime"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	"github.com/buildbarn/bb-playground/pkg/proto/configuration/upload_directory"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	dag_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: upload_directory upload_directory.jsonnet")
		}
		var configuration upload_directory.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}

		directory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(configuration.Path))
		if err != nil {
			return util.StatusWrap(err, "Failed to open directory to upload")
		}

		namespace, err := object.NewNamespace(configuration.Namespace)
		if err != nil {
			return util.StatusWrap(err, "Invalid namespace")
		}

		directoryParameters, err := model_filesystem.NewDirectoryCreationParametersFromProto(configuration.DirectoryCreationParameters, namespace.ReferenceFormat)
		if err != nil {
			return util.StatusWrap(err, "Invalid directory creation parameters")
		}
		fileParameters, err := model_filesystem.NewFileCreationParametersFromProto(configuration.FileCreationParameters, namespace.ReferenceFormat)
		if err != nil {
			return util.StatusWrap(err, "Invalid file creation parameters")
		}

		group, groupCtx := errgroup.WithContext(ctx)
		var rootDirectoryMessage model_core.MessageWithReferences[*model_filesystem_pb.Directory, model_filesystem.CapturedObject]
		group.Go(func() error {
			return model_filesystem.CreateDirectoryMerkleTree(
				groupCtx,
				semaphore.NewWeighted(int64(runtime.NumCPU())),
				group,
				directoryParameters,
				fileParameters,
				directory,
				model_filesystem.FileDiscardingDirectoryMerkleTreeCapturer,
				&rootDirectoryMessage,
			)
		})
		if err := group.Wait(); err != nil {
			return util.StatusWrap(err, "Failed to construct directory Merkle tree")
		}

		rootReferences, rootChildren := rootDirectoryMessage.Patcher.SortAndSetReferences()
		rootDirectoryObjectContents, err := directoryParameters.EncodeDirectory(rootReferences, rootDirectoryMessage.Message)
		if err != nil {
			return util.StatusWrap(err, "Failed to get computed directory object for root directory")
		}
		rootDirectoryObject := model_filesystem.CapturedObject{
			Contents: rootDirectoryObjectContents,
			Children: rootChildren,
		}

		rootDirectoryReference := rootDirectoryObject.Contents.GetReference()
		log.Printf("Starting upload of directory with reference %s / %s", rootDirectoryReference, base64.StdEncoding.EncodeToString(rootDirectoryReference.GetRawReference()))

		grpcClientFactory := grpc.NewBaseClientFactory(grpc.BaseClientDialer, nil, nil)
		grpcClient, err := grpcClientFactory.NewClientFromConfiguration(configuration.GrpcClient)
		if err != nil {
			return util.StatusWrap(err, "Failed to create gRPC client")
		}

		if configuration.MaximumUnfinalizedParentsLimit == nil {
			return status.Error(codes.InvalidArgument, "No maximum unfinalized parents limit provided")
		}
		maximumUnfinalizedParentsLimit := object.NewLimit(configuration.MaximumUnfinalizedParentsLimit)

		if err := dag.UploadDAG(
			ctx,
			dag_pb.NewUploaderClient(grpcClient),
			object.GlobalReference{
				InstanceName:   namespace.InstanceName,
				LocalReference: rootDirectoryObject.Contents.GetReference(),
			},
			model_filesystem.NewCapturedDirectoryWalker(
				directoryParameters.DirectoryAccessParameters,
				fileParameters,
				directory,
				&rootDirectoryObject,
			),
			semaphore.NewWeighted(int64(runtime.NumCPU())),
			maximumUnfinalizedParentsLimit,
		); err != nil {
			return util.StatusWrap(err, "Failed to upload directory")
		}
		return nil
	})
}
