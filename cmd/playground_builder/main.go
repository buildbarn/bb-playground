package main

import (
	"context"
	"crypto/ecdh"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"runtime"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_analysis "github.com/buildbarn/bb-playground/pkg/model/analysis"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/encoding"
	build_pb "github.com/buildbarn/bb-playground/pkg/proto/build"
	"github.com/buildbarn/bb-playground/pkg/proto/configuration/playground_builder"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	remoteexecution_pb "github.com/buildbarn/bb-playground/pkg/proto/remoteexecution"
	dag_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/dag"
	object_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	object_grpc "github.com/buildbarn/bb-playground/pkg/storage/object/grpc"
	object_namespacemapping "github.com/buildbarn/bb-playground/pkg/storage/object/namespacemapping"
	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	bb_http "github.com/buildbarn/bb-storage/pkg/http"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

		roundTripper, err := bb_http.NewRoundTripperFromConfiguration(configuration.HttpClient)
		if err != nil {
			return util.StatusWrap(err, "Failed to create HTTP client")
		}

		filePool, err := re_filesystem.NewFilePoolFromConfiguration(configuration.FilePool)
		if err != nil {
			return util.StatusWrap(err, "Failed to create file pool")
		}

		cacheDirectory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(configuration.CacheDirectoryPath))
		if err != nil {
			return util.StatusWrap(err, "Failed to create cache directory")
		}

		executionGRPCClient, err := grpcClientFactory.NewClientFromConfiguration(configuration.ExecutionGrpcClient)
		if err != nil {
			return util.StatusWrap(err, "Failed to create execution gRPC client")
		}

		executionClientPrivateKeyBlock, _ := pem.Decode([]byte(configuration.ExecutionClientPrivateKey))
		if executionClientPrivateKeyBlock == nil {
			return status.Error(codes.InvalidArgument, "Execution client private key does not contain a PEM block")
		}
		if executionClientPrivateKeyBlock.Type != "PRIVATE KEY" {
			return status.Error(codes.InvalidArgument, "Execution client private key PEM block is not of type PRIVATE KEY")
		}
		executionClientPrivateKey, err := x509.ParsePKCS8PrivateKey(executionClientPrivateKeyBlock.Bytes)
		if err != nil {
			return util.StatusWrap(err, "Failed to parse execution client private key")
		}
		executionClientECDHPrivateKey, ok := executionClientPrivateKey.(*ecdh.PrivateKey)
		if !ok {
			return status.Error(codes.InvalidArgument, "Execution client private key is not an ECDH private key")
		}
		var executionClientCertificates [][]byte
		for certificateBlock, remainder := pem.Decode([]byte(configuration.ExecutionClientCertificateChain)); certificateBlock != nil; certificateBlock, remainder = pem.Decode(remainder) {
			if certificateBlock.Type != "CERTIFICATE" {
				return status.Error(codes.InvalidArgument, "Execution client certificate PEM block is not of type CERTIFICATE")
			}
			executionClientCertificates = append(executionClientCertificates, certificateBlock.Bytes)
		}

		if err := bb_grpc.NewServersFromConfigurationAndServe(
			configuration.GrpcServers,
			func(s grpc.ServiceRegistrar) {
				build_pb.RegisterBuilderServer(s, &builderServer{
					objectDownloader:              objectDownloader,
					dagUploaderClient:             dag_pb.NewUploaderClient(storageGRPCClient),
					objectContentsWalkerSemaphore: semaphore.NewWeighted(int64(runtime.NumCPU())),
					httpClient: &http.Client{
						Transport: bb_http.NewMetricsRoundTripper(roundTripper, "Builder"),
					},
					filePool:                    filePool,
					cacheDirectory:              cacheDirectory,
					executionClient:             remoteexecution_pb.NewExecutionClient(executionGRPCClient),
					executionClientPrivateKey:   executionClientECDHPrivateKey,
					executionClientCertificates: executionClientCertificates,
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
	objectDownloader              object.Downloader[object.GlobalReference]
	dagUploaderClient             dag_pb.UploaderClient
	objectContentsWalkerSemaphore *semaphore.Weighted
	httpClient                    *http.Client
	filePool                      re_filesystem.FilePool
	cacheDirectory                filesystem.Directory
	executionClient               remoteexecution_pb.ExecutionClient
	executionClientPrivateKey     *ecdh.PrivateKey
	executionClientCertificates   [][]byte
}

func (s *builderServer) PerformBuild(request *build_pb.PerformBuildRequest, server build_pb.Builder_PerformBuildServer) error {
	ctx := server.Context()

	namespace, err := object.NewNamespace(request.Namespace)
	if err != nil {
		return util.StatusWrap(err, "Invalid namespace")
	}
	instanceName := namespace.InstanceName
	objectDownloader := object_namespacemapping.NewNamespaceAddingDownloader(s.objectDownloader, namespace)
	buildSpecificationReference, err := namespace.NewGlobalReference(request.BuildSpecificationReference)
	if err != nil {
		return util.StatusWrap(err, "Invalid build specification reference")
	}
	buildSpecificationEncoder, err := encoding.NewBinaryEncoderFromProto(
		request.BuildSpecificationEncoders,
		uint32(namespace.ReferenceFormat.GetMaximumObjectSizeBytes()),
	)
	if err != nil {
		return util.StatusWrap(err, "Invalid build specification encoders")
	}
	value, err := evaluation.FullyComputeValue(
		ctx,
		model_analysis.NewTypedComputer(model_analysis.NewBaseComputer(
			objectDownloader,
			buildSpecificationReference,
			buildSpecificationEncoder,
			s.httpClient,
			s.filePool,
			s.cacheDirectory,
			s.executionClient,
			s.executionClientPrivateKey,
			s.executionClientCertificates,
		)),
		model_core.NewSimpleMessage[proto.Message](&model_analysis_pb.BuildResult_Key{}),
		func(references []object.LocalReference, objectContentsWalkers []dag.ObjectContentsWalker) error {
			for i, reference := range references {
				if err := dag.UploadDAG(
					ctx,
					s.dagUploaderClient,
					object.GlobalReference{
						InstanceName:   instanceName,
						LocalReference: reference,
					},
					objectContentsWalkers[i],
					s.objectContentsWalkerSemaphore,
					// Assume everything we attempt
					// to upload is memory backed.
					object.Unlimited,
				); err != nil {
					return fmt.Errorf("failed to store DAG with reference %s: %w", reference.String(), err)
				}
			}
			return nil
		},
	)
	if err != nil {
		return err
	}
	return status.Errorf(codes.Internal, "XXX: %s", value)
}
