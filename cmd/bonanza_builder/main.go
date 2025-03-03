package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"

	re_filesystem "github.com/buildbarn/bb-remote-execution/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_http "github.com/buildbarn/bb-storage/pkg/http"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/evaluation"
	model_analysis "github.com/buildbarn/bonanza/pkg/model/analysis"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/encoding"
	"github.com/buildbarn/bonanza/pkg/proto/configuration/bonanza_builder"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_build_pb "github.com/buildbarn/bonanza/pkg/proto/model/build"
	model_command_pb "github.com/buildbarn/bonanza/pkg/proto/model/command"
	remoteexecution_pb "github.com/buildbarn/bonanza/pkg/proto/remoteexecution"
	remoteworker_pb "github.com/buildbarn/bonanza/pkg/proto/remoteworker"
	dag_pb "github.com/buildbarn/bonanza/pkg/proto/storage/dag"
	object_pb "github.com/buildbarn/bonanza/pkg/proto/storage/object"
	remoteexecution "github.com/buildbarn/bonanza/pkg/remoteexecution"
	"github.com/buildbarn/bonanza/pkg/remoteworker"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"
	object_grpc "github.com/buildbarn/bonanza/pkg/storage/object/grpc"
	object_namespacemapping "github.com/buildbarn/bonanza/pkg/storage/object/namespacemapping"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: bonanza_builder bonanza_builder.jsonnet")
		}
		var configuration bonanza_builder.ApplicationConfiguration
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

		executionClientPrivateKey, err := remoteexecution.ParseECDHPrivateKey([]byte(configuration.ExecutionClientPrivateKey))
		if err != nil {
			return util.StatusWrap(err, "Failed to parse execution client private key")
		}
		executionClientCertificateChain, err := remoteexecution.ParseCertificateChain([]byte(configuration.ExecutionClientCertificateChain))
		if err != nil {
			return util.StatusWrap(err, "Failed to parse execution client certificate chain")
		}

		remoteWorkerConnection, err := grpcClientFactory.NewClientFromConfiguration(configuration.RemoteWorkerGrpcClient)
		if err != nil {
			return util.StatusWrap(err, "Failed to create remote worker RPC client")
		}
		remoteWorkerClient := remoteworker_pb.NewOperationQueueClient(remoteWorkerConnection)

		platformPrivateKeys, err := remoteworker.ParsePlatformPrivateKeys(configuration.PlatformPrivateKeys)
		if err != nil {
			return err
		}
		clientCertificateAuthorities, err := remoteworker.ParseClientCertificateAuthorities(configuration.ClientCertificateAuthorities)
		if err != nil {
			return err
		}
		workerName, err := json.Marshal(configuration.WorkerId)
		if err != nil {
			return util.StatusWrap(err, "Failed to marshal worker ID")
		}

		executor := &builderExecutor{
			objectDownloader:              objectDownloader,
			dagUploaderClient:             dag_pb.NewUploaderClient(storageGRPCClient),
			objectContentsWalkerSemaphore: semaphore.NewWeighted(int64(runtime.NumCPU())),
			httpClient: &http.Client{
				Transport: bb_http.NewMetricsRoundTripper(roundTripper, "Builder"),
			},
			filePool:       filePool,
			cacheDirectory: cacheDirectory,
			executionClient: remoteexecution.NewClient[*model_command_pb.Action, emptypb.Empty, *model_command_pb.Result](
				remoteexecution_pb.NewExecutionClient(executionGRPCClient),
				executionClientPrivateKey,
				executionClientCertificateChain,
			),
		}
		client, err := remoteworker.NewClient(
			remoteWorkerClient,
			executor,
			clock.SystemClock,
			random.CryptoThreadSafeGenerator,
			platformPrivateKeys,
			clientCertificateAuthorities,
			configuration.WorkerId,
			/* sizeClass = */ 0,
			/* isLargestSizeClass = */ true,
		)
		if err != nil {
			return util.StatusWrap(err, "Failed to create remote worker client")
		}
		remoteworker.LaunchWorkerThread(siblingsGroup, client.Run, string(workerName))

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}

type builderExecutor struct {
	objectDownloader              object.Downloader[object.GlobalReference]
	dagUploaderClient             dag_pb.UploaderClient
	objectContentsWalkerSemaphore *semaphore.Weighted
	httpClient                    *http.Client
	filePool                      re_filesystem.FilePool
	cacheDirectory                filesystem.Directory
	executionClient               *remoteexecution.Client[*model_command_pb.Action, emptypb.Empty, *emptypb.Empty, *model_command_pb.Result]
}

func (e *builderExecutor) CheckReadiness(ctx context.Context) error {
	return nil
}

func (e *builderExecutor) Execute(ctx context.Context, action *model_build_pb.Action, executionTimeout time.Duration, executionEvents chan<- proto.Message) (proto.Message, time.Duration, remoteworker_pb.CurrentState_Completed_Result) {
	namespace, err := object.NewNamespace(action.Namespace)
	if err != nil {
		return &model_build_pb.Result{
			Status: status.Convert(util.StatusWrap(err, "Invalid namespace")).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}
	instanceName := namespace.InstanceName
	objectDownloader := object_namespacemapping.NewNamespaceAddingDownloader(e.objectDownloader, namespace)
	buildSpecificationReference, err := namespace.NewGlobalReference(action.BuildSpecificationReference)
	if err != nil {
		return &model_build_pb.Result{
			Status: status.Convert(util.StatusWrap(err, "Invalid build specification reference")).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}
	buildSpecificationEncoder, err := encoding.NewBinaryEncoderFromProto(
		action.BuildSpecificationEncoders,
		uint32(namespace.ReferenceFormat.GetMaximumObjectSizeBytes()),
	)
	if err != nil {
		return &model_build_pb.Result{
			Status: status.Convert(util.StatusWrap(err, "Invalid build specification encoder")).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}
	value, err := evaluation.FullyComputeValue(
		ctx,
		model_analysis.NewTypedComputer(model_analysis.NewBaseComputer(
			objectDownloader,
			buildSpecificationReference,
			buildSpecificationEncoder,
			e.httpClient,
			e.filePool,
			e.cacheDirectory,
			e.executionClient,
		)),
		model_core.NewMessage[proto.Message, object.OutgoingReferences](&model_analysis_pb.BuildResult_Key{}, object.OutgoingReferencesList{}),
		func(references []object.LocalReference, objectContentsWalkers []dag.ObjectContentsWalker) error {
			for i, reference := range references {
				if err := dag.UploadDAG(
					ctx,
					e.dagUploaderClient,
					object.GlobalReference{
						InstanceName:   instanceName,
						LocalReference: reference,
					},
					objectContentsWalkers[i],
					e.objectContentsWalkerSemaphore,
					// Assume everything we attempt
					// to upload is memory backed.
					object.Unlimited,
				); err != nil {
					return fmt.Errorf("failed to store DAG with reference %e: %w", reference.String(), err)
				}
			}
			return nil
		},
	)
	if err != nil {
		return &model_build_pb.Result{
			Status: status.Convert(err).Proto(),
		}, 0, remoteworker_pb.CurrentState_Completed_FAILED
	}
	return &model_build_pb.Result{
		Status: status.Newf(codes.Internal, "TODO: %s", value).Proto(),
	}, 0, remoteworker_pb.CurrentState_Completed_FAILED
}
