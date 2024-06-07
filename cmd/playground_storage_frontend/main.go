package main

import (
	"context"
	"os"

	"github.com/buildbarn/bb-playground/pkg/proto/configuration/playground_storage_frontend"
	dag_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/dag"
	object_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	tag_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/tag"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	object_grpc "github.com/buildbarn/bb-playground/pkg/storage/object/grpc"
	object_leaserenewing "github.com/buildbarn/bb-playground/pkg/storage/object/leaserenewing"
	object_mirrored "github.com/buildbarn/bb-playground/pkg/storage/object/mirrored"
	object_sharded "github.com/buildbarn/bb-playground/pkg/storage/object/sharded"
	"github.com/buildbarn/bb-playground/pkg/storage/tag"
	tag_grpc "github.com/buildbarn/bb-playground/pkg/storage/tag/grpc"
	tag_leaserenewing "github.com/buildbarn/bb-playground/pkg/storage/tag/leaserenewing"
	tag_mirrored "github.com/buildbarn/bb-playground/pkg/storage/tag/mirrored"
	tag_sharded "github.com/buildbarn/bb-playground/pkg/storage/tag/sharded"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	grpc_configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: playground_storage_frontend playground_storage_frontend.jsonnet")
		}
		var configuration playground_storage_frontend.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}
		lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global)
		if err != nil {
			return util.StatusWrap(err, "Failed to apply global configuration options")
		}

		if configuration.MaximumUnfinalizedParentsLimit == nil {
			return status.Error(codes.InvalidArgument, "No maximum unfinalized parents limit provided")
		}
		maximumUnfinalizedParentsLimit := object.NewLimit(configuration.MaximumUnfinalizedParentsLimit)
		objectStoreSemaphore := semaphore.NewWeighted(configuration.ObjectStoreConcurrency)

		// Construct object and tag stores for mirrored replicas.
		objectStoreA, tagStoreA, err := createShardsForReplica(grpcClientFactory, configuration.GrpcClientsShardsReplicaA)
		if err != nil {
			return util.StatusWrap(err, "Failed to create storage shards for replica A")
		}
		objectStoreB, tagStoreB, err := createShardsForReplica(grpcClientFactory, configuration.GrpcClientsShardsReplicaA)
		if err != nil {
			return util.StatusWrap(err, "Failed to create storage shards for replica A")
		}

		// Combine mirrored replicas together.
		objectDownloader := object_mirrored.NewMirroredDownloader(objectStoreA, objectStoreB)
		objectUploader := object_leaserenewing.NewLeaseRenewingUploader(
			object_mirrored.NewMirroredUploader(objectStoreA, objectStoreB),
			objectStoreSemaphore,
			maximumUnfinalizedParentsLimit,
		)
		dependenciesGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
			for objectUploader.ProcessSingleObject(ctx) {
			}
			return nil
		})

		tagUpdater := tag_mirrored.NewMirroredUpdater(tagStoreA, tagStoreB)
		tagResolver := tag_leaserenewing.NewLeaseRenewingResolver(
			tag.NewStore(
				tag_mirrored.NewMirroredResolver(tagStoreA, tagStoreB),
				tagUpdater,
			),
			objectUploader,
		)

		if err := bb_grpc.NewServersFromConfigurationAndServe(
			configuration.GrpcServers,
			func(s grpc.ServiceRegistrar) {
				// Services for downloading DAGs.
				object_pb.RegisterDownloaderServer(
					s,
					object.NewDownloaderServer(objectDownloader),
				)
				tag_pb.RegisterResolverServer(
					s,
					tag.NewResolverServer(tagResolver),
				)

				// Services for uploading DAGs.
				dag_pb.RegisterUploaderServer(
					s,
					dag.NewUploaderServer(
						objectUploader,
						objectStoreSemaphore,
						tagUpdater,
						configuration.MaximumUnfinalizedDagsCount,
						maximumUnfinalizedParentsLimit,
					),
				)
			},
			siblingsGroup,
		); err != nil {
			return util.StatusWrap(err, "gRPC server failure")
		}

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}

func createShardsForReplica(grpcClientFactory bb_grpc.ClientFactory, grpcConfigurations []*grpc_configuration.ClientConfiguration) (object.Store[object.GlobalReference, []byte], tag.Store[object.Namespace, object.GlobalReference, []byte], error) {
	// Create object & tag stores for each shard.
	objectDownloaders := make([]object.Downloader[object.GlobalReference], 0, len(grpcConfigurations))
	objectUploaders := make([]object.Uploader[object.GlobalReference, []byte], 0, len(grpcConfigurations))
	tagResolvers := make([]tag.Resolver[object.Namespace], 0, len(grpcConfigurations))
	tagUpdaters := make([]tag.Updater[object.GlobalReference, []byte], 0, len(grpcConfigurations))
	for index, grpcConfiguration := range grpcConfigurations {
		grpcClient, err := grpcClientFactory.NewClientFromConfiguration(grpcConfiguration)
		if err != nil {
			return nil, nil, util.StatusWrapf(err, "Failed to create gRPC client for shard at index %d", index)
		}

		objectDownloaders = append(objectDownloaders, object_grpc.NewGRPCDownloader(
			object_pb.NewDownloaderClient(grpcClient),
		))
		objectUploaders = append(objectUploaders, object_grpc.NewGRPCUploader(
			object_pb.NewUploaderClient(grpcClient),
		))
		tagResolvers = append(tagResolvers, tag_grpc.NewGRPCResolver(
			tag_pb.NewResolverClient(grpcClient),
		))
		tagUpdaters = append(tagUpdaters, tag_grpc.NewGRPCUpdater(
			tag_pb.NewUpdaterClient(grpcClient),
		))
	}

	// If we have multiple stores, instantiate the sharded backend.
	switch len(grpcConfigurations) {
	case 0:
		return nil, nil, status.Error(codes.InvalidArgument, "No shards provided")
	case 1:
		return object.NewStore(objectDownloaders[0], objectUploaders[0]),
			tag.NewStore(tagResolvers[0], tagUpdaters[0]),
			nil
	default:
		return object.NewStore(
				object_sharded.NewShardedDownloader(objectDownloaders),
				object_sharded.NewShardedUploader[object.GlobalReference, []byte](objectUploaders),
			),
			tag.NewStore(
				tag_sharded.NewShardedResolver(tagResolvers),
				tag_sharded.NewShardedUpdater[object.GlobalReference, []byte](tagUpdaters),
			),
			nil
	}
}
