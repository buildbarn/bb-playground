package main

import (
	"context"
	"os"

	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/proto/configuration/bonanza_storage_shard"
	object_pb "github.com/buildbarn/bonanza/pkg/proto/storage/object"
	tag_pb "github.com/buildbarn/bonanza/pkg/proto/storage/tag"
	"github.com/buildbarn/bonanza/pkg/storage/object"
	object_leasemarshaling "github.com/buildbarn/bonanza/pkg/storage/object/leasemarshaling"
	object_local "github.com/buildbarn/bonanza/pkg/storage/object/local"
	object_namespacemapping "github.com/buildbarn/bonanza/pkg/storage/object/namespacemapping"
	"github.com/buildbarn/bonanza/pkg/storage/tag"
	tag_leasemarshaling "github.com/buildbarn/bonanza/pkg/storage/tag/leasemarshaling"
	tag_local "github.com/buildbarn/bonanza/pkg/storage/tag/local"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: bonanza_storage_shard bonanza_storage_shard.jsonnet")
		}
		var configuration bonanza_storage_shard.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}
		lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global)
		if err != nil {
			return util.StatusWrap(err, "Failed to apply global configuration options")
		}

		objectStore := object_local.NewLocalStore()
		tagStore := tag_local.NewLocalStore()
		leaseMarshaler := object_local.LocalLeaseMarshaler

		if err := bb_grpc.NewServersFromConfigurationAndServe(
			configuration.GrpcServers,
			func(s grpc.ServiceRegistrar) {
				object_pb.RegisterDownloaderServer(
					s,
					object.NewDownloaderServer(
						object_namespacemapping.NewNamespaceRemovingDownloader[object.GlobalReference](
							objectStore,
						),
					),
				)
				object_pb.RegisterUploaderServer(
					s,
					object.NewUploaderServer(
						object_leasemarshaling.NewLeaseMarshalingUploader(
							object_namespacemapping.NewNamespaceRemovingUploader[object.GlobalReference](
								objectStore,
							),
							leaseMarshaler,
						),
					),
				)
				tag_pb.RegisterResolverServer(
					s,
					tag.NewResolverServer(
						tagStore,
					),
				)
				tag_pb.RegisterUpdaterServer(
					s,
					tag.NewUpdaterServer(
						tag_leasemarshaling.NewLeaseMarshalingUpdater(
							tagStore,
							leaseMarshaler,
						),
					),
				)
			},
			siblingsGroup,
			grpcClientFactory,
		); err != nil {
			return util.StatusWrap(err, "gRPC server failure")
		}

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}
