package grpc

import (
	"context"

	object_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

type grpcDownloader struct {
	client object_pb.DownloaderClient
}

// NewGRPCDownloader creates an object downloader that forwards all
// requests to fetch objects to a remote server using gRPC.
func NewGRPCDownloader(client object_pb.DownloaderClient) object.Downloader[object.GlobalReference] {
	return &grpcDownloader{
		client: client,
	}
}

func (d *grpcDownloader) DownloadObject(ctx context.Context, reference object.GlobalReference) (*object.Contents, error) {
	response, err := d.client.DownloadObject(ctx, &object_pb.DownloadObjectRequest{
		Namespace: reference.GetNamespace().ToProto(),
		Reference: reference.GetRawReference(),
	})
	if err != nil {
		return nil, err
	}
	contents, err := object.NewContentsFromProto(reference.LocalReference, response.Contents)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Server returned invalid object contents")
	}
	return contents, nil
}
