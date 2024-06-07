package grpc

import (
	"context"

	tag_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/tag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/tag"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/anypb"
)

type grpcResolver struct {
	client tag_pb.ResolverClient
}

// NewGRPCResolver creates a tag resolver that forwards all requests to
// resolve tags to a remote server using gRPC.
func NewGRPCResolver(client tag_pb.ResolverClient) tag.Resolver[object.Namespace] {
	return &grpcResolver{
		client: client,
	}
}

func (d *grpcResolver) ResolveTag(ctx context.Context, namespace object.Namespace, tag *anypb.Any) (object.LocalReference, bool, error) {
	response, err := d.client.ResolveTag(ctx, &tag_pb.ResolveTagRequest{
		Namespace: namespace.ToProto(),
		Tag:       tag,
	})
	if err != nil {
		var badReference object.LocalReference
		return badReference, false, err
	}
	reference, err := namespace.NewGlobalReference(response.Reference)
	if err != nil {
		var badReference object.LocalReference
		return badReference, false, util.StatusWrapWithCode(err, codes.Internal, "Server returned an invalid reference")
	}
	return reference.LocalReference, response.Complete, nil
}
