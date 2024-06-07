package tag

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/proto/storage/tag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type resolverServer struct {
	resolver Resolver[object.Namespace]
}

func NewResolverServer(resolver Resolver[object.Namespace]) tag.ResolverServer {
	return &resolverServer{
		resolver: resolver,
	}
}

func (s *resolverServer) ResolveTag(ctx context.Context, request *tag.ResolveTagRequest) (*tag.ResolveTagResponse, error) {
	namespace, err := object.NewNamespace(request.Namespace)
	if err != nil {
		return nil, util.StatusWrap(err, "Invalid namespace")
	}
	if request.Tag == nil {
		return nil, status.Error(codes.InvalidArgument, "No tag provided")
	}
	reference, complete, err := s.resolver.ResolveTag(ctx, namespace, request.Tag)
	if err != nil {
		return nil, err
	}
	return &tag.ResolveTagResponse{
		Reference: reference.GetRawReference(),
		Complete:  complete,
	}, nil
}
