package tag

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/types/known/anypb"
)

type Resolver[TNamespace any] interface {
	ResolveTag(ctx context.Context, namespace TNamespace, tag *anypb.Any) (reference object.LocalReference, complete bool, err error)
}

type ResolverForTesting Resolver[object.Namespace]
