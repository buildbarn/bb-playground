package tag

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/types/known/anypb"
)

// Resolver of tags.
type Resolver[TNamespace any] interface {
	ResolveTag(ctx context.Context, namespace TNamespace, tag *anypb.Any) (reference object.LocalReference, complete bool, err error)
}

// ResolverForTesting is an instantiated version of Resolver, which may
// be used to generate mocks for tests.
type ResolverForTesting Resolver[object.Namespace]
