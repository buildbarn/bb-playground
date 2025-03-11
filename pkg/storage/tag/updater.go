package tag

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/types/known/anypb"
)

// Updater of tags in storage. Tags can be used to assign a stable
// identifier to an object.
type Updater[TReference any, TLease any] interface {
	UpdateTag(ctx context.Context, tag *anypb.Any, reference TReference, lease TLease, overwrite bool) error
}

// UpdaterForTesting is an instantiated version of Updater, which may be
// used for generating mocks to be used by tests.
type UpdaterForTesting Updater[object.GlobalReference, any]
