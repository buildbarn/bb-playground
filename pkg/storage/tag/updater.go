package tag

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"google.golang.org/protobuf/types/known/anypb"
)

type Updater[TReference any, TLease any] interface {
	UpdateTag(ctx context.Context, tag *anypb.Any, reference TReference, lease TLease, overwrite bool) error
}

type UpdaterForTesting Updater[object.GlobalReference, any]
