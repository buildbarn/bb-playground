package leasemarshaling

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/storage/object/leasemarshaling"
	"github.com/buildbarn/bb-playground/pkg/storage/tag"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/protobuf/types/known/anypb"
)

type leaseMarshalingUpdater[TReference any, TLease any] struct {
	base      tag.Updater[TReference, TLease]
	marshaler leasemarshaling.LeaseMarshaler[TLease]
}

// NewLeaseMarshalingUpdater creates a decorator for tag.Updater that
// converts leases in the format of byte slices to the native
// representation of the storage backend. This is typically needed if a
// storage backend is exposed via the network.
func NewLeaseMarshalingUpdater[TReference, TLease any](base tag.Updater[TReference, TLease], marshaler leasemarshaling.LeaseMarshaler[TLease]) tag.Updater[TReference, []byte] {
	return &leaseMarshalingUpdater[TReference, TLease]{
		base:      base,
		marshaler: marshaler,
	}
}

func (u *leaseMarshalingUpdater[TReference, TLease]) UpdateTag(ctx context.Context, tag *anypb.Any, reference TReference, lease []byte, overwrite bool) error {
	var unmarshaledLease TLease
	if len(lease) > 0 {
		var err error
		unmarshaledLease, err = u.marshaler.UnmarshalLease(lease)
		if err != nil {
			return util.StatusWrap(err, "Invalid lease")
		}
	}
	return u.base.UpdateTag(ctx, tag, reference, unmarshaledLease, overwrite)
}
