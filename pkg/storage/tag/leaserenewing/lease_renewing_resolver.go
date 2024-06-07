package leaserenewing

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/tag"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/protobuf/types/known/anypb"
)

type LeaseRenewingNamespace[TReference any] interface {
	WithLocalReference(localReference object.LocalReference) TReference
}

type leaseRenewingResolver[TNamespace LeaseRenewingNamespace[TReference], TReference any, TLease any] struct {
	tagStore       tag.Store[TNamespace, TReference, TLease]
	objectUploader object.Uploader[TReference, TLease]
}

// NewLeaseRenewingResolver creates a decorator for tag.Resolver that
// attempts to reobtain leases for tags that reference objects for which
// the lease is expired or missing. Upon success, the tag is updated to
// use the new lease.
func NewLeaseRenewingResolver[TNamespace LeaseRenewingNamespace[TReference], TReference, TLease any](tagStore tag.Store[TNamespace, TReference, TLease], objectUploader object.Uploader[TReference, TLease]) tag.Resolver[TNamespace] {
	return &leaseRenewingResolver[TNamespace, TReference, TLease]{
		tagStore:       tagStore,
		objectUploader: objectUploader,
	}
}

func (r *leaseRenewingResolver[TNamespace, TReference, TLease]) ResolveTag(ctx context.Context, namespace TNamespace, tag *anypb.Any) (object.LocalReference, bool, error) {
	localReference, complete, err := r.tagStore.ResolveTag(ctx, namespace, tag)
	if err != nil || complete {
		return localReference, complete, err
	}

	// Tag has an expired lease. Attempt to obtain a new lease on
	// the root object referenced by the tag.
	globalReference := namespace.WithLocalReference(localReference)
	result, err := r.objectUploader.UploadObject(
		ctx,
		globalReference,
		/* contents = */ nil,
		/* childrenLeases = */ nil,
		/* wantContentsIfIncomplete = */ false,
	)
	if err != nil {
		var badReference object.LocalReference
		return badReference, false, util.StatusWrapf(err, "Failed to obtain lease for object with reference %s", localReference)
	}

	switch resultType := result.(type) {
	case object.UploadObjectComplete[TLease]:
		// Update the tag to use the new lease. This permits
		// lookups for the same tag in the nearby future to go
		// faster.
		if err := r.tagStore.UpdateTag(
			ctx,
			tag,
			globalReference,
			resultType.Lease,
			/* overwrite = */ false,
		); err != nil {
			var badReference object.LocalReference
			return badReference, false, util.StatusWrapf(err, "Failed to update tag with lease for object with reference %s", localReference)
		}
		return localReference, true, nil
	case object.UploadObjectIncomplete[TLease], object.UploadObjectMissing[TLease]:
		return localReference, false, nil
	default:
		panic("unknown upload object result type")
	}
}
