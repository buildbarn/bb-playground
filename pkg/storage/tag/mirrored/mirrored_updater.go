package mirrored

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/storage/object/mirrored"
	"github.com/buildbarn/bonanza/pkg/storage/tag"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/anypb"
)

type mirroredUpdater[TReference any, TLeaseA, TLeaseB any] struct {
	replicaA tag.Updater[TReference, TLeaseA]
	replicaB tag.Updater[TReference, TLeaseB]
}

// NewMirroredUpdater creates a decorator for tag.Updater that forwards
// requests to update tags to a pair of backends that are configured to
// mirror each other's contents.
func NewMirroredUpdater[TReference, TLeaseA, TLeaseB any](replicaA tag.Updater[TReference, TLeaseA], replicaB tag.Updater[TReference, TLeaseB]) tag.Updater[TReference, mirrored.MirroredLease[TLeaseA, TLeaseB]] {
	return &mirroredUpdater[TReference, TLeaseA, TLeaseB]{
		replicaA: replicaA,
		replicaB: replicaB,
	}
}

func (u *mirroredUpdater[TReference, TLeaseA, TLeaseB]) UpdateTag(ctx context.Context, tag *anypb.Any, reference TReference, lease mirrored.MirroredLease[TLeaseA, TLeaseB], overwrite bool) error {
	// Forward the request to both replicas in parallel.
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		if err := u.replicaA.UpdateTag(groupCtx, tag, reference, lease.LeaseA, overwrite); err != nil {
			return util.StatusWrap(err, "Replica A")
		}
		return nil
	})
	group.Go(func() error {
		if err := u.replicaB.UpdateTag(groupCtx, tag, reference, lease.LeaseB, overwrite); err != nil {
			return util.StatusWrap(err, "Replica B")
		}
		return nil
	})
	return group.Wait()
}
