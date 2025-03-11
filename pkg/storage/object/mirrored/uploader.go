package mirrored

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type uploader[TReference any, TLeaseA, TLeaseB any] struct {
	replicaA object.Store[TReference, TLeaseA]
	replicaB object.Store[TReference, TLeaseB]
}

// NewUploader creates a decorator for object.Uploader that writes all
// objects to two replicas. If any inconsistencies between the backends
// are detected (e.g., the object is only present in one of the
// backends), they are repaired automatically.
func NewUploader[TReference, TLeaseA, TLeaseB any](replicaA object.Store[TReference, TLeaseA], replicaB object.Store[TReference, TLeaseB]) object.Uploader[TReference, Lease[TLeaseA, TLeaseB]] {
	return &uploader[TReference, TLeaseA, TLeaseB]{
		replicaA: replicaA,
		replicaB: replicaB,
	}
}

func (u *uploader[TReference, TLeaseA, TLeaseB]) UploadObject(ctx context.Context, reference TReference, contents *object.Contents, childrenLeases []Lease[TLeaseA, TLeaseB], wantContentsIfIncomplete bool) (object.UploadObjectResult[Lease[TLeaseA, TLeaseB]], error) {
	// Decompose the leases into separate lists for replicas A and B.
	childrenLeasesA := make([]TLeaseA, 0, len(childrenLeases))
	childrenLeasesB := make([]TLeaseB, 0, len(childrenLeases))
	for _, lease := range childrenLeases {
		childrenLeasesA = append(childrenLeasesA, lease.LeaseA)
		childrenLeasesB = append(childrenLeasesB, lease.LeaseB)
	}

	// Forward the original request to both replicas.
	group, groupCtx := errgroup.WithContext(ctx)
	var resultA object.UploadObjectResult[TLeaseA]
	var resultB object.UploadObjectResult[TLeaseB]
	group.Go(func() (err error) {
		resultA, err = u.replicaA.UploadObject(groupCtx, reference, contents, childrenLeasesA, wantContentsIfIncomplete)
		if err != nil {
			return util.StatusWrap(err, "Replica A")
		}
		return nil
	})
	group.Go(func() (err error) {
		resultB, err = u.replicaB.UploadObject(groupCtx, reference, contents, childrenLeasesB, wantContentsIfIncomplete)
		if err != nil {
			return util.StatusWrap(err, "Replica B")
		}
		return nil
	})
	err := group.Wait()
	if err != nil {
		return nil, err
	}

	for {
		// If one of the results contains the object's contents,
		// extract it. This prevents the need for calling
		// DownloadObject() if the object needs to be replicated.
		if incomplete, ok := resultA.(object.UploadObjectIncomplete[TLeaseA]); contents == nil && ok {
			contents = incomplete.Contents
		}
		if incomplete, ok := resultB.(object.UploadObjectIncomplete[TLeaseB]); contents == nil && ok {
			contents = incomplete.Contents
		}
		var contentsIfIncomplete *object.Contents
		if wantContentsIfIncomplete {
			contentsIfIncomplete = contents
		}

		// Combine the results from both replicas.
		switch resultDataA := resultA.(type) {
		case object.UploadObjectComplete[TLeaseA]:
			switch resultDataB := resultB.(type) {
			case object.UploadObjectComplete[TLeaseB]:
				// Objects present in both replicas. Combine
				// both leases.
				return object.UploadObjectComplete[Lease[TLeaseA, TLeaseB]]{
					Lease: Lease[TLeaseA, TLeaseB]{
						LeaseA: resultDataA.Lease,
						LeaseB: resultDataB.Lease,
					},
				}, nil
			case object.UploadObjectIncomplete[TLeaseB]:
				// Only the object in replica B is
				// incomplete. Return results of replica B.
				return object.UploadObjectIncomplete[Lease[TLeaseA, TLeaseB]]{
					Contents:                     contentsIfIncomplete,
					WantOutgoingReferencesLeases: resultDataB.WantOutgoingReferencesLeases,
				}, nil
			case object.UploadObjectMissing[TLeaseB]:
				goto ReplicateAToB
			default:
				panic("unexpected upload object result type")
			}
		case object.UploadObjectIncomplete[TLeaseA]:
			switch resultDataB := resultB.(type) {
			case object.UploadObjectComplete[TLeaseB]:
				// Only the object in replica A is
				// incomplete. Return results of replica A.
				return object.UploadObjectIncomplete[Lease[TLeaseA, TLeaseB]]{
					Contents:                     contentsIfIncomplete,
					WantOutgoingReferencesLeases: resultDataA.WantOutgoingReferencesLeases,
				}, nil
			case object.UploadObjectIncomplete[TLeaseB]:
				// The object in both replica A and B is
				// incomplete. Return the results of
				// both replicas.
				resultWantLeases := make([]int, 0, max(len(resultDataA.WantOutgoingReferencesLeases), len(resultDataB.WantOutgoingReferencesLeases)))
				i, j := 0, 0
				for i < len(resultDataA.WantOutgoingReferencesLeases) && j < len(resultDataB.WantOutgoingReferencesLeases) {
					if li, lj := resultDataA.WantOutgoingReferencesLeases[i], resultDataB.WantOutgoingReferencesLeases[j]; li < lj {
						resultWantLeases = append(resultWantLeases, li)
						i++
					} else if li > lj {
						resultWantLeases = append(resultWantLeases, lj)
						j++
					} else {
						resultWantLeases = append(resultWantLeases, li)
						i++
						j++
					}
				}
				return object.UploadObjectIncomplete[Lease[TLeaseA, TLeaseB]]{
					Contents:                     contentsIfIncomplete,
					WantOutgoingReferencesLeases: append(append(resultWantLeases, resultDataA.WantOutgoingReferencesLeases[i:]...), resultDataB.WantOutgoingReferencesLeases[j:]...),
				}, nil
			case object.UploadObjectMissing[TLeaseB]:
				goto ReplicateAToB
			default:
				panic("unexpected upload object result type")
			}
		case object.UploadObjectMissing[TLeaseA]:
			switch resultB.(type) {
			case object.UploadObjectComplete[TLeaseB]:
				goto ReplicateBToA
			case object.UploadObjectIncomplete[TLeaseB]:
				goto ReplicateBToA
			case object.UploadObjectMissing[TLeaseB]:
				// The object is missing in both replicas
				// A and B. Report the object as missing.
				return object.UploadObjectMissing[Lease[TLeaseA, TLeaseB]]{}, nil
			default:
				panic("unexpected upload object result type")
			}
		default:
			panic("unexpected upload object result type")
		}

	ReplicateAToB:
		// Object is only present in replica A. Replicate it
		// from replica A to B.
		if contents == nil {
			contents, err = u.replicaA.DownloadObject(ctx, reference)
			if err != nil {
				if status.Code(err) != codes.NotFound {
					return nil, util.StatusWrap(err, "Replica A")
				}
				return object.UploadObjectMissing[Lease[TLeaseA, TLeaseB]]{}, nil
			}
		}

		resultB, err = u.replicaB.UploadObject(
			ctx,
			reference,
			contents,
			childrenLeasesB,
			/* wantContentsIfIncomplete = */ false,
		)
		if err != nil {
			return nil, util.StatusWrap(err, "Replica B")
		}
		continue

	ReplicateBToA:
		// Object is only present in replica B. Replicate it
		// from replica B to A.
		if contents == nil {
			contents, err = u.replicaB.DownloadObject(ctx, reference)
			if err != nil {
				if status.Code(err) != codes.NotFound {
					return nil, util.StatusWrap(err, "Replica B")
				}
				return object.UploadObjectMissing[Lease[TLeaseA, TLeaseB]]{}, nil
			}
		}

		resultA, err = u.replicaA.UploadObject(
			ctx,
			reference,
			contents,
			childrenLeasesA,
			/* wantContentsIfIncomplete = */ false,
		)
		if err != nil {
			return nil, util.StatusWrap(err, "Replica A")
		}
	}
}
