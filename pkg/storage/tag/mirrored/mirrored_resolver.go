package mirrored

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/storage/object"
	"github.com/buildbarn/bonanza/pkg/storage/tag"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

type mirroredResolver[TNamespace any] struct {
	replicaA tag.Resolver[TNamespace]
	replicaB tag.Resolver[TNamespace]
}

// NewMirroredResolver creates a decorator for tag.Resolver that
// forwards requests to resolve tags to a pair of backends that are
// configured to mirror each other's contents.
func NewMirroredResolver[TNamespace any](replicaA, replicaB tag.Resolver[TNamespace]) tag.Resolver[TNamespace] {
	return &mirroredResolver[TNamespace]{
		replicaA: replicaA,
		replicaB: replicaB,
	}
}

func (r *mirroredResolver[TNamespace]) ResolveTag(ctx context.Context, namespace TNamespace, tag *anypb.Any) (object.LocalReference, bool, error) {
	// Send request to both replicas.
	var referenceA, referenceB object.LocalReference
	var completeA, completeB bool
	var foundA, foundB bool
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		var err error
		referenceA, completeA, err = r.replicaA.ResolveTag(groupCtx, namespace, tag)
		if err == nil {
			foundA = true
			return nil
		} else if status.Code(err) == codes.NotFound {
			return nil
		}
		return util.StatusWrap(err, "Replica A")
	})
	group.Go(func() error {
		var err error
		referenceB, completeB, err = r.replicaB.ResolveTag(groupCtx, namespace, tag)
		if err == nil {
			foundB = true
			return nil
		} else if status.Code(err) == codes.NotFound {
			return nil
		}
		return util.StatusWrap(err, "Replica B")
	})
	if err := group.Wait(); err != nil {
		var badReference object.LocalReference
		return badReference, false, err
	}

	// Combine results from both replicas.
	//
	// If the tag is present in only a single replica, we return it,
	// but announce it as being incomplete. This should cause lease
	// renewing to replicate the tag to the other replica.
	//
	// If the tag is present in both replicas, but resolves to a
	// different reference, we suppress it. This should cause the
	// caller to recreate the data and overwrite the tag in both
	// replicas, causing the tag to be consistent once more.
	if foundA && foundB {
		if referenceA != referenceB {
			var badReference object.LocalReference
			return badReference, false, status.Errorf(
				codes.NotFound,
				"Replica A resolves tag to object with reference %s, while replica B resolves tag to object with reference %s",
				referenceA,
				referenceB,
			)
		}
		return referenceA, completeA && completeB, nil
	} else if foundA {
		return referenceA, false, nil
	} else if foundB {
		return referenceB, false, nil
	}
	var badReference object.LocalReference
	return badReference, false, status.Error(codes.NotFound, "Tag not found")
}
