package readcaching

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ReadCachingReference[T any] interface {
	GetLocalReference() object.LocalReference
	Flatten() T
}

type readCachingDownloader[TReference ReadCachingReference[TReference], TLeaseFast any] struct {
	slow object.Downloader[TReference]
	fast object.Store[TReference, TLeaseFast]
}

// NewReadCachingDownloader creates a decorator for object.Downloader that adds
// read caching.
//
// Read requests first go to a fast backend. If that backend returns
// NOT_FOUND, the data is read from the slow backend and written into
// the fast backend prior to returning.
//
// Writes always go to the fast backend.
func NewReadCachingDownloader[TReference ReadCachingReference[TReference], TLeaseFast any](slow object.Downloader[TReference], fast object.Store[TReference, TLeaseFast]) object.Downloader[TReference] {
	return &readCachingDownloader[TReference, TLeaseFast]{
		slow: slow,
		fast: fast,
	}
}

func (d *readCachingDownloader[TReference, TLeaseFast]) DownloadObject(ctx context.Context, reference TReference) (*object.Contents, error) {
	// Attempt to load object from the fast backend.
	flatReference := reference.Flatten()
	if flatObjectContents, err := d.fast.DownloadObject(ctx, flatReference); err == nil {
		objectContents, err := flatObjectContents.Unflatten(reference.GetLocalReference())
		if err != nil {
			return nil, util.StatusWrap(err, "Unflatten object contents")
		}
		return objectContents, nil
	} else if status.Code(err) != codes.NotFound {
		return nil, util.StatusWrap(err, "Get object from fast backend")
	}

	// Object not found in the fast backend. Get it from the slow
	// backend and replicate it to the fast backend.
	//
	// TODO: Add a mechanism to prevent parallel replication of the
	// same object.
	objectContents, err := d.slow.DownloadObject(ctx, reference)
	if err != nil {
		return nil, util.StatusWrap(err, "Get object from slow backend")
	}
	result, err := d.fast.UploadObject(
		ctx,
		flatReference,
		objectContents.Flatten(),
		/* childrenLeases = */ nil,
		/* wantContentsIfIncomplete = */ false,
	)
	if err != nil {
		return nil, util.StatusWrap(err, "Put object in fast backend")
	}
	switch result.(type) {
	case object.UploadObjectComplete[TLeaseFast]:
		return objectContents, nil
	case object.UploadObjectIncomplete[TLeaseFast]:
		panic("storing objects with degree zero should never cause leases to be missing")
	default:
		panic("unexpected upload object result type")
	}
}
