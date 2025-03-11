package mirrored

import (
	"context"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type downloader[TReference any, TLeaseA, TLeaseB any] struct {
	aThenB unidirectionalDownloader[TReference, TLeaseA, TLeaseB]
	bThenA unidirectionalDownloader[TReference, TLeaseB, TLeaseA]
	round  atomic.Uint32
}

// NewDownloader creates a decorator for object.Downloader that attempts
// to read objects from two replicas.
//
// If an object is only present in the second replicas that was queried,
// it is automatically copied to the first replica. This ensures that
// the object remains available if the second replica were to experience
// data loss.
func NewDownloader[TReference, TLeaseA, TLeaseB any](backendA object.Store[TReference, TLeaseA], backendB object.Store[TReference, TLeaseB]) object.Downloader[TReference] {
	return &downloader[TReference, TLeaseA, TLeaseB]{
		aThenB: unidirectionalDownloader[TReference, TLeaseA, TLeaseB]{
			firstBackend:      backendA,
			firstBackendName:  "Backend A",
			secondBackend:     backendB,
			secondBackendName: "Backend B",
		},
		bThenA: unidirectionalDownloader[TReference, TLeaseB, TLeaseA]{
			firstBackend:      backendB,
			firstBackendName:  "Backend B",
			secondBackend:     backendA,
			secondBackendName: "Backend A",
		},
	}
}

func (d *downloader[TReference, TLeaseA, TLeaseB]) DownloadObject(ctx context.Context, reference TReference) (*object.Contents, error) {
	// Alternate the order between backend A and backend B to spread
	// out the load.
	if d.round.Add(1)%2 == 1 {
		return d.aThenB.downloadObject(ctx, reference)
	}
	return d.bThenA.downloadObject(ctx, reference)
}

type unidirectionalDownloader[TReference any, TLeaseFirst, TLeaseSecond any] struct {
	firstBackend      object.Store[TReference, TLeaseFirst]
	firstBackendName  string
	secondBackend     object.Store[TReference, TLeaseSecond]
	secondBackendName string
}

func (d *unidirectionalDownloader[TReference, TLeaseFirst, TLeaseSecond]) downloadObject(ctx context.Context, reference TReference) (*object.Contents, error) {
	// Attempt to download the object from the first backend.
	contents, err := d.firstBackend.DownloadObject(ctx, reference)
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return nil, util.StatusWrap(err, d.firstBackendName)
		}

		// Fall back to downloading it from the second backend.
		contents, err = d.secondBackend.DownloadObject(ctx, reference)
		if err != nil {
			if status.Code(err) != codes.NotFound {
				return nil, util.StatusWrap(err, d.secondBackendName)
			}
			return nil, err
		}

		// Replicate the object to the first backend. As we
		// don't replicate any of its children, we can't provide
		// leases to make the object complete. This is good
		// enough to make subsequent calls to DownloadObject()
		// work.
		if _, err := d.firstBackend.UploadObject(
			ctx,
			reference,
			contents,
			/* childLeases = */ nil,
			/* wantContentsIfIncomplete = */ false,
		); err != nil {
			return nil, util.StatusWrap(err, d.secondBackendName)
		}
	}
	return contents, nil
}
