package mirrored

import (
	"context"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mirroredDownloader[TReference any, TLeaseA, TLeaseB any] struct {
	aThenB unidirectionalMirroredDownloader[TReference, TLeaseA, TLeaseB]
	bThenA unidirectionalMirroredDownloader[TReference, TLeaseB, TLeaseA]
	round  atomic.Uint32
}

func NewMirroredDownloader[TReference, TLeaseA, TLeaseB any](backendA object.Store[TReference, TLeaseA], backendB object.Store[TReference, TLeaseB]) object.Downloader[TReference] {
	return &mirroredDownloader[TReference, TLeaseA, TLeaseB]{
		aThenB: unidirectionalMirroredDownloader[TReference, TLeaseA, TLeaseB]{
			firstBackend:      backendA,
			firstBackendName:  "Backend A",
			secondBackend:     backendB,
			secondBackendName: "Backend B",
		},
		bThenA: unidirectionalMirroredDownloader[TReference, TLeaseB, TLeaseA]{
			firstBackend:      backendB,
			firstBackendName:  "Backend B",
			secondBackend:     backendA,
			secondBackendName: "Backend A",
		},
	}
}

func (d *mirroredDownloader[TReference, TLeaseA, TLeaseB]) DownloadObject(ctx context.Context, reference TReference) (*object.Contents, error) {
	// Alternate the order between backend A and backend B to spread
	// out the load.
	if d.round.Add(1)%2 == 1 {
		return d.aThenB.downloadObject(ctx, reference)
	}
	return d.bThenA.downloadObject(ctx, reference)
}

type unidirectionalMirroredDownloader[TReference any, TLeaseFirst, TLeaseSecond any] struct {
	firstBackend      object.Store[TReference, TLeaseFirst]
	firstBackendName  string
	secondBackend     object.Store[TReference, TLeaseSecond]
	secondBackendName string
}

func (d *unidirectionalMirroredDownloader[TReference, TLeaseFirst, TLeaseSecond]) downloadObject(ctx context.Context, reference TReference) (*object.Contents, error) {
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
