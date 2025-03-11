package object

import (
	"context"
)

// Uploader of objects into storage.
//
// When UploadObject() is called without contents and leases set to nil,
// the backend reports whether the object exists and whether leases to
// its children are still valid. If the object does not exist, or leases
// of its children have expired, another call can be performed to
// provide the object's contents or update its leases.
type Uploader[TReference any, TLease any] interface {
	UploadObject(ctx context.Context, reference TReference, contents *Contents, childrenLeases []TLease, wantContentsIfIncomplete bool) (UploadObjectResult[TLease], error)
}

// UploadObjectResult is the result returned by Uploader.UploadObject().
// A backend may report that an object is complete (i.e., present and
// all of its leases are valid), incomplete (i.e., present, but one or
// more leases have expired) or missing (i.e., not present in storage).
type UploadObjectResult[TLease any] interface {
	isUploadObjectResult(TLease)
}

func (UploadObjectComplete[TLease]) isUploadObjectResult(TLease)   {}
func (UploadObjectIncomplete[TLease]) isUploadObjectResult(TLease) {}
func (UploadObjectMissing[TLease]) isUploadObjectResult(TLease)    {}

// UploadObjectComplete indicates that an object is complete (i.e.,
// present and all of its leases are valid).
type UploadObjectComplete[TLease any] struct {
	Lease TLease
}

// UploadObjectIncomplete indicates that an object is incomplete (i.e.,
// present, but one or more leases have expired). It is expected that
// the caller attempts to obtain updated leases for the object's
// children and calls UploadObject() again to provide the updated
// leases.
type UploadObjectIncomplete[TLease any] struct {
	Contents                     *Contents
	WantOutgoingReferencesLeases []int
}

// UploadObjectMissing indicates that an object is missing (i.e., not
// present in storage).
type UploadObjectMissing[TLease any] struct{}

type (
	// UploaderForTesting is an instantiation of Uploader for
	// generating mocks used by tests.
	UploaderForTesting Uploader[GlobalReference, any]

	// UploaderForTestingBytes is an instantiation of Uploader for
	// generating mocks used by tests.
	UploaderForTestingBytes Uploader[GlobalReference, []byte]
)
