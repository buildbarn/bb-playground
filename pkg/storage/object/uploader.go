package object

import (
	"context"
)

type Uploader[TReference any, TLease any] interface {
	UploadObject(ctx context.Context, reference TReference, contents *Contents, childrenLeases []TLease, wantContentsIfIncomplete bool) (UploadObjectResult[TLease], error)
}

type UploadObjectResult[TLease any] interface {
	isUploadObjectResult(TLease)
}

func (UploadObjectComplete[TLease]) isUploadObjectResult(TLease)   {}
func (UploadObjectIncomplete[TLease]) isUploadObjectResult(TLease) {}
func (UploadObjectMissing[TLease]) isUploadObjectResult(TLease)    {}

type UploadObjectComplete[TLease any] struct {
	Lease TLease
}

type UploadObjectIncomplete[TLease any] struct {
	Contents                     *Contents
	WantOutgoingReferencesLeases []int
}

type UploadObjectMissing[TLease any] struct{}

type (
	UploaderForTesting      Uploader[GlobalReference, any]
	UploaderForTestingBytes Uploader[GlobalReference, []byte]
)
