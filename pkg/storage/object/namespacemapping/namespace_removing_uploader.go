package namespaced

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/storage/object"
)

type namespaceRemovingUploader[TReference object.BasicReference, TLease any] struct {
	base object.Uploader[object.LocalReference, TLease]
}

// NewNamespaceRemovingDownloader creates a decorator for Uploader that
// converts references provided to UploadObject() to LocalReferences.
// This is useful if the storage backend is oblivious of namespaces
// (e.g., local disk based storage).
func NewNamespaceRemovingUploader[TReference object.BasicReference, TLease any](base object.Uploader[object.LocalReference, TLease]) object.Uploader[TReference, TLease] {
	return &namespaceRemovingUploader[TReference, TLease]{
		base: base,
	}
}

func (d *namespaceRemovingUploader[TReference, TLease]) UploadObject(ctx context.Context, reference TReference, contents *object.Contents, childrenLeases []TLease, wantContentsIfIncomplete bool) (object.UploadObjectResult[TLease], error) {
	return d.base.UploadObject(ctx, reference.GetLocalReference(), contents, childrenLeases, wantContentsIfIncomplete)
}
