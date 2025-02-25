package namespaced

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/storage/object"
)

type NamespaceRemovingReference interface {
	GetLocalReference() object.LocalReference
}

type namespaceRemovingDownloader[TReference NamespaceRemovingReference] struct {
	base object.Downloader[object.LocalReference]
}

// NewNamespaceRemovingDownloader creates a decorator for Downloader
// that converts references provided to DownloadObject() to
// LocalReferences. This is useful if the storage backend is oblivious
// of namespaces (e.g., local disk based storage).
func NewNamespaceRemovingDownloader[TReference NamespaceRemovingReference](base object.Downloader[object.LocalReference]) object.Downloader[TReference] {
	return &namespaceRemovingDownloader[TReference]{
		base: base,
	}
}

func (d *namespaceRemovingDownloader[TReference]) DownloadObject(ctx context.Context, reference TReference) (*object.Contents, error) {
	return d.base.DownloadObject(ctx, reference.GetLocalReference())
}
