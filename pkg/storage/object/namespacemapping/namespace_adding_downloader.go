package namespaced

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/storage/object"
)

type NamespaceAddingNamespace[T any] interface {
	WithLocalReference(reference object.LocalReference) T
}

type namespaceAddingDownloader[TNamespace NamespaceAddingNamespace[TReference], TReference any] struct {
	base      object.Downloader[TReference]
	namespace TNamespace
}

func NewNamespaceAddingDownloader[TNamespace NamespaceAddingNamespace[TReference], TReference any](base object.Downloader[TReference], namespace TNamespace) object.Downloader[object.LocalReference] {
	return &namespaceAddingDownloader[TNamespace, TReference]{
		base:      base,
		namespace: namespace,
	}
}

func (d *namespaceAddingDownloader[TNamespace, TReference]) DownloadObject(ctx context.Context, reference object.LocalReference) (*object.Contents, error) {
	return d.base.DownloadObject(ctx, d.namespace.WithLocalReference(reference))
}
