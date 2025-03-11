package object

import (
	"context"
)

// Downloader of objects from storage.
type Downloader[TReference any] interface {
	DownloadObject(ctx context.Context, reference TReference) (*Contents, error)
}

// DownloaderForTesting is an instantiation of Downloader for generating
// mocks to be used as part of tests.
type DownloaderForTesting Downloader[GlobalReference]
