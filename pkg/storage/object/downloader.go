package object

import (
	"context"
)

type Downloader[TReference any] interface {
	DownloadObject(ctx context.Context, reference TReference) (*Contents, error)
}

type DownloaderForTesting Downloader[GlobalReference]
