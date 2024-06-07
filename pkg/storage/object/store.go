package object

type Store[TReference any, TLease any] interface {
	Downloader[TReference]
	Uploader[TReference, TLease]
}

// NewStore is a helper function for creating a Store that is backed by
// separate instances of Downloader and Uploader.
func NewStore[TReference, TLease any](downloader Downloader[TReference], uploader Uploader[TReference, TLease]) Store[TReference, TLease] {
	return struct {
		Downloader[TReference]
		Uploader[TReference, TLease]
	}{
		Downloader: downloader,
		Uploader:   uploader,
	}
}

type StoreForTesting Store[GlobalReference, any]
