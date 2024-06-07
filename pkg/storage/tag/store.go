package tag

type Store[TNamespace any, TReference any, TLease any] interface {
	Resolver[TNamespace]
	Updater[TReference, TLease]
}

// NewStore is a helper function for creating a Store that is backed by
// separate instances of Resolver and Updater.
func NewStore[TNamespace, TReference, TLease any](resolver Resolver[TNamespace], updater Updater[TReference, TLease]) Store[TNamespace, TReference, TLease] {
	return struct {
		Resolver[TNamespace]
		Updater[TReference, TLease]
	}{
		Resolver: resolver,
		Updater:  updater,
	}
}
