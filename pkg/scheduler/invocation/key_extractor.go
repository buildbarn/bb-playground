package invocation

import (
	"context"
)

// KeyExtractor is responsible for extracting an invocation key from an
// incoming execution request. Operations will be grouped by invocation
// key and scheduled fairly.
//
// Implementations of KeyExtract may construct keys based on request
// metadata or user credentials.
type KeyExtractor interface {
	ExtractKey(ctx context.Context) (Key, error)
}
