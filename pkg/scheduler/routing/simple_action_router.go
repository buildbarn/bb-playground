package routing

import (
	"context"

	remoteexecution_pb "github.com/buildbarn/bb-playground/pkg/proto/remoteexecution"
	"github.com/buildbarn/bb-playground/pkg/scheduler/initialsizeclass"
	"github.com/buildbarn/bb-playground/pkg/scheduler/invocation"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type simpleActionRouter struct {
	invocationKeyExtractors  []invocation.KeyExtractor
	initialSizeClassAnalyzer initialsizeclass.Analyzer
}

// NewSimpleActionRouter creates an ActionRouter that creates invocation
// keys and an initial size class selector by independently calling into
// separate extractors/analyzers.
//
// This implementation should be sufficient for most simple setups,
// where only a small number of execution platforms exist, or where
// scheduling decisions are identical for all platforms.
func NewSimpleActionRouter(invocationKeyExtractors []invocation.KeyExtractor, initialSizeClassAnalyzer initialsizeclass.Analyzer) ActionRouter {
	return &simpleActionRouter{
		invocationKeyExtractors:  invocationKeyExtractors,
		initialSizeClassAnalyzer: initialSizeClassAnalyzer,
	}
}

func (ar *simpleActionRouter) RouteAction(ctx context.Context, action *remoteexecution_pb.Action) ([]invocation.Key, initialsizeclass.Selector, error) {
	invocationKeys := make([]invocation.Key, 0, len(ar.invocationKeyExtractors))
	for _, invocationKeyExtractor := range ar.invocationKeyExtractors {
		invocationKey, err := invocationKeyExtractor.ExtractKey(ctx)
		if err != nil {
			return nil, nil, util.StatusWrap(err, "Failed to extract invocation key")
		}
		invocationKeys = append(invocationKeys, invocationKey)
	}
	initialSizeClassSelector, err := ar.initialSizeClassAnalyzer.Analyze(ctx, action)
	if err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to analyze initial size class")
	}
	return invocationKeys, initialSizeClassSelector, nil
}
