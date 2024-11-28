package routing

import (
	"context"

	remoteexecution_pb "github.com/buildbarn/bb-playground/pkg/proto/remoteexecution"
	"github.com/buildbarn/bb-playground/pkg/scheduler/initialsizeclass"
	"github.com/buildbarn/bb-playground/pkg/scheduler/invocation"
)

// ActionRouter is responsible for doing all forms of analysis on an
// incoming execution request up to the point where InMemoryBuildQueue
// acquires locks and enqueues an operation. ActionRouter is responsible
// for the following things:
//
//   - To extract invocation keys from the client's context, so that
//     InMemoryBuildQueue can group operations belonging to the same
//     client and schedule them fairly with respect to other clients.
//   - To create an initial size class selector, which InMemoryBuildQueue
//     can use to select the appropriate worker size.
type ActionRouter interface {
	RouteAction(ctx context.Context, action *remoteexecution_pb.Action) ([]invocation.Key, initialsizeclass.Selector, error)
}
