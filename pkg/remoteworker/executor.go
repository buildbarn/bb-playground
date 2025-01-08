package remoteworker

import (
	"context"
	"time"

	remoteworker_pb "github.com/buildbarn/bb-playground/pkg/proto/remoteworker"

	"google.golang.org/protobuf/proto"
)

// Executor is the interface for the ability to run execute actions and
// yield execution events.
type Executor[TAction any] interface {
	CheckReadiness(ctx context.Context) error
	Execute(ctx context.Context, action TAction, executionTimeout time.Duration, executionEvents chan<- proto.Message) (proto.Message, time.Duration, remoteworker_pb.CurrentState_Completed_Result)
}
