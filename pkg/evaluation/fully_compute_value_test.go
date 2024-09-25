package evaluation_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"go.uber.org/mock/gomock"
)

func TestFullyComputeValue(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	t.Run("Fibonacci", func(t *testing.T) {
		// Example usage, where we provide a very basic
		// implementation of Computer that attempts to compute
		// the Fibonacci sequence recursively. Due to
		// memoization, this should run in polynomial time.
		computer := NewMockComputer(ctrl)
		computer.EXPECT().ComputeValue(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, key proto.Message, e evaluation.Environment) (model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker], error) {
				// Base case: fib(0) and fib(1).
				k := key.(*wrapperspb.UInt32Value)
				if k.Value <= 1 {
					return model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker]{
						Message: &wrapperspb.UInt64Value{
							Value: uint64(k.Value),
						},
						Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
					}, nil
				}

				// Recursion: fib(n) = fib(n-2) + fib(n-1).
				v0 := e.GetValue(&wrapperspb.UInt32Value{
					Value: k.Value - 2,
				})
				v1 := e.GetValue(&wrapperspb.UInt32Value{
					Value: k.Value - 1,
				})
				if !v0.IsSet() || !v1.IsSet() {
					return model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker]{}, nil
				}
				return model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker]{
					Message: &wrapperspb.UInt64Value{
						Value: v0.Message.(*wrapperspb.UInt64Value).Value + v1.Message.(*wrapperspb.UInt64Value).Value,
					},
					Patcher: model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker](),
				}, nil
			}).
			AnyTimes()

		m, err := evaluation.FullyComputeValue(ctx, computer, &wrapperspb.UInt32Value{
			Value: 93,
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &wrapperspb.UInt64Value{
			Value: 12200160415121876738,
		}, m.Message)
	})
}