package evaluation

import (
	"context"
	"errors"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"google.golang.org/protobuf/proto"
)

var ErrMissingDependency = errors.New("missing dependency")

type Computer interface {
	ComputeMessageValue(ctx context.Context, key proto.Message, e Environment) (model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker], error)
	ComputeNativeValue(ctx context.Context, key proto.Message, e Environment) (any, error)
}
