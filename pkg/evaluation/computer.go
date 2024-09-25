package evaluation

import (
	"context"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"google.golang.org/protobuf/proto"
)

type Computer interface {
	ComputeValue(ctx context.Context, key proto.Message, e Environment) (model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker], error)
}
