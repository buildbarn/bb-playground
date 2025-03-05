package evaluation

import (
	"context"
	"errors"

	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

var ErrMissingDependency = errors.New("missing dependency")

type Computer[TReference any] interface {
	ComputeMessageValue(ctx context.Context, key model_core.Message[proto.Message, TReference], e Environment[TReference]) (model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker], error)
	ComputeNativeValue(ctx context.Context, key model_core.Message[proto.Message, TReference], e Environment[TReference]) (any, error)
}

type ComputerForTesting Computer[object.LocalReference]
