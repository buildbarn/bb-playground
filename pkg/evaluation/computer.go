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

type Computer[TReference, TMetadata any] interface {
	ComputeMessageValue(ctx context.Context, key model_core.Message[proto.Message, TReference], e Environment[TReference, TMetadata]) (model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker], error)
	ComputeNativeValue(ctx context.Context, key model_core.Message[proto.Message, TReference], e Environment[TReference, TMetadata]) (any, error)
}

type ComputerForTesting Computer[object.LocalReference, model_core.ReferenceMetadata]
