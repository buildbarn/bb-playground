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

type Computer interface {
	ComputeMessageValue(ctx context.Context, key model_core.Message[proto.Message, object.OutgoingReferences[object.LocalReference]], e Environment) (model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker], error)
	ComputeNativeValue(ctx context.Context, key model_core.Message[proto.Message, object.OutgoingReferences[object.LocalReference]], e Environment) (any, error)
}
