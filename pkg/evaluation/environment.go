package evaluation

import (
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

type Environment interface {
	GetMessageValue(key model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker]) model_core.Message[proto.Message, object.OutgoingReferences[object.LocalReference]]
	GetNativeValue(key model_core.PatchedMessage[proto.Message, dag.ObjectContentsWalker]) (any, bool)
}
