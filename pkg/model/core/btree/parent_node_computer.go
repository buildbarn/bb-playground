package btree

import (
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

// ParentNodeComputer can be used by implementations of LevelBuilder to
// combine the values of nodes stored in an object into a single node
// that can be stored in its parent.
type ParentNodeComputer[TNode proto.Message, TMetadata any] func(
	contents *object.Contents,
	childNodes []TNode,
	outgoingReferences object.OutgoingReferences,
	metadata []TMetadata,
) (model_core.MessageWithReferences[TNode, TMetadata], error)

type ParentNodeComputerForTesting ParentNodeComputer[*model_filesystem_pb.FileContents, string]
