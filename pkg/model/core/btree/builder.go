package btree

import (
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"

	"google.golang.org/protobuf/proto"
)

// Builder of B-trees.
type Builder[TNode proto.Message, TMetadata any] interface {
	PushChild(node model_core.PatchedMessage[TNode, TMetadata]) error
	FinalizeList() (model_core.PatchedMessage[[]TNode, TMetadata], error)
	FinalizeSingle() (model_core.PatchedMessage[TNode, TMetadata], error)
}
