package btree

import (
	"context"
	"iter"

	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/dereference"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

// AllLeaves can be used to iterate all leaf entries contained in a B-tree.
func AllLeaves[
	TMessage any,
	TMessagePtr interface {
		*TMessage
		proto.Message
	},
	TOutgoingReferences object.OutgoingReferences[TReference],
	TReference any,
](
	ctx context.Context,
	dereferencer dereference.Dereferencer[model_core.Message[[]TMessagePtr, TOutgoingReferences], TOutgoingReferences],
	root model_core.Message[[]TMessagePtr, TOutgoingReferences],
	traverser func(model_core.Message[TMessagePtr, TOutgoingReferences]) (*model_core_pb.Reference, error),
	errOut *error,
) iter.Seq[model_core.Message[TMessagePtr, TOutgoingReferences]] {
	lists := []model_core.Message[[]TMessagePtr, TOutgoingReferences]{root}
	return func(yield func(model_core.Message[TMessagePtr, TOutgoingReferences]) bool) {
		for len(lists) > 0 {
			lastList := &lists[len(lists)-1]
			if len(lastList.Message) == 0 {
				lists = lists[:len(lists)-1]
			} else {
				entry := lastList.Message[0]
				lastList.Message = lastList.Message[1:]
				if childReference, err := traverser(model_core.NewNestedMessage(*lastList, entry)); err != nil {
					*errOut = err
					return
				} else if childReference == nil {
					// Traverser wants us to yield a leaf.
					if !yield(model_core.NewNestedMessage(*lastList, entry)) {
						*errOut = nil
						return
					}
				} else {
					// Traverser wants us to enter a child.
					child, err := dereference.Dereference[model_core.Message[[]TMessagePtr, TOutgoingReferences]](
						ctx,
						dereferencer,
						model_core.NewNestedMessage(*lastList, childReference),
					)
					if err != nil {
						*errOut = err
						return
					}
					lists = append(lists, child)
				}
			}
		}
	}
}
