package btree

import (
	"context"
	"iter"

	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"

	"google.golang.org/protobuf/proto"
)

// AllLeaves can be used to iterate all leaf entries contained in a B-tree.
func AllLeaves[
	TMessage any,
	TMessagePtr interface {
		*TMessage
		proto.Message
	},
	TReference any,
](
	ctx context.Context,
	reader model_parser.ParsedObjectReader[TReference, model_core.Message[[]TMessagePtr, TReference]],
	root model_core.Message[[]TMessagePtr, TReference],
	traverser func(model_core.Message[TMessagePtr, TReference]) (*model_core_pb.Reference, error),
	errOut *error,
) iter.Seq[model_core.Message[TMessagePtr, TReference]] {
	lists := []model_core.Message[[]TMessagePtr, TReference]{root}
	return func(yield func(model_core.Message[TMessagePtr, TReference]) bool) {
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
					child, err := model_parser.Dereference[model_core.Message[[]TMessagePtr, TReference]](
						ctx,
						reader,
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
