package btree

import (
	"context"
	"iter"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_core_pb "github.com/buildbarn/bb-playground/pkg/proto/model/core"
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

// AllLeaves can be used to iterate all leaf entries contained in a B-tree.
func AllLeaves[
	TMessage any,
	TMessagePtr interface {
		*TMessage
		proto.Message
	},
](
	ctx context.Context,
	reader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[[]TMessagePtr]],
	root model_core.Message[[]TMessagePtr],
	traverser func(TMessagePtr) *model_core_pb.Reference,
	errOut *error,
) iter.Seq[model_core.Message[TMessagePtr]] {
	lists := []model_core.Message[[]TMessagePtr]{root}
	return func(yield func(model_core.Message[TMessagePtr]) bool) {
		for len(lists) > 0 {
			lastList := &lists[len(lists)-1]
			if len(lastList.Message) == 0 {
				lists = lists[:len(lists)-1]
			} else {
				entry := lastList.Message[0]
				lastList.Message = lastList.Message[1:]
				if childReference := traverser(entry); childReference == nil {
					// Traverser wants us to yield a leaf.
					if !yield(model_core.Message[TMessagePtr]{
						Message:            entry,
						OutgoingReferences: lastList.OutgoingReferences,
					}) {
						*errOut = nil
						return
					}
				} else {
					// Traverser wants us to enter a child.
					index, err := model_core.GetIndexFromReferenceMessage(childReference, lastList.OutgoingReferences.GetDegree())
					if err != nil {
						*errOut = err
						return
					}
					child, _, err := reader.ReadParsedObject(
						ctx,
						lastList.OutgoingReferences.GetOutgoingReference(index),
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
