package btree

import (
	"context"

	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/dereference"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

// Find an entry contained in a B-tree.
func Find[
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
	list model_core.Message[[]TMessagePtr, TOutgoingReferences],
	cmp func(TMessagePtr) (int, *model_core_pb.Reference),
) (model_core.Message[TMessagePtr, TOutgoingReferences], error) {
	for {
		low, high := 0, len(list.Message)
		var childReference *model_core_pb.Reference
		for low < high {
			index := int(uint(low+high) >> 1)
			r, reference := cmp(list.Message[index])
			if r < 0 {
				// Entry should be closer to the start
				// of the list.
				high = index
			} else if r > 0 {
				// Entry should be closer to the end of
				// the list. Do preserve the reference
				// to the child in case we don't find
				// any better matches.
				low = index + 1
				childReference = reference
			} else {
				// Exact match. We may stop searching.
				if reference == nil {
					return model_core.NewNestedMessage(list, list.Message[index]), nil
				}
				childReference = reference
				break
			}
		}
		if childReference == nil {
			// Found no exact match, and also did not find a
			// child that may include a matching entry.
			return model_core.Message[TMessagePtr, TOutgoingReferences]{}, nil
		}

		// Load the child from storage and continue searching.
		var err error
		list, err = dereference.Dereference(ctx, dereferencer, model_core.NewNestedMessage(list, childReference))
		if err != nil {
			return model_core.Message[TMessagePtr, TOutgoingReferences]{}, err
		}
	}
}
