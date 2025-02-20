package btree

import (
	"context"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_core_pb "github.com/buildbarn/bb-playground/pkg/proto/model/core"
	"github.com/buildbarn/bb-playground/pkg/storage/object"

	"google.golang.org/protobuf/proto"
)

// Find an entry contained in a B-tree.
func Find[
	TMessage any,
	TMessagePtr interface {
		*TMessage
		proto.Message
	},
](
	ctx context.Context,
	reader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[[]TMessagePtr]],
	list model_core.Message[[]TMessagePtr],
	cmp func(TMessagePtr) (int, *model_core_pb.Reference),
) (model_core.Message[TMessagePtr], error) {
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
					return model_core.Message[TMessagePtr]{
						Message:            list.Message[index],
						OutgoingReferences: list.OutgoingReferences,
					}, nil
				}
				childReference = reference
				break
			}
		}
		if childReference == nil {
			// Found no exact match, and also did not find a
			// child that may include a matching entry.
			return model_core.Message[TMessagePtr]{}, nil
		}

		// Load the child from storage and continue searching.
		index, err := model_core.GetIndexFromReferenceMessage(childReference, list.OutgoingReferences.GetDegree())
		if err != nil {
			return model_core.Message[TMessagePtr]{}, err
		}
		list, _, err = reader.ReadParsedObject(ctx, list.OutgoingReferences.GetOutgoingReference(index))
		if err != nil {
			return model_core.Message[TMessagePtr]{}, err
		}
	}
}
