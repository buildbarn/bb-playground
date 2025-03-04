package starlark

import (
	"context"
	"errors"
	"iter"

	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/btree"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

// AllListLeafElementsDeduplicatingParents walks over a list and returns
// all leaf elements contained within. In the process, it records which
// parent elements are encountered and skips duplicates.
//
// This function can be used to efficiently iterate lists that should be
// interpreted as sets. As depsets are backed by lists internally, this
// function can be used in part to implement depset.to_list().
//
// Note that this function does not perform deduplication of leaf
// elements. Only parents are deduplicated.
func AllListLeafElementsSkippingDuplicateParents[TOutgoingReferences object.OutgoingReferences[object.LocalReference]](
	ctx context.Context,
	reader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[[]*model_starlark_pb.List_Element, TOutgoingReferences]],
	rootList model_core.Message[[]*model_starlark_pb.List_Element, TOutgoingReferences],
	listsSeen map[object.LocalReference]struct{},
	errOut *error,
) iter.Seq[model_core.Message[*model_starlark_pb.Value, TOutgoingReferences]] {
	allLeaves := btree.AllLeaves(
		ctx,
		reader,
		rootList,
		func(element model_core.Message[*model_starlark_pb.List_Element, TOutgoingReferences]) (*model_core_pb.Reference, error) {
			if level, ok := element.Message.Level.(*model_starlark_pb.List_Element_Parent_); ok {
				listReferenceMessage := level.Parent.Reference
				listReference, err := model_core.FlattenReference(model_core.NewNestedMessage(element, level.Parent.Reference))
				if err != nil {
					return nil, err
				}
				if _, ok := listsSeen[listReference]; ok {
					// Parent was already seen before.
					// Skip it.
					return nil, nil
				}

				// Parent was not seen before. Enter it.
				listsSeen[listReference] = struct{}{}
				return listReferenceMessage, nil
			}
			return nil, nil
		},
		errOut,
	)
	return func(yield func(model_core.Message[*model_starlark_pb.Value, TOutgoingReferences]) bool) {
		allLeaves(func(entry model_core.Message[*model_starlark_pb.List_Element, TOutgoingReferences]) bool {
			switch level := entry.Message.Level.(type) {
			case *model_starlark_pb.List_Element_Leaf:
				return yield(model_core.NewNestedMessage(entry, level.Leaf))
			case *model_starlark_pb.List_Element_Parent_:
				// Parent that was traversed previously,
				// which needs to be skipped.
				return true
			default:
				*errOut = errors.New("not a valid leaf entry")
				return false
			}
		})
	}
}
