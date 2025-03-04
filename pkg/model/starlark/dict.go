package starlark

import (
	"context"
	"errors"
	"iter"

	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/btree"
	"github.com/buildbarn/bonanza/pkg/model/core/dereference"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	model_starlark_pb "github.com/buildbarn/bonanza/pkg/proto/model/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

func AllDictLeafEntries[TOutgoingReferences object.OutgoingReferences[object.LocalReference]](
	ctx context.Context,
	dereferencer dereference.Dereferencer[model_core.Message[[]*model_starlark_pb.Dict_Entry, TOutgoingReferences], object.LocalReference],
	rootDict model_core.Message[*model_starlark_pb.Dict, TOutgoingReferences],
	errOut *error,
) iter.Seq[model_core.Message[*model_starlark_pb.Dict_Entry_Leaf, TOutgoingReferences]] {
	allLeaves := btree.AllLeaves(
		ctx,
		dereferencer,
		model_core.NewNestedMessage(rootDict, rootDict.Message.Entries),
		func(entry model_core.Message[*model_starlark_pb.Dict_Entry, TOutgoingReferences]) (*model_core_pb.Reference, error) {
			if parent, ok := entry.Message.Level.(*model_starlark_pb.Dict_Entry_Parent_); ok {
				return parent.Parent.Reference, nil
			}
			return nil, nil
		},
		errOut,
	)
	return func(yield func(model_core.Message[*model_starlark_pb.Dict_Entry_Leaf, TOutgoingReferences]) bool) {
		allLeaves(func(entry model_core.Message[*model_starlark_pb.Dict_Entry, TOutgoingReferences]) bool {
			leafEntry, ok := entry.Message.Level.(*model_starlark_pb.Dict_Entry_Leaf_)
			if !ok {
				*errOut = errors.New("not a valid leaf entry")
				return false
			}
			return yield(model_core.NewNestedMessage(entry, leafEntry.Leaf))
		})
	}
}
