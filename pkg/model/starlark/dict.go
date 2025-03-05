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
)

func AllDictLeafEntries[TReference any](
	ctx context.Context,
	reader model_parser.ParsedObjectReader[TReference, model_core.Message[[]*model_starlark_pb.Dict_Entry, TReference]],
	rootDict model_core.Message[*model_starlark_pb.Dict, TReference],
	errOut *error,
) iter.Seq[model_core.Message[*model_starlark_pb.Dict_Entry_Leaf, TReference]] {
	allLeaves := btree.AllLeaves(
		ctx,
		reader,
		model_core.NewNestedMessage(rootDict, rootDict.Message.Entries),
		func(entry model_core.Message[*model_starlark_pb.Dict_Entry, TReference]) (*model_core_pb.Reference, error) {
			if parent, ok := entry.Message.Level.(*model_starlark_pb.Dict_Entry_Parent_); ok {
				return parent.Parent.Reference, nil
			}
			return nil, nil
		},
		errOut,
	)
	return func(yield func(model_core.Message[*model_starlark_pb.Dict_Entry_Leaf, TReference]) bool) {
		allLeaves(func(entry model_core.Message[*model_starlark_pb.Dict_Entry, TReference]) bool {
			leafEntry, ok := entry.Message.Level.(*model_starlark_pb.Dict_Entry_Leaf_)
			if !ok {
				*errOut = errors.New("not a valid leaf entry")
				return false
			}
			return yield(model_core.NewNestedMessage(entry, leafEntry.Leaf))
		})
	}
}
