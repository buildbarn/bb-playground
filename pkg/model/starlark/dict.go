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

func AllDictLeafEntries(
	ctx context.Context,
	reader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[[]*model_starlark_pb.Dict_Entry]],
	rootDict model_core.Message[*model_starlark_pb.Dict],
	errOut *error,
) iter.Seq[model_core.Message[*model_starlark_pb.Dict_Entry_Leaf]] {
	allLeaves := btree.AllLeaves(
		ctx,
		reader,
		model_core.Message[[]*model_starlark_pb.Dict_Entry]{
			Message:            rootDict.Message.Entries,
			OutgoingReferences: rootDict.OutgoingReferences,
		},
		func(entry model_core.Message[*model_starlark_pb.Dict_Entry]) (*model_core_pb.Reference, error) {
			if parent, ok := entry.Message.Level.(*model_starlark_pb.Dict_Entry_Parent_); ok {
				return parent.Parent.Reference, nil
			}
			return nil, nil
		},
		errOut,
	)
	return func(yield func(model_core.Message[*model_starlark_pb.Dict_Entry_Leaf]) bool) {
		allLeaves(func(entry model_core.Message[*model_starlark_pb.Dict_Entry]) bool {
			leafEntry, ok := entry.Message.Level.(*model_starlark_pb.Dict_Entry_Leaf_)
			if !ok {
				*errOut = errors.New("not a valid leaf entry")
				return false
			}
			return yield(model_core.Message[*model_starlark_pb.Dict_Entry_Leaf]{
				Message:            leafEntry.Leaf,
				OutgoingReferences: entry.OutgoingReferences,
			})
		})
	}
}
