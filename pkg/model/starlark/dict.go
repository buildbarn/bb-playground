package starlark

import (
	"context"
	"errors"
	"iter"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
)

func AllDictLeafEntries(
	ctx context.Context,
	reader model_parser.ParsedObjectReader[object.LocalReference, model_core.Message[*model_starlark_pb.Dict]],
	rootDict model_core.Message[*model_starlark_pb.Dict],
	errOut *error,
) iter.Seq[model_core.Message[*model_starlark_pb.Dict_Entry_Leaf]] {
	dicts := []model_core.Message[[]*model_starlark_pb.Dict_Entry]{{
		Message:            rootDict.Message.Entries,
		OutgoingReferences: rootDict.OutgoingReferences,
	}}
	return func(yield func(leafEntry model_core.Message[*model_starlark_pb.Dict_Entry_Leaf]) bool) {
		for len(dicts) > 0 {
			lastDict := &dicts[len(dicts)-1]
			if len(lastDict.Message) == 0 {
				dicts = dicts[:len(dicts)-1]
			} else {
				entry := lastDict.Message[0]
				lastDict.Message = lastDict.Message[1:]
				switch level := entry.Level.(type) {
				case *model_starlark_pb.Dict_Entry_Leaf_:
					if !yield(model_core.Message[*model_starlark_pb.Dict_Entry_Leaf]{
						Message:            level.Leaf,
						OutgoingReferences: lastDict.OutgoingReferences,
					}) {
						*errOut = nil
						return
					}
				case *model_starlark_pb.Dict_Entry_Parent_:
					index, err := model_core.GetIndexFromReferenceMessage(level.Parent.Reference, lastDict.OutgoingReferences.GetDegree())
					if err != nil {
						*errOut = err
						return
					}
					child, _, err := reader.ReadParsedObject(
						ctx,
						lastDict.OutgoingReferences.GetOutgoingReference(index),
					)
					if err != nil {
						*errOut = err
						return
					}
					dicts = append(dicts, model_core.Message[[]*model_starlark_pb.Dict_Entry]{
						Message:            child.Message.Entries,
						OutgoingReferences: child.OutgoingReferences,
					})
				default:
					*errOut = errors.New("dict entry is of an unknown type")
					return
				}
			}
		}
	}
}
