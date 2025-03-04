package dereference

import (
	"context"

	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

// Dereferencer of references. Given the outgoing references of an
// object, this type is responsible for following references of a given
// type (e.g., Directory) and returning the data associated with the
// referenced object in parsed form.
//
// Simple implementations of Dereferencer may always attempt to download
// objects from storage. More complex implementations may provide
// caching.
type Dereferencer[TValue any, TReference any] interface {
	Dereference(ctx context.Context, reference TReference) (TValue, error)
}

// Dereference a model_core_pb.Reference. This is a helper function for
// following references that are stored in Protobuf messages.
func Dereference[TValue any, TOutgoingReferences object.OutgoingReferences[TReference], TReference any](ctx context.Context, dereferencer Dereferencer[TValue, TReference], m model_core.Message[*model_core_pb.Reference, TOutgoingReferences]) (TValue, error) {
	reference, err := model_core.FlattenReference(m)
	if err != nil {
		var badValue TValue
		return badValue, err
	}
	return dereferencer.Dereference(ctx, reference)
}
