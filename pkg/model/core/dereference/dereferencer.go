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
//
// By extending TOutgoingReferences, it is possible to let objects
// reference each other directory, making it possible to implement
// Dereferencer in such a way that it merely extracts the target object
// from TOutgoingReferences. This removes the need for having a central
// namespace in which objects live.
type Dereferencer[TValue any, TOutgoingReferences any] interface {
	Dereference(ctx context.Context, outgoingReferences TOutgoingReferences, index int) (TValue, error)
}

// Dereference a model_core_pb.Reference. This is a helper function for
// following references that are stored in Protobuf messages.
func Dereference[TValue any, TOutgoingReferences object.OutgoingReferences[TReference], TReference any](ctx context.Context, dereferencer Dereferencer[TValue, TOutgoingReferences], m model_core.Message[*model_core_pb.Reference, TOutgoingReferences]) (TValue, error) {
	index, err := model_core.GetIndexFromReferenceMessage(m.Message, m.OutgoingReferences.GetDegree())
	if err != nil {
		var badValue TValue
		return badValue, err
	}
	return dereferencer.Dereference(ctx, m.OutgoingReferences, index)
}
