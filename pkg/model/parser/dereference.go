package parser

import (
	"context"

	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

// Dereference a reference message, returning the value that's
// associated with it.
func Dereference[
	TValue any,
	TOutgoingReferences object.OutgoingReferences[TReference],
	TReference any,
](
	ctx context.Context,
	reader ParsedObjectReader[TReference, TValue],
	m model_core.Message[*model_core_pb.Reference, TOutgoingReferences],
) (TValue, error) {
	reference, err := model_core.FlattenReference(m)
	if err != nil {
		var bad TValue
		return bad, nil
	}
	value, _, err := reader.ReadParsedObject(ctx, reference)
	return value, err
}
