package parser

import (
	"context"

	"github.com/buildbarn/bonanza/pkg/storage/object"
)

type ObjectParser[TReference, TParsedObject any] interface {
	ParseObject(ctx context.Context, reference TReference, outgoingReferences object.OutgoingReferences[TReference], data []byte) (TParsedObject, int, error)
}
