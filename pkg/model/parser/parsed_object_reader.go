package parser

import (
	"context"
)

type ParsedObjectReader[TReference, TParsedObject any] interface {
	ReadParsedObject(ctx context.Context, reference TReference) (TParsedObject, error)
}
