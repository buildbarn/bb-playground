package dereference

import (
	"context"

	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

type readingDereferencer[T any] struct {
	reader model_parser.ParsedObjectReader[object.LocalReference, T]
}

// NewReadingDereferencer is a simple implementation of Dereferencer
// that calls into a model_parser.ParsedObjectReader to download an
// object and parse it.
//
// TODO: This implementation should likely be removed in the long run,
// as it is too simple and inefficient for production workloads.
func NewReadingDereferencer[T any](reader model_parser.ParsedObjectReader[object.LocalReference, T]) Dereferencer[T, object.OutgoingReferences[object.LocalReference]] {
	return &readingDereferencer[T]{
		reader: reader,
	}
}

func (d *readingDereferencer[T]) Dereference(ctx context.Context, outgoingReferences object.OutgoingReferences[object.LocalReference], index int) (T, error) {
	v, _, err := d.reader.ReadParsedObject(ctx, outgoingReferences.GetOutgoingReference(index))
	return v, err
}
