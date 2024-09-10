package parser

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/model/encoding"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type storageBackedParsedObjectReader[TReference, TParsedObject any] struct {
	downloader object.Downloader[TReference]
	encoder    encoding.BinaryEncoder
	parser     ObjectParser[TReference, TParsedObject]
}

func NewStorageBackedParsedObjectReader[TReference, TParsedObject any](downloader object.Downloader[TReference], encoder encoding.BinaryEncoder, parser ObjectParser[TReference, TParsedObject]) ParsedObjectReader[TReference, TParsedObject] {
	return &storageBackedParsedObjectReader[TReference, TParsedObject]{
		downloader: downloader,
		encoder:    encoder,
		parser:     parser,
	}
}

func (r *storageBackedParsedObjectReader[TReference, TParsedObject]) ReadParsedObject(ctx context.Context, reference TReference) (TParsedObject, int, error) {
	var badParsedObject TParsedObject
	contents, err := r.downloader.DownloadObject(ctx, reference)
	if err != nil {
		return badParsedObject, 0, err
	}
	data, err := r.encoder.DecodeBinary(contents.GetPayload())
	if err != nil {
		return badParsedObject, 0, util.StatusWrap(err, "Failed to decode object contents")
	}
	parsedObject, parsedSizeBytes, err := r.parser.ParseObject(ctx, reference, contents, data)
	if err != nil {
		return badParsedObject, 0, util.StatusWrap(err, "Failed to parse object contents")
	}
	return parsedObject, parsedSizeBytes, nil
}
