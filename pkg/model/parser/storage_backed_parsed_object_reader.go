package parser

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/model/encoding"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

type storageBackedParsedObjectReader[TParsedObject any] struct {
	downloader object.Downloader[object.LocalReference]
	encoder    encoding.BinaryEncoder
	parser     ObjectParser[object.LocalReference, TParsedObject]
}

func NewStorageBackedParsedObjectReader[TParsedObject any](downloader object.Downloader[object.LocalReference], encoder encoding.BinaryEncoder, parser ObjectParser[object.LocalReference, TParsedObject]) ParsedObjectReader[object.LocalReference, TParsedObject] {
	return &storageBackedParsedObjectReader[TParsedObject]{
		downloader: downloader,
		encoder:    encoder,
		parser:     parser,
	}
}

func (r *storageBackedParsedObjectReader[TParsedObject]) ReadParsedObject(ctx context.Context, reference object.LocalReference) (TParsedObject, int, error) {
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
