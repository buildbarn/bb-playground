package parser

import (
	"context"

	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

type downloadingParsedObjectReader[TReference any] struct {
	downloader object.Downloader[TReference]
}

func NewDownloadingParsedObjectReader[TReference any](downloader object.Downloader[TReference]) ParsedObjectReader[TReference, model_core.Message[[]byte, object.LocalReference]] {
	return &downloadingParsedObjectReader[TReference]{
		downloader: downloader,
	}
}

func (r *downloadingParsedObjectReader[TReference]) ReadParsedObject(ctx context.Context, reference TReference) (model_core.Message[[]byte, object.LocalReference], error) {
	contents, err := r.downloader.DownloadObject(ctx, reference)
	if err != nil {
		return model_core.Message[[]byte, object.LocalReference]{}, err
	}
	return model_core.NewMessage(contents.GetPayload(), object.OutgoingReferences[object.LocalReference](contents)), nil
}
