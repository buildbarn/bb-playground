package core

import (
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

// WalkableReferenceMetadata can be implemented by reference metadata to
// indicate that they can be converted to a dag.ObjectContentsWalker,
// allowing the contents of any captured objects to be obtained up to
// once. This allows them to be written to a storage server.
type WalkableReferenceMetadata interface {
	ReferenceMetadata

	ToObjectContentsWalker() dag.ObjectContentsWalker
}

func MapReferenceMetadataToWalkers[TMetadata WalkableReferenceMetadata](p *ReferenceMessagePatcher[TMetadata]) *ReferenceMessagePatcher[dag.ObjectContentsWalker] {
	return MapReferenceMessagePatcherMetadata(
		p,
		func(reference object.LocalReference, metadata TMetadata) dag.ObjectContentsWalker {
			return metadata.ToObjectContentsWalker()
		},
	)
}
