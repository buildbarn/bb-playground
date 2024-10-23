package filesystem

import (
	"context"
	"io"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-storage/pkg/util"
	cdc "github.com/buildbarn/go-cdc"
)

// CreateFileMerkleTree creates a Merkle tree structure that corresponds
// to the contents of a single file. If a file is small, it stores all
// of its contents in a single object. If a file is large, it creates a
// B-tree, where chunks of data are joined together using a
// FileContentsList message.
//
// Chunking of large files is performed using the MaxCDC algorithm. The
// resulting B-tree is a Prolly tree. This ensures that minor changes to
// a file also result in minor changes to the resulting Merkle tree.
func CreateFileMerkleTree[T model_core.ReferenceMetadata](ctx context.Context, parameters *FileCreationParameters, f io.Reader, capturer FileMerkleTreeCapturer[T]) (model_core.PatchedMessage[*model_filesystem_pb.FileContents, T], error) {
	chunker := cdc.NewMaxContentDefinedChunker(
		f,
		/* bufferSizeBytes = */ max(parameters.referenceFormat.GetMaximumObjectSizeBytes(), parameters.chunkMinimumSizeBytes+parameters.chunkMaximumSizeBytes),
		parameters.chunkMinimumSizeBytes,
		parameters.chunkMaximumSizeBytes,
	)
	treeBuilder := btree.NewUniformProllyBuilder(
		parameters.fileContentsListMinimumSizeBytes,
		parameters.fileContentsListMaximumSizeBytes,
		btree.NewObjectCreatingNodeMerger[*model_filesystem_pb.FileContents, T](
			parameters.fileContentsListEncoder,
			parameters.referenceFormat,
			/* parentNodeComputer = */ func(contents *object.Contents, childNodes []*model_filesystem_pb.FileContents, outgoingReferences object.OutgoingReferences, metadata []T) (model_core.PatchedMessage[*model_filesystem_pb.FileContents, T], error) {
				// Compute the total file size to store
				// in the parent FileContents node.
				var totalSizeBytes uint64
				for _, childNode := range childNodes {
					totalSizeBytes += childNode.TotalSizeBytes
				}

				patcher := model_core.NewReferenceMessagePatcher[T]()
				return model_core.PatchedMessage[*model_filesystem_pb.FileContents, T]{
					Message: &model_filesystem_pb.FileContents{
						TotalSizeBytes: totalSizeBytes,
						Reference:      patcher.AddReference(contents.GetReference(), capturer.CaptureFileContentsList(contents, metadata)),
					},
					Patcher: patcher,
				}, nil
			},
		),
	)

	for {
		// Permit cancelation.
		if err := util.StatusFromContext(ctx); err != nil {
			return model_core.PatchedMessage[*model_filesystem_pb.FileContents, T]{}, err
		}

		// Read the next chunk of data from the file and create
		// a chunk object out of it.
		chunk, err := chunker.ReadNextChunk()
		if err != nil {
			if err == io.EOF {
				// Emit the final FileContentsList
				// messages and return the FileContents
				// message of the file's root.
				return treeBuilder.FinalizeSingle()
			}
			return model_core.PatchedMessage[*model_filesystem_pb.FileContents, T]{}, err
		}
		chunkContents, err := parameters.EncodeChunk(chunk)
		if err != nil {
			return model_core.PatchedMessage[*model_filesystem_pb.FileContents, T]{}, err
		}

		// Insert a FileContents message for it into the B-tree.
		patcher := model_core.NewReferenceMessagePatcher[T]()
		if err := treeBuilder.PushChild(model_core.PatchedMessage[*model_filesystem_pb.FileContents, T]{
			Message: &model_filesystem_pb.FileContents{
				Reference:      patcher.AddReference(chunkContents.GetReference(), capturer.CaptureChunk(chunkContents)),
				TotalSizeBytes: uint64(len(chunk)),
			},
			Patcher: patcher,
		}); err != nil {
			return model_core.PatchedMessage[*model_filesystem_pb.FileContents, T]{}, err
		}
	}
}
