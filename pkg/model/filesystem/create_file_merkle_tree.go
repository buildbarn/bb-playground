package filesystem

import (
	"context"
	"io"
	"math"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/btree"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"
	cdc "github.com/buildbarn/go-cdc"
)

// CreateFileMerkleTree creates a Merkle tree structure that corresponds
// to the contents of a single file. If a file is small, it stores all
// of its contents in a single object. If a file is large, it creates a
// B-tree.
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
			/* parentNodeComputer = */ func(createdObject model_core.CreatedObject[T], childNodes []*model_filesystem_pb.FileContents) (model_core.PatchedMessage[*model_filesystem_pb.FileContents, T], error) {
				// Compute the total file size to store
				// in the parent FileContents node.
				var totalSizeBytes uint64
				for _, childNode := range childNodes {
					totalSizeBytes += childNode.TotalSizeBytes
				}

				patcher := model_core.NewReferenceMessagePatcher[T]()
				return model_core.NewPatchedMessage(
					&model_filesystem_pb.FileContents{
						TotalSizeBytes: totalSizeBytes,
						Reference: patcher.AddReference(
							createdObject.Contents.GetReference(),
							capturer.CaptureFileContentsList(createdObject),
						),
					},
					patcher,
				), nil
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
				// Emit the final lists of FileContents
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
		if err := treeBuilder.PushChild(model_core.NewPatchedMessage(
			&model_filesystem_pb.FileContents{
				Reference:      patcher.AddReference(chunkContents.GetReference(), capturer.CaptureChunk(chunkContents)),
				TotalSizeBytes: uint64(len(chunk)),
			},
			patcher,
		)); err != nil {
			return model_core.PatchedMessage[*model_filesystem_pb.FileContents, T]{}, err
		}
	}
}

// CreateChunkDiscardingFileMerkleTree is a helper function for creating
// a Merkle tree of a file and immediately constructing an
// ObjectContentsWalker for it. This function takes ownership of the
// file that is provided. Its contents may be re-read when the
// ObjectContentsWalker is accessed, and it will be released when no
// more ObjectContentsWalkers for the file exist.
func CreateChunkDiscardingFileMerkleTree(ctx context.Context, parameters *FileCreationParameters, f filesystem.FileReader) (model_core.PatchedMessage[*model_filesystem_pb.FileContents, dag.ObjectContentsWalker], error) {
	fileContents, err := CreateFileMerkleTree(
		ctx,
		parameters,
		io.NewSectionReader(f, 0, math.MaxInt64),
		ChunkDiscardingFileMerkleTreeCapturer,
	)
	if err != nil {
		f.Close()
		return model_core.PatchedMessage[*model_filesystem_pb.FileContents, dag.ObjectContentsWalker]{}, err
	}

	if !fileContents.IsSet() {
		// File is empty. Close the file immediately, so that it
		// doesn't leak.
		f.Close()
		return model_core.PatchedMessage[*model_filesystem_pb.FileContents, dag.ObjectContentsWalker]{}, err
	}

	return model_core.PatchedMessage[*model_filesystem_pb.FileContents, dag.ObjectContentsWalker]{
		Message: fileContents.Message,
		Patcher: model_core.MapReferenceMessagePatcherMetadata(
			fileContents.Patcher,
			func(reference object.LocalReference, metadata CapturedObject) dag.ObjectContentsWalker {
				return NewCapturedFileWalker(
					parameters,
					f,
					reference,
					fileContents.Message.TotalSizeBytes,
					&metadata,
				)
			},
		),
	}, nil
}
