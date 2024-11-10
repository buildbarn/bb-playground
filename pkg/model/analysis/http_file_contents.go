package analysis

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
)

func (c *baseComputer) ComputeHttpFileContentsValue(ctx context.Context, key *model_analysis_pb.HttpFileContents_Key, e HttpFileContentsEnvironment) (PatchedHttpFileContentsValue, error) {
	fileCreationParameters, gotFileCreationParameters := e.GetFileCreationParametersObjectValue(&model_analysis_pb.FileCreationParametersObject_Key{})
	if !gotFileCreationParameters {
		return PatchedHttpFileContentsValue{}, evaluation.ErrMissingDependency
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, key.Url, nil)
	if err != nil {
		return PatchedHttpFileContentsValue{}, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return PatchedHttpFileContentsValue{}, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// Download the file to the local system.
		downloadedFile, err := c.filePool.NewFile()
		if err != nil {
			return PatchedHttpFileContentsValue{}, err
		}
		if _, err := io.Copy(&sectionWriter{w: downloadedFile}, resp.Body); err != nil {
			downloadedFile.Close()
			return PatchedHttpFileContentsValue{}, err
		}

		if key.Integrity != "" {
			// TODO: Validate integrity of the downloaded file!
		}

		// Compute a Merkle tree of the file. Don't keep any
		// chunks of data in memory, as we would consume a large
		// amount of memory otherwise.
		fileMerkleTree, err := model_filesystem.CreateFileMerkleTree(
			ctx,
			fileCreationParameters,
			io.NewSectionReader(downloadedFile, 0, math.MaxInt64),
			model_filesystem.ChunkDiscardingFileMerkleTreeCapturer,
		)
		if err != nil {
			downloadedFile.Close()
			return PatchedHttpFileContentsValue{}, err
		}

		if fileMerkleTree.Message == nil {
			// Downloaded file is empty. We can close the
			// file immediately.
			downloadedFile.Close()
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpFileContents_Value{
				Exists: &model_analysis_pb.HttpFileContents_Value_Exists{},
			}), nil
		}

		// Downloaded file is non-empty. Attach the file's contents to
		// the result. The file will be closed once the upload is
		// completed.
		return PatchedHttpFileContentsValue{
			Message: &model_analysis_pb.HttpFileContents_Value{
				Exists: &model_analysis_pb.HttpFileContents_Value_Exists{
					Contents: fileMerkleTree.Message,
				},
			},
			Patcher: model_core.MapReferenceMessagePatcherMetadata(
				fileMerkleTree.Patcher,
				func(reference object.LocalReference, metadata model_filesystem.CapturedObject) dag.ObjectContentsWalker {
					return model_filesystem.NewCapturedFileWalker(
						fileCreationParameters,
						downloadedFile,
						reference,
						fileMerkleTree.Message.TotalSizeBytes,
						&metadata,
					)
				},
			),
		}, nil
	case http.StatusNotFound:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpFileContents_Value{}), nil
	default:
		return PatchedHttpFileContentsValue{}, fmt.Errorf("received unexpected HTTP response %#v", resp.Status)
	}
}

// sectionWriter provides an implementation of io.Writer on top of
// io.WriterAt. It is similar to io.SectionReader, but then for writes.
type sectionWriter struct {
	w           io.WriterAt
	offsetBytes int64
}

func (w *sectionWriter) Write(p []byte) (int, error) {
	n, err := w.w.WriteAt(p, w.offsetBytes)
	w.offsetBytes += int64(n)
	return n, err
}

func (w *sectionWriter) WriteString(s string) (int, error) {
	n, err := w.w.WriteAt([]byte(s), w.offsetBytes)
	w.offsetBytes += int64(n)
	return n, err
}
