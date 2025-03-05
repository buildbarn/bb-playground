package analysis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bonanza/pkg/evaluation"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
)

func (c *baseComputer[TReference]) ComputeHttpFileContentsValue(ctx context.Context, key *model_analysis_pb.HttpFileContents_Key, e HttpFileContentsEnvironment[TReference]) (PatchedHttpFileContentsValue, error) {
	fileCreationParameters, gotFileCreationParameters := e.GetFileCreationParametersObjectValue(&model_analysis_pb.FileCreationParametersObject_Key{})
	if !gotFileCreationParameters {
		return PatchedHttpFileContentsValue{}, evaluation.ErrMissingDependency
	}

ProcessURLs:
	for _, url := range key.Urls {
		// Store copies of the file in a local cache directory.
		// TODO: Remove this feature once our storage is robust enough.
		urlHash := sha256.Sum256([]byte(url))
		filename := path.MustNewComponent(hex.EncodeToString(urlHash[:]))
		downloadedFile, err := c.cacheDirectory.OpenReadWrite(filename, filesystem.DontCreate)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return PatchedHttpFileContentsValue{}, err
			}
			downloadedFile, err = c.cacheDirectory.OpenReadWrite(filename, filesystem.CreateExcl(0o666))
			if err != nil {
				return PatchedHttpFileContentsValue{}, err
			}

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				downloadedFile.Close()
				return PatchedHttpFileContentsValue{}, err
			}

			resp, err := c.httpClient.Do(req)
			if err != nil {
				downloadedFile.Close()
				return PatchedHttpFileContentsValue{}, err
			}
			defer resp.Body.Close()

			switch resp.StatusCode {
			case http.StatusOK:
				// Download the file to the local system.
				if _, err := io.Copy(model_filesystem.NewSectionWriter(downloadedFile), resp.Body); err != nil {
					downloadedFile.Close()
					c.cacheDirectory.Remove(filename)
					return PatchedHttpFileContentsValue{}, err
				}
			case http.StatusNotFound:
				downloadedFile.Close()
				c.cacheDirectory.Remove(filename)
				continue ProcessURLs
			default:
				downloadedFile.Close()
				c.cacheDirectory.Remove(filename)
				if key.AllowFail {
					continue ProcessURLs
				}
				return PatchedHttpFileContentsValue{}, fmt.Errorf("received unexpected HTTP response %#v", resp.Status)
			}
		}

		if key.Integrity != "" {
			// TODO: Validate integrity of the downloaded file!
		}

		// Compute a Merkle tree of the file and return it. The
		// downloaded file is removed after uploading completes.
		// any chunks of data in memory, as we would consume a large
		// amount of memory otherwise.
		fileMerkleTree, err := model_filesystem.CreateChunkDiscardingFileMerkleTree(ctx, fileCreationParameters, downloadedFile)
		if err != nil {
			return PatchedHttpFileContentsValue{}, err
		}
		if fileMerkleTree.Message == nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpFileContents_Value{
				Exists: &model_analysis_pb.HttpFileContents_Value_Exists{},
			}), nil
		}
		return PatchedHttpFileContentsValue{
			Message: &model_analysis_pb.HttpFileContents_Value{
				Exists: &model_analysis_pb.HttpFileContents_Value_Exists{
					Contents: fileMerkleTree.Message,
				},
			},
			Patcher: fileMerkleTree.Patcher,
		}, nil
	}

	// File not found.
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.HttpFileContents_Value{}), nil
}
