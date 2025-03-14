package analysis

import (
	"context"
	"errors"

	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
)

func (c *baseComputer[TReference, TMetadata]) ComputeFilesInPackageValue(ctx context.Context, key *model_analysis_pb.FilesInPackage_Key, e FilesInPackageEnvironment[TReference, TMetadata]) (PatchedFilesInPackageValue, error) {
	return PatchedFilesInPackageValue{}, errors.New("TODO: Implement!")
}
