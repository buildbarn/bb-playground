package analysis

import (
	"context"
	"errors"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
)

func (c *baseComputer[TReference, TMetadata]) ComputeGlobValue(ctx context.Context, key *model_analysis_pb.Glob_Key, e GlobEnvironment[TReference, TMetadata]) (PatchedGlobValue, error) {
	filesInPackageValue := e.GetFilesInPackageValue(&model_analysis_pb.FilesInPackage_Key{
		Package: key.Package,
	})
	if !filesInPackageValue.IsSet() {
		return PatchedGlobValue{}, evaluation.ErrMissingDependency
	}
	return PatchedGlobValue{}, errors.New("TODO: Implement!")
}
