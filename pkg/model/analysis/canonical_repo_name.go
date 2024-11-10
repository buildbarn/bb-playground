package analysis

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeCanonicalRepoNameValue(ctx context.Context, key *model_analysis_pb.CanonicalRepoName_Key, e CanonicalRepoNameEnvironment) (PatchedCanonicalRepoNameValue, error) {
	fromCanonicalRepo, err := label.NewCanonicalRepo(key.FromCanonicalRepo)
	if err != nil {
		return PatchedCanonicalRepoNameValue{}, fmt.Errorf("invalid canonical repo: %w", err)
	}
	moduleRepoMappingValue := e.GetModuleRepoMappingValue(&model_analysis_pb.ModuleRepoMapping_Key{
		ModuleInstance: fromCanonicalRepo.GetModuleInstance().String(),
	})
	if !moduleRepoMappingValue.IsSet() {
		return PatchedCanonicalRepoNameValue{}, evaluation.ErrMissingDependency
	}
	toApparentRepo := key.ToApparentRepo
	mappings := moduleRepoMappingValue.Message.Mappings
	mappingIndex := sort.Search(
		len(mappings),
		func(i int) bool { return mappings[i].FromApparentRepo >= toApparentRepo },
	)
	if mappingIndex >= len(mappings) || mappings[mappingIndex].FromApparentRepo != toApparentRepo {
		return PatchedCanonicalRepoNameValue{}, errors.New("unknown repo")
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
		ToCanonicalRepo: mappings[mappingIndex].ToCanonicalRepo,
	}), nil
}
