package analysis

import (
	"context"
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
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
			Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
				Failure: fmt.Sprintf("Invalid canonical repo %#v: %s", key.FromCanonicalRepo, err),
			},
		}), nil
	}
	moduleRepoMappingValue := e.GetModuleRepoMappingValue(&model_analysis_pb.ModuleRepoMapping_Key{
		ModuleInstance: fromCanonicalRepo.GetModuleInstance().String(),
	})
	if !moduleRepoMappingValue.IsSet() {
		return PatchedCanonicalRepoNameValue{}, evaluation.ErrMissingDependency
	}
	switch moduleRepoMappingResult := moduleRepoMappingValue.Message.Result.(type) {
	case *model_analysis_pb.ModuleRepoMapping_Value_Success_:
		toApparentRepo := key.ToApparentRepo
		mappings := moduleRepoMappingResult.Success.Mappings
		if i := sort.Search(
			len(mappings),
			func(i int) bool { return mappings[i].ApparentRepo >= toApparentRepo },
		); i < len(mappings) && mappings[i].ApparentRepo == toApparentRepo {
			toCanonicalRepo := mappings[i].CanonicalRepo
			if toCanonicalRepo == "" {
				// TODO: Properly implement module extensions!
				toCanonicalRepo = toApparentRepo + "+"
			}
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
				Result: &model_analysis_pb.CanonicalRepoName_Value_ToCanonicalRepo{
					ToCanonicalRepo: toCanonicalRepo,
				},
			}), nil
		}
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
			Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
				Failure: "Unknown repo",
			},
		}), nil
	case *model_analysis_pb.ModuleRepoMapping_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
			Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
				Failure: moduleRepoMappingResult.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.CanonicalRepoName_Value{
			Result: &model_analysis_pb.CanonicalRepoName_Value_Failure{
				Failure: "Module repo mapping value has an unknown result type",
			},
		}), nil
	}
}
