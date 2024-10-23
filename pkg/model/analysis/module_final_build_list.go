package analysis

import (
	"context"
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
)

func (c *baseComputer) ComputeModuleFinalBuildListValue(ctx context.Context, key *model_analysis_pb.ModuleFinalBuildList_Key, e ModuleFinalBuildListEnvironment) (PatchedModuleFinalBuildListValue, error) {
	roughBuildListValue := e.GetModuleRoughBuildListValue(&model_analysis_pb.ModuleRoughBuildList_Key{})
	if !roughBuildListValue.IsSet() {
		return PatchedModuleFinalBuildListValue{}, evaluation.ErrMissingDependency
	}

	switch roughBuildListResult := roughBuildListValue.Message.Result.(type) {
	case *model_analysis_pb.ModuleRoughBuildList_Value_Success:
		var buildList []*model_analysis_pb.BuildList_Module
		var previousVersionStr string
		var previousVersion label.ModuleVersion
		roughBuildList := roughBuildListResult.Success.Modules
		for i, module := range roughBuildList {
			version, err := label.NewModuleVersion(module.Version)
			if err != nil {
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleFinalBuildList_Value{
					Result: &model_analysis_pb.ModuleFinalBuildList_Value_Failure{
						Failure: fmt.Sprintf("Module %#v has invalid version %#v: %s", module.Name, module.Version, err),
					},
				}), nil
			}

			if len(buildList) == 0 || buildList[len(buildList)-1].Name != module.Name {
				// New module.
				buildList = append(buildList, module)
			} else if cmp := previousVersion.Compare(version); cmp < 0 {
				// Same module, but a higher version.
				buildList[len(buildList)-1] = module
			} else if cmp == 0 && (i+1 >= len(roughBuildList) || module.Name != roughBuildList[i+1].Name) {
				// Prevent selection process from being
				// non-deterministic.
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleFinalBuildList_Value{
					Result: &model_analysis_pb.ModuleFinalBuildList_Value_Failure{
						Failure: fmt.Sprintf("Module %#v has ambiguous highest versions %#v and %#v", module.Name, previousVersionStr, module.Version),
					},
				}), nil
			}

			previousVersionStr = module.Version
			previousVersion = version
		}

		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleFinalBuildList_Value{
			Result: &model_analysis_pb.ModuleFinalBuildList_Value_Success{
				Success: &model_analysis_pb.BuildList{
					Modules: buildList,
				},
			},
		}), nil
	case *model_analysis_pb.ModuleRoughBuildList_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleFinalBuildList_Value{
			Result: &model_analysis_pb.ModuleFinalBuildList_Value_Failure{
				Failure: roughBuildListResult.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleFinalBuildList_Value{
			Result: &model_analysis_pb.ModuleFinalBuildList_Value_Failure{
				Failure: "Rough build list value has an unknown result type",
			},
		}), nil
	}
}
