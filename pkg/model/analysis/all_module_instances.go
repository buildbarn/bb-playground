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

func (c *baseComputer) ComputeAllModuleInstancesValue(ctx context.Context, key *model_analysis_pb.AllModuleInstances_Key, e AllModuleInstancesEnvironment) (PatchedAllModuleInstancesValue, error) {
	// The list of all module instances can be obtained by merging
	// the list of modules for which we have overrides with ones we
	// are downloading from registries.
	finalBuildListValue := e.GetModuleFinalBuildListValue(&model_analysis_pb.ModuleFinalBuildList_Key{})
	modulesWithOverridesValue := e.GetModulesWithOverridesValue(&model_analysis_pb.ModulesWithOverrides_Key{})
	if !finalBuildListValue.IsSet() || !modulesWithOverridesValue.IsSet() {
		return PatchedAllModuleInstancesValue{}, evaluation.ErrMissingDependency
	}

	var buildList []*model_analysis_pb.BuildList_Module
	switch result := finalBuildListValue.Message.Result.(type) {
	case *model_analysis_pb.ModuleFinalBuildList_Value_Success:
		buildList = result.Success.Modules
	case *model_analysis_pb.ModuleFinalBuildList_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.AllModuleInstances_Value{
			Result: &model_analysis_pb.AllModuleInstances_Value_Failure{
				Failure: result.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.AllModuleInstances_Value{
			Result: &model_analysis_pb.AllModuleInstances_Value_Failure{
				Failure: "Final build list value has an unknown result type",
			},
		}), nil
	}

	var overridesList []*model_analysis_pb.OverridesList_Module
	switch result := modulesWithOverridesValue.Message.Result.(type) {
	case *model_analysis_pb.ModulesWithOverrides_Value_Success:
		overridesList = result.Success.Modules
	case *model_analysis_pb.ModulesWithOverrides_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.AllModuleInstances_Value{
			Result: &model_analysis_pb.AllModuleInstances_Value_Failure{
				Failure: result.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.AllModuleInstances_Value{
			Result: &model_analysis_pb.AllModuleInstances_Value_Failure{
				Failure: "Final build list value has an unknown result type",
			},
		}), nil
	}

	var moduleInstances []string
	for len(buildList) > 0 || len(overridesList) > 0 {
		var moduleNameStr string
		var moduleVersions []string
		if len(overridesList) == 0 || buildList[0].Name <= overridesList[0].Name {
			moduleNameStr = buildList[0].Name
			buildList = buildList[1:]
		} else {
			moduleNameStr = overridesList[0].Name
			moduleVersions = overridesList[0].Versions
			overridesList = overridesList[1:]
		}

		moduleName, err := label.NewModule(moduleNameStr)
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.AllModuleInstances_Value{
				Result: &model_analysis_pb.AllModuleInstances_Value_Failure{
					Failure: fmt.Sprintf("Invalid module name %#v: %s", moduleNameStr, err),
				},
			}), nil
		}
		if len(moduleVersions) == 0 {
			moduleInstances = append(moduleInstances, moduleName.ToModuleInstance(nil).String())
		} else {
			for _, moduleVersionStr := range moduleVersions {
				moduleVersion, err := label.NewModuleVersion(moduleVersionStr)
				if err != nil {
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.AllModuleInstances_Value{
						Result: &model_analysis_pb.AllModuleInstances_Value_Failure{
							Failure: fmt.Sprintf("Invalid module version %#v for module %#v: %s", moduleVersionStr, moduleNameStr, err),
						},
					}), nil
				}
				moduleInstances = append(moduleInstances, moduleName.ToModuleInstance(&moduleVersion).String())
			}
		}
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.AllModuleInstances_Value{
		Result: &model_analysis_pb.AllModuleInstances_Value_Success_{
			Success: &model_analysis_pb.AllModuleInstances_Value_Success{
				ModuleInstances: moduleInstances,
			},
		},
	}), nil
}
