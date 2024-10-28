package analysis

import (
	"context"
	"errors"
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	pg_starlark "github.com/buildbarn/bb-playground/pkg/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type registeredToolchainExtractingModuleDotBazelHandler struct {
	environment    RegisteredToolchainsEnvironment
	moduleInstance label.ModuleInstance
}

func (registeredToolchainExtractingModuleDotBazelHandler) BazelDep(name label.Module, version *label.ModuleVersion, maxCompatibilityLevel int, repoName label.ApparentRepo, devDependency bool) error {
	return nil
}

func (registeredToolchainExtractingModuleDotBazelHandler) Module(name label.Module, version *label.ModuleVersion, compatibilityLevel int, repoName label.ApparentRepo, bazelCompatibility []string) error {
	return nil
}

func (registeredToolchainExtractingModuleDotBazelHandler) RegisterExecutionPlatforms(platformLabels []label.ApparentLabel, devDependency bool) error {
	return nil
}

func (registeredToolchainExtractingModuleDotBazelHandler) RegisterToolchains(toolchainLabels []label.ApparentLabel, devDependency bool) error {
	panic("TODO")
}

func (registeredToolchainExtractingModuleDotBazelHandler) UseExtension(extensionBzlFile label.ApparentLabel, extensionName string, devDependency, isolate bool) (pg_starlark.ModuleExtensionProxy, error) {
	return pg_starlark.NullModuleExtensionProxy, nil
}

func (registeredToolchainExtractingModuleDotBazelHandler) UseRepoRule(repoRuleBzlFile label.ApparentLabel, repoRuleName string) (pg_starlark.RepoRuleProxy, error) {
	return func(name label.ApparentRepo, devDependency bool, attrs map[string]starlark.Value) error {
		return nil
	}, nil
}

func (c *baseComputer) ComputeRegisteredToolchainsValue(ctx context.Context, key *model_analysis_pb.RegisteredToolchains_Key, e RegisteredToolchainsEnvironment) (PatchedRegisteredToolchainsValue, error) {
	allModuleInstancesValue := e.GetAllModuleInstancesValue(&model_analysis_pb.AllModuleInstances_Key{})
	if !allModuleInstancesValue.IsSet() {
		return PatchedRegisteredToolchainsValue{}, evaluation.ErrMissingDependency
	}
	var allModuleInstances []string
	switch result := allModuleInstancesValue.Message.Result.(type) {
	case *model_analysis_pb.AllModuleInstances_Value_Success_:
		allModuleInstances = result.Success.ModuleInstances
	case *model_analysis_pb.AllModuleInstances_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredToolchains_Value{
			Result: &model_analysis_pb.RegisteredToolchains_Value_Failure{
				Failure: result.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredToolchains_Value{
			Result: &model_analysis_pb.RegisteredToolchains_Value_Failure{
				Failure: "All module instances value has an unknown result type",
			},
		}), nil
	}

	fileReader, err := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if err != nil {
		if !errors.Is(err, evaluation.ErrMissingDependency) {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredToolchains_Value{
				Result: &model_analysis_pb.RegisteredToolchains_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}
		return PatchedRegisteredToolchainsValue{}, err
	}

	// TODO: We currently iterate over all modules in alphabetical
	// order. Is this desirable? Maybe we should process the root
	// module first?
	// TODO: We should also respect --extra_toolchains.
	missingDependency := false
	for _, moduleInstanceStr := range allModuleInstances {
		moduleInstance, err := label.NewModuleInstance(moduleInstanceStr)
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredToolchains_Value{
				Result: &model_analysis_pb.RegisteredToolchains_Value_Failure{
					Failure: fmt.Sprintf("Invalid module instance %#v: %s", moduleInstanceStr, err),
				},
			}), nil
		}
		handler := registeredToolchainExtractingModuleDotBazelHandler{
			environment:    e,
			moduleInstance: moduleInstance,
		}
		if err := c.parseModuleInstanceModuleDotBazel(ctx, moduleInstance, e, fileReader, &handler); err != nil {
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredToolchains_Value{
					Result: &model_analysis_pb.RegisteredToolchains_Value_Failure{
						Failure: err.Error(),
					},
				}), nil
			}
			missingDependency = true
			continue
		}
	}
	if missingDependency {
		return PatchedRegisteredToolchainsValue{}, evaluation.ErrMissingDependency
	}

	panic("TODO")
}
