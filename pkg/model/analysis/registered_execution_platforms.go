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

	"google.golang.org/protobuf/encoding/protojson"

	"go.starlark.net/starlark"
)

type registeredExecutionPlatformExtractingModuleDotBazelHandler struct {
	environment    RegisteredExecutionPlatformsEnvironment
	moduleInstance label.ModuleInstance
}

func (registeredExecutionPlatformExtractingModuleDotBazelHandler) BazelDep(name label.Module, version *label.ModuleVersion, maxCompatibilityLevel int, repoName label.ApparentRepo, devDependency bool) error {
	return nil
}

func (registeredExecutionPlatformExtractingModuleDotBazelHandler) Module(name label.Module, version *label.ModuleVersion, compatibilityLevel int, repoName label.ApparentRepo, bazelCompatibility []string) error {
	return nil
}

func (h *registeredExecutionPlatformExtractingModuleDotBazelHandler) RegisterExecutionPlatforms(platformLabels []label.ApparentLabel, devDependency bool) error {
	for _, apparentPlatformLabel := range platformLabels {
		canonicalPlatformLabel, err := resolveApparentLabel(h.environment, h.moduleInstance.GetBareCanonicalRepo(), apparentPlatformLabel)
		if err != nil {
			return err
		}
		configuredTargetValue := h.environment.GetConfiguredTargetValue(&model_analysis_pb.ConfiguredTarget_Key{
			Label: canonicalPlatformLabel.String(),
		})
		if !configuredTargetValue.IsSet() {
			return evaluation.ErrMissingDependency
		}
		panic(protojson.Format(configuredTargetValue.Message))
	}
	return nil
}

func (registeredExecutionPlatformExtractingModuleDotBazelHandler) RegisterToolchains(toolchainLabels []label.ApparentLabel, devDependency bool) error {
	return nil
}

func (registeredExecutionPlatformExtractingModuleDotBazelHandler) UseExtension(extensionBzlFile label.ApparentLabel, extensionName string, devDependency, isolate bool) (pg_starlark.ModuleExtensionProxy, error) {
	return pg_starlark.NullModuleExtensionProxy, nil
}

func (registeredExecutionPlatformExtractingModuleDotBazelHandler) UseRepoRule(repoRuleBzlFile label.ApparentLabel, repoRuleName string) (pg_starlark.RepoRuleProxy, error) {
	return func(name label.ApparentRepo, devDependency bool, attrs map[string]starlark.Value) error {
		return nil
	}, nil
}

func (c *baseComputer) ComputeRegisteredExecutionPlatformsValue(ctx context.Context, key *model_analysis_pb.RegisteredExecutionPlatforms_Key, e RegisteredExecutionPlatformsEnvironment) (PatchedRegisteredExecutionPlatformsValue, error) {
	allModuleInstancesValue := e.GetAllModuleInstancesValue(&model_analysis_pb.AllModuleInstances_Key{})
	if !allModuleInstancesValue.IsSet() {
		return PatchedRegisteredExecutionPlatformsValue{}, evaluation.ErrMissingDependency
	}
	var allModuleInstances []string
	switch result := allModuleInstancesValue.Message.Result.(type) {
	case *model_analysis_pb.AllModuleInstances_Value_Success_:
		allModuleInstances = result.Success.ModuleInstances
	case *model_analysis_pb.AllModuleInstances_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredExecutionPlatforms_Value{
			Result: &model_analysis_pb.RegisteredExecutionPlatforms_Value_Failure{
				Failure: result.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredExecutionPlatforms_Value{
			Result: &model_analysis_pb.RegisteredExecutionPlatforms_Value_Failure{
				Failure: "All module instances value has an unknown result type",
			},
		}), nil
	}

	fileReader, err := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if err != nil {
		if !errors.Is(err, evaluation.ErrMissingDependency) {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredExecutionPlatforms_Value{
				Result: &model_analysis_pb.RegisteredExecutionPlatforms_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}
		return PatchedRegisteredExecutionPlatformsValue{}, err
	}

	missingDependency := false
	for _, moduleInstanceStr := range allModuleInstances {
		moduleInstance, err := label.NewModuleInstance(moduleInstanceStr)
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredExecutionPlatforms_Value{
				Result: &model_analysis_pb.RegisteredExecutionPlatforms_Value_Failure{
					Failure: fmt.Sprintf("Invalid module instance %#v: %s", moduleInstanceStr, err),
				},
			}), nil
		}
		handler := registeredExecutionPlatformExtractingModuleDotBazelHandler{
			environment:    e,
			moduleInstance: moduleInstance,
		}
		if err := c.parseModuleInstanceModuleDotBazel(ctx, moduleInstance, e, fileReader, &handler); err != nil {
			if !errors.Is(err, evaluation.ErrMissingDependency) {
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredExecutionPlatforms_Value{
					Result: &model_analysis_pb.RegisteredExecutionPlatforms_Value_Failure{
						Failure: err.Error(),
					},
				}), nil
			}
			missingDependency = true
			continue
		}
	}
	if missingDependency {
		return PatchedRegisteredExecutionPlatformsValue{}, evaluation.ErrMissingDependency
	}

	panic("TODO")
}
