package analysis

import (
	"context"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	pg_starlark "github.com/buildbarn/bb-playground/pkg/starlark"

	"go.starlark.net/starlark"
)

type registeredToolchainExtractingModuleDotBazelHandler struct {
	environment           RegisteredToolchainsEnvironment
	moduleInstance        label.ModuleInstance
	ignoreDevDependencies bool
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

func (h *registeredToolchainExtractingModuleDotBazelHandler) RegisterToolchains(toolchainLabels []label.ApparentLabel, devDependency bool) error {
	if !devDependency || !h.ignoreDevDependencies {
		for _, apparentToolchainLabel := range toolchainLabels {
			canonicalPlatformLabel, err := resolveApparentLabel(h.environment, h.moduleInstance.GetBareCanonicalRepo(), apparentToolchainLabel)
			if err != nil {
				return err
			}
			configuredTargetValue := h.environment.GetConfiguredTargetValue(&model_analysis_pb.ConfiguredTarget_Key{
				Label: canonicalPlatformLabel.String(),
			})
			if !configuredTargetValue.IsSet() {
				return evaluation.ErrMissingDependency
			}
			panic("TODO")
		}
	}
	return nil
}

func (registeredToolchainExtractingModuleDotBazelHandler) UseExtension(extensionBzlFile label.ApparentLabel, extensionName label.StarlarkIdentifier, devDependency, isolate bool) (pg_starlark.ModuleExtensionProxy, error) {
	return pg_starlark.NullModuleExtensionProxy, nil
}

func (registeredToolchainExtractingModuleDotBazelHandler) UseRepoRule(repoRuleBzlFile label.ApparentLabel, repoRuleName string) (pg_starlark.RepoRuleProxy, error) {
	return func(name label.ApparentRepo, devDependency bool, attrs map[string]starlark.Value) error {
		return nil
	}, nil
}

func (c *baseComputer) ComputeRegisteredToolchainsValue(ctx context.Context, key *model_analysis_pb.RegisteredToolchains_Key, e RegisteredToolchainsEnvironment) (PatchedRegisteredToolchainsValue, error) {
	if err := c.visitModuleDotBazelFilesBreadthFirst(ctx, e, func(moduleInstance label.ModuleInstance, ignoreDevDependencies bool) pg_starlark.ChildModuleDotBazelHandler {
		return &registeredToolchainExtractingModuleDotBazelHandler{
			environment:           e,
			moduleInstance:        moduleInstance,
			ignoreDevDependencies: ignoreDevDependencies,
		}
	}); err != nil {
		return PatchedRegisteredToolchainsValue{}, err
	}

	panic("TODO")
}
