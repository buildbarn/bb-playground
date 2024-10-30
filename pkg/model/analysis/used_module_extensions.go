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

type usedModuleExtension struct {
	moduleExtension *ModuleExtension
	users           map[label.ModuleInstance]*moduleExtensionUser
}

type moduleExtensionUser struct{}

type usedModuleExtensionProxy struct {
	user          *moduleExtensionUser
	devDependency bool
}

func (usedModuleExtensionProxy) Tag(className string, attrs map[string]starlark.Value) error {
	return nil
}

func (usedModuleExtensionProxy) UseRepo(repos map[label.ApparentRepo]label.ApparentRepo) error {
	return nil
}

type usedModuleExtensionExtractingModuleDotBazelHandler struct {
	environment           UsedModuleExtensionsEnvironment
	moduleInstance        label.ModuleInstance
	ignoreDevDependencies bool
	usedModuleExtensions  map[label.ModuleExtension]*usedModuleExtension
}

func (usedModuleExtensionExtractingModuleDotBazelHandler) BazelDep(name label.Module, version *label.ModuleVersion, maxCompatibilityLevel int, repoName label.ApparentRepo, devDependency bool) error {
	return nil
}

func (usedModuleExtensionExtractingModuleDotBazelHandler) Module(name label.Module, version *label.ModuleVersion, compatibilityLevel int, repoName label.ApparentRepo, bazelCompatibility []string) error {
	return nil
}

func (usedModuleExtensionExtractingModuleDotBazelHandler) RegisterExecutionPlatforms(platformLabels []label.ApparentLabel, devDependency bool) error {
	return nil
}

func (usedModuleExtensionExtractingModuleDotBazelHandler) RegisterToolchains(toolchainLabels []label.ApparentLabel, devDependency bool) error {
	return nil
}

func (h *usedModuleExtensionExtractingModuleDotBazelHandler) UseExtension(extensionBzlFile label.ApparentLabel, extensionName label.StarlarkIdentifier, devDependency, isolate bool) (pg_starlark.ModuleExtensionProxy, error) {
	if devDependency && h.ignoreDevDependencies {
		return pg_starlark.NullModuleExtensionProxy, nil
	}

	// Look up the module extension properties, so that we obtain
	// the canonical identifier of the Starlark module_extension
	// declaration.
	canonicalExtensionBzlFile, err := resolveApparentLabel(h.environment, h.moduleInstance.GetBareCanonicalRepo(), extensionBzlFile)
	if err != nil {
		return nil, err
	}
	moduleExtensionIdentifier := canonicalExtensionBzlFile.AppendStarlarkIdentifier(extensionName).String()
	moduleExtension, err := h.environment.GetModuleExtensionValue(&model_analysis_pb.ModuleExtension_Key{
		Identifier: moduleExtensionIdentifier,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to obtain definition of module extension %#v: %w", moduleExtensionIdentifier, err)
	}
	moduleExtensionName := moduleExtension.CanonicalIdentifier.ToModuleExtension()

	ume, ok := h.usedModuleExtensions[moduleExtensionName]
	if ok {
		// Safety belt: prevent a single module instance from
		// declaring multiple module extensions with the same
		// name. This would cause collisions when both of them
		// declare repos with the same name.
		if moduleExtension.CanonicalIdentifier != ume.moduleExtension.CanonicalIdentifier {
			return nil, fmt.Errorf(
				"module extension declarations %#v and %#v have the same name, meaning they would both declare repos with prefix %#v",
				moduleExtension.CanonicalIdentifier.String(),
				ume.moduleExtension.CanonicalIdentifier.String(),
				moduleExtensionName.String(),
			)
		}
	} else {
		ume = &usedModuleExtension{
			moduleExtension: moduleExtension,
			users:           map[label.ModuleInstance]*moduleExtensionUser{},
		}
		h.usedModuleExtensions[moduleExtensionName] = ume
	}

	meu, ok := ume.users[h.moduleInstance]
	if !ok {
		meu = &moduleExtensionUser{}
		ume.users[h.moduleInstance] = meu
	}

	return &usedModuleExtensionProxy{
		user:          meu,
		devDependency: devDependency,
	}, nil
}

func (usedModuleExtensionExtractingModuleDotBazelHandler) UseRepoRule(repoRuleBzlFile label.ApparentLabel, repoRuleName string) (pg_starlark.RepoRuleProxy, error) {
	return func(name label.ApparentRepo, devDependency bool, attrs map[string]starlark.Value) error {
		return nil
	}, nil
}

func (c *baseComputer) ComputeUsedModuleExtensionsValue(ctx context.Context, key *model_analysis_pb.UsedModuleExtensions_Key, e UsedModuleExtensionsEnvironment) (PatchedUsedModuleExtensionsValue, error) {
	usedModuleExtensions := map[label.ModuleExtension]*usedModuleExtension{}
	if err := c.visitModuleDotBazelFilesBreadthFirst(ctx, e, func(moduleInstance label.ModuleInstance, ignoreDevDependencies bool) pg_starlark.ChildModuleDotBazelHandler {
		return &usedModuleExtensionExtractingModuleDotBazelHandler{
			environment:           e,
			moduleInstance:        moduleInstance,
			ignoreDevDependencies: ignoreDevDependencies,
			usedModuleExtensions:  usedModuleExtensions,
		}
	}); err != nil {
		if !errors.Is(err, evaluation.ErrMissingDependency) {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.UsedModuleExtensions_Value{
				Result: &model_analysis_pb.UsedModuleExtensions_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}
		return PatchedUsedModuleExtensionsValue{}, evaluation.ErrMissingDependency
	}
	panic("TODO")
}
