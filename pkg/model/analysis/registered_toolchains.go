package analysis

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	pg_starlark "github.com/buildbarn/bb-playground/pkg/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

var declaredToolchainInfoProviderIdentifier = label.MustNewCanonicalStarlarkIdentifier("@@builtins_core+//:exports.bzl%DeclaredToolchainInfo")

const toolchainRuleIdentifier = "@@builtins_core+//:exports.bzl%toolchain"

type registeredToolchainExtractingModuleDotBazelHandler struct {
	context                    context.Context
	computer                   *baseComputer
	environment                RegisteredToolchainsEnvironment
	moduleInstance             label.ModuleInstance
	ignoreDevDependencies      bool
	registeredToolchainsByType map[string][]*model_analysis_pb.RegisteredToolchain
}

func (registeredToolchainExtractingModuleDotBazelHandler) BazelDep(name label.Module, version *label.ModuleVersion, maxCompatibilityLevel int, repoName label.ApparentRepo, devDependency bool) error {
	return nil
}

func (registeredToolchainExtractingModuleDotBazelHandler) Module(name label.Module, version *label.ModuleVersion, compatibilityLevel int, repoName label.ApparentRepo, bazelCompatibility []string) error {
	return nil
}

func (registeredToolchainExtractingModuleDotBazelHandler) RegisterExecutionPlatforms(platformTargetPatterns []label.ApparentTargetPattern, devDependency bool) error {
	return nil
}

func (h *registeredToolchainExtractingModuleDotBazelHandler) RegisterToolchains(toolchainTargetPatterns []label.ApparentTargetPattern, devDependency bool) error {
	if !devDependency || !h.ignoreDevDependencies {
		missingDependencies := false
		for _, apparentToolchainTargetPattern := range toolchainTargetPatterns {
			canonicalToolchainTargetPattern, err := resolveApparent(h.environment, h.moduleInstance.GetBareCanonicalRepo(), apparentToolchainTargetPattern)
			if err != nil {
				if errors.Is(err, evaluation.ErrMissingDependency) {
					missingDependencies = true
					continue
				}
				return err
			}
			var iterErr error
			for canonicalToolchainLabel := range h.computer.expandCanonicalTargetPattern(h.context, h.environment, canonicalToolchainTargetPattern, &iterErr) {
				visibleTargetValue := h.environment.GetVisibleTargetValue(&model_analysis_pb.VisibleTarget_Key{
					FromPackage:        canonicalToolchainLabel.GetCanonicalPackage().String(),
					ToLabel:            canonicalToolchainLabel.String(),
					PermitAliasNoMatch: true,
				})
				if !visibleTargetValue.IsSet() {
					missingDependencies = true
					continue
				}

				toolchainLabelStr := visibleTargetValue.Message.Label
				if toolchainLabelStr == "" {
					// Target is an alias() that does not
					// have a default condition. Ignore.
					continue
				}
				toolchainLabel, err := label.NewCanonicalLabel(toolchainLabelStr)
				if err != nil {
					return fmt.Errorf("invalid toolchain label %#v: %w", toolchainLabelStr, err)
				}

				targetValue := h.environment.GetTargetValue(&model_analysis_pb.Target_Key{
					Label: toolchainLabelStr,
				})
				if !targetValue.IsSet() {
					missingDependencies = true
					continue
				}
				ruleTarget, ok := targetValue.Message.Definition.GetKind().(*model_starlark_pb.Target_Definition_RuleTarget)
				if !ok {
					return fmt.Errorf("toolchain %#v is not a rule target", toolchainLabelStr)
				}
				if ruleTarget.RuleTarget.RuleIdentifier != toolchainRuleIdentifier {
					// Non-toolchain target.
					continue
				}

				declaredToolchainInfoProvider, err := getProviderFromConfiguredTarget(
					h.environment,
					toolchainLabelStr,
					declaredToolchainInfoProviderIdentifier,
				)
				if err != nil {
					if errors.Is(err, evaluation.ErrMissingDependency) {
						missingDependencies = true
						continue
					}
					return fmt.Errorf("toolchain %#v: %w", toolchainLabelStr, err)
				}

				var toolchain, toolchainType *string
				for _, field := range declaredToolchainInfoProvider.Message {
					switch field.Name {
					case "target_settings":
						_, ok := field.Value.GetKind().(*model_starlark_pb.Value_List)
						if !ok {
							return fmt.Errorf("target_settings field of DeclaredToolchainInfo of toolchain %#v is not a list", toolchainLabelStr)
						}
					case "toolchain":
						l, ok := field.Value.GetKind().(*model_starlark_pb.Value_Label)
						if !ok {
							return fmt.Errorf("toolchain field of DeclaredToolchainInfo of toolchain %#v is not a label", toolchainLabelStr)
						}
						toolchain = &l.Label
					case "toolchain_type":
						l, ok := field.Value.GetKind().(*model_starlark_pb.Value_Label)
						if !ok {
							return fmt.Errorf("toolchain_type field of DeclaredToolchainInfo of toolchain %#v is not a label", toolchainLabelStr)
						}
						toolchainType = &l.Label
					}
				}
				if toolchain == nil {
					return fmt.Errorf("DeclaredToolchainInfo of toolchain %#v does not contain field toolchain", toolchainLabelStr)
				}
				if toolchainType == nil {
					return fmt.Errorf("DeclaredToolchainInfo of toolchain %#v does not contain field toolchain_type", toolchainLabelStr)
				}

				toolchainPackage := toolchainLabel.GetCanonicalPackage()
				execCompatibleWith, err := h.computer.constraintValuesToConstraints(
					h.environment,
					toolchainPackage,
					ruleTarget.RuleTarget.ExecCompatibleWith,
				)
				if err != nil {
					if !errors.Is(err, evaluation.ErrMissingDependency) {
						return err
					}
					missingDependencies = true
				}
				targetCompatibleWith, err := h.computer.constraintValuesToConstraints(
					h.environment,
					toolchainPackage,
					ruleTarget.RuleTarget.TargetCompatibleWith,
				)
				if err != nil {
					if !errors.Is(err, evaluation.ErrMissingDependency) {
						return err
					}
					missingDependencies = true
				}

				if !missingDependencies {
					h.registeredToolchainsByType[*toolchainType] = append(
						h.registeredToolchainsByType[*toolchainType],
						&model_analysis_pb.RegisteredToolchain{
							ExecCompatibleWith:   execCompatibleWith,
							TargetCompatibleWith: targetCompatibleWith,
							// TODO: Set TargetSettings!
							Toolchain: *toolchain,
							Package:   toolchainLabel.GetCanonicalPackage().String(),
						},
					)
				}
			}
			if iterErr != nil {
				if !errors.Is(err, evaluation.ErrMissingDependency) {
					return fmt.Errorf("failed to expand target pattern %#v: %w", canonicalToolchainTargetPattern.String(), iterErr)
				}
				missingDependencies = true
			}
		}

		if missingDependencies {
			return evaluation.ErrMissingDependency
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
	registeredToolchainsByType := map[string][]*model_analysis_pb.RegisteredToolchain{}
	if err := c.visitModuleDotBazelFilesBreadthFirst(ctx, e, func(moduleInstance label.ModuleInstance, ignoreDevDependencies bool) pg_starlark.ChildModuleDotBazelHandler {
		return &registeredToolchainExtractingModuleDotBazelHandler{
			context:                    ctx,
			computer:                   c,
			environment:                e,
			moduleInstance:             moduleInstance,
			ignoreDevDependencies:      ignoreDevDependencies,
			registeredToolchainsByType: registeredToolchainsByType,
		}
	}); err != nil {
		return PatchedRegisteredToolchainsValue{}, err
	}

	toolchainTypes := make([]*model_analysis_pb.RegisteredToolchains_Value_RegisteredToolchainType, 0, len(registeredToolchainsByType))
	for _, toolchainType := range slices.Sorted(maps.Keys(registeredToolchainsByType)) {
		toolchainTypes = append(toolchainTypes, &model_analysis_pb.RegisteredToolchains_Value_RegisteredToolchainType{
			ToolchainType: toolchainType,
			Toolchains:    registeredToolchainsByType[toolchainType],
		})
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredToolchains_Value{
		ToolchainTypes: toolchainTypes,
	}), nil
}
