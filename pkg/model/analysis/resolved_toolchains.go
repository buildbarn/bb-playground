package analysis

import (
	"context"
	"fmt"
	"strings"

	"github.com/buildbarn/bonanza/pkg/evaluation"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_core_pb "github.com/buildbarn/bonanza/pkg/proto/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
)

func constraintsAreCompatible(actual, expected []*model_analysis_pb.Constraint) bool {
	for len(actual) > 0 && len(expected) > 0 {
		if cmp := strings.Compare(actual[0].Setting, expected[0].Setting); cmp < 0 {
			// Object has an additional constraint that we
			// don't care about. Ignore it.
			actual = actual[1:]
		} else if cmp > 0 {
			if expected[0].Value != "" {
				// Object lacks a constraint that we
				// care about.
				return false
			}
			// Object lacks a constraint for which it is
			// required that its value is equal to the
			// default value. This is acceptable.
			expected = expected[1:]
		} else {
			if expected[0].Value != actual[0].Value {
				// Object has a constraint whose value
				// differs from what is expected.
				return false
			}
			actual = actual[1:]
			expected = expected[1:]
		}
	}
	for _, c := range expected {
		if c.Value != "" {
			// Object lacks a constraint whose value has to
			// differ from the constraint's default value.
			return false
		}
	}
	return true
}

func (c *baseComputer) ComputeResolvedToolchainsValue(ctx context.Context, key model_core.Message[*model_analysis_pb.ResolvedToolchains_Key], e ResolvedToolchainsEnvironment) (PatchedResolvedToolchainsValue, error) {
	// Obtain all compatible execution platforms and toolchains.
	missingDependencies := false
	compatibleExecutionPlatforms := e.GetCompatibleExecutionPlatformsValue(&model_analysis_pb.CompatibleExecutionPlatforms_Key{
		Constraints: key.Message.ExecCompatibleWith,
	})
	if !compatibleExecutionPlatforms.IsSet() {
		missingDependencies = true
	}
	compatibleToolchainsByType := make([][]*model_analysis_pb.RegisteredToolchain, 0, len(key.Message.Toolchains))
	for _, toolchain := range key.Message.Toolchains {
		configurationReference := model_core.NewPatchedMessageFromExisting(
			model_core.Message[*model_core_pb.Reference]{
				Message:            key.Message.ConfigurationReference,
				OutgoingReferences: key.OutgoingReferences,
			},
			func(index int) dag.ObjectContentsWalker {
				return dag.ExistingObjectContentsWalker
			},
		)
		compatibleToolchainsForTypeValue := e.GetCompatibleToolchainsForTypeValue(
			model_core.PatchedMessage[*model_analysis_pb.CompatibleToolchainsForType_Key, dag.ObjectContentsWalker]{
				Message: &model_analysis_pb.CompatibleToolchainsForType_Key{
					ToolchainType:          toolchain.ToolchainType,
					ConfigurationReference: configurationReference.Message,
				},
				Patcher: configurationReference.Patcher,
			},
		)
		if !compatibleToolchainsForTypeValue.IsSet() {
			missingDependencies = true
			continue
		}
		compatibleToolchainsForType := compatibleToolchainsForTypeValue.Message.Toolchains
		if len(compatibleToolchainsForType) == 0 && toolchain.Mandatory {
			return PatchedResolvedToolchainsValue{}, fmt.Errorf("dependency on toolchain type %#v is mandatory, but no toolchains exist that are compatible with the target", toolchain.ToolchainType)
		}
		compatibleToolchainsByType = append(compatibleToolchainsByType, compatibleToolchainsForType)
	}
	if missingDependencies {
		return PatchedResolvedToolchainsValue{}, evaluation.ErrMissingDependency
	}

	// Pick the first execution platform having at least one
	// matching toolchain for all mandatory toolchain types.
	executionPlatforms := compatibleExecutionPlatforms.Message.ExecutionPlatforms
	toolchainTypeHasAtLeastOneMatchingExecutionPlatform := make([]bool, len(compatibleToolchainsByType))
CheckExecutionPlatform:
	for _, executionPlatform := range executionPlatforms {
		resolvedToolchains := make([]*model_analysis_pb.RegisteredToolchain, 0, len(compatibleToolchainsByType))
	CheckToolchainType:
		for i, toolchainsForType := range compatibleToolchainsByType {
			for _, toolchain := range toolchainsForType {
				if constraintsAreCompatible(executionPlatform.Constraints, toolchain.ExecCompatibleWith) {
					toolchainTypeHasAtLeastOneMatchingExecutionPlatform[i] = true
					resolvedToolchains = append(resolvedToolchains, toolchain)
					continue CheckToolchainType
				}
			}

			// Did not find any compatible toolchain.
			if key.Message.Toolchains[i].Mandatory {
				continue CheckExecutionPlatform
			}
			toolchainTypeHasAtLeastOneMatchingExecutionPlatform[i] = true
			resolvedToolchains = append(resolvedToolchains, nil)
		}

		// Found an execution platform for which all mandatory
		// toolchain types have a compatible toolchain.
		//
		// The label stored in DeclaredToolchainInfo.toolchain
		// does not have aliases expanded, as that would in many
		// cases cause all toolchains to be downloaded,
		// regardless of their use. This means that alias
		// expansion needs to happen here.
		toolchainIdentifiers := make([]string, 0, len(resolvedToolchains))
		missingDependencies := false
		for _, resolvedToolchain := range resolvedToolchains {
			if resolvedToolchain == nil {
				// Optional toolchain that is missing.
				toolchainIdentifiers = append(toolchainIdentifiers, "")
			} else {
				visibleTarget := e.GetVisibleTargetValue(
					model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](
						&model_analysis_pb.VisibleTarget_Key{
							FromPackage: resolvedToolchain.Package,
							ToLabel:     resolvedToolchain.Toolchain,
							// Don't specify a configuration, as
							// the toolchain() itself is also
							// evaluated without one.
						},
					),
				)
				if !visibleTarget.IsSet() {
					missingDependencies = true
					continue
				}
				if !missingDependencies {
					toolchainIdentifiers = append(toolchainIdentifiers, visibleTarget.Message.Label)
				}
			}
		}
		if missingDependencies {
			return PatchedResolvedToolchainsValue{}, evaluation.ErrMissingDependency
		}
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ResolvedToolchains_Value{
			ToolchainIdentifiers: toolchainIdentifiers,
		}), nil
	}

	for i, hasMatching := range toolchainTypeHasAtLeastOneMatchingExecutionPlatform {
		if !hasMatching {
			return PatchedResolvedToolchainsValue{}, fmt.Errorf("dependency on toolchain type %#v is mandatory, but none of the %d toolchains that are compatible with the target are also compatible with any of the %d execution platforms", key.Message.Toolchains[i].ToolchainType, len(compatibleToolchainsByType[i]), len(executionPlatforms))
		}
	}
	return PatchedResolvedToolchainsValue{}, fmt.Errorf("even though all mandatory toolchain types have at least one toolchain that is compatible with one of the %d execution platforms, no single execution platform exists for which all mandatory toolchain types have at least one compatible toolchain", len(executionPlatforms))
}
