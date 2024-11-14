package analysis

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	pg_starlark "github.com/buildbarn/bb-playground/pkg/starlark"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type getPlatformInfoProviderEnvironment interface {
	GetConfiguredTargetValue(key *model_analysis_pb.ConfiguredTarget_Key) model_core.Message[*model_analysis_pb.ConfiguredTarget_Value]
}

// getPlatformInfoProvider looks up the PlatformInfo provider that is
// returned by a platform() target.
func getPlatformInfoProvider(e getPlatformInfoProviderEnvironment, platformLabel label.CanonicalLabel) (model_core.Message[*model_starlark_pb.Struct], error) {
	configuredTargetValue := e.GetConfiguredTargetValue(&model_analysis_pb.ConfiguredTarget_Key{
		Label: platformLabel.String(),
	})
	if !configuredTargetValue.IsSet() {
		return model_core.Message[*model_starlark_pb.Struct]{}, evaluation.ErrMissingDependency
	}

	const platformInfoProviderIdentifier = "@@builtins_core+//:exports.bzl%PlatformInfo"
	providerInstances := configuredTargetValue.Message.ProviderInstances
	providerIndex := sort.Search(
		len(providerInstances),
		func(i int) bool { return providerInstances[i].ProviderIdentifier >= platformInfoProviderIdentifier },
	)
	if providerIndex >= len(providerInstances) || providerInstances[providerIndex].ProviderIdentifier != platformInfoProviderIdentifier {
		return model_core.Message[*model_starlark_pb.Struct]{}, fmt.Errorf("target did not yield provider %#v", platformInfoProviderIdentifier)
	}
	return model_core.Message[*model_starlark_pb.Struct]{
		Message:            providerInstances[providerIndex],
		OutgoingReferences: configuredTargetValue.OutgoingReferences,
	}, nil
}

type registeredExecutionPlatformExtractingModuleDotBazelHandler struct {
	environment           RegisteredExecutionPlatformsEnvironment
	moduleInstance        label.ModuleInstance
	ignoreDevDependencies bool
	valueDecodingOptions  *model_starlark.ValueDecodingOptions
	executionPlatforms    *[]*model_analysis_pb.RegisteredExecutionPlatforms_Value_ExecutionPlatform
}

func (registeredExecutionPlatformExtractingModuleDotBazelHandler) BazelDep(name label.Module, version *label.ModuleVersion, maxCompatibilityLevel int, repoName label.ApparentRepo, devDependency bool) error {
	return nil
}

func (registeredExecutionPlatformExtractingModuleDotBazelHandler) Module(name label.Module, version *label.ModuleVersion, compatibilityLevel int, repoName label.ApparentRepo, bazelCompatibility []string) error {
	return nil
}

func (h *registeredExecutionPlatformExtractingModuleDotBazelHandler) RegisterExecutionPlatforms(platformLabels []label.ApparentLabel, devDependency bool) error {
	if !devDependency || !h.ignoreDevDependencies {
		for _, apparentPlatformLabel := range platformLabels {
			canonicalPlatformLabel, err := resolveApparentLabel(h.environment, h.moduleInstance.GetBareCanonicalRepo(), apparentPlatformLabel)
			if err != nil {
				return err
			}
			platformInfoProvider, err := getPlatformInfoProvider(h.environment, canonicalPlatformLabel)
			if err != nil {
				return fmt.Errorf("failed to obtain PlatformInfo provider for target %#v: %w", canonicalPlatformLabel.String(), err)
			}

			// Decode it.
			// TODO: Would there be a way for us to directly
			// extract the values without decoding to
			// Starlark types first?
			strukt, err := model_starlark.DecodeStruct(platformInfoProvider, h.valueDecodingOptions)
			if err != nil {
				return fmt.Errorf("failed to decode PlatformInfo provider of target %#v: %w", canonicalPlatformLabel.String(), err)
			}
			structFields := starlark.StringDict{}
			strukt.ToStringDict(structFields)

			// Extract constraints and exec_properties fields.
			constraintsValue, ok := structFields["constraints"]
			if !ok {
				return fmt.Errorf("PlatformInfo provider of target %#v does not contain field \"constraints\"", canonicalPlatformLabel.String())
			}
			var constraints map[label.CanonicalLabel]label.CanonicalLabel
			if err := unpack.Dict(model_starlark.LabelUnpackerInto, model_starlark.LabelUnpackerInto).UnpackInto(nil, constraintsValue, &constraints); err != nil {
				return fmt.Errorf("invalid value for field \"constraints\" of PlatformInfo provider of target %#v", canonicalPlatformLabel.String())
			}

			execPropertiesValue, ok := structFields["exec_properties"]
			if !ok {
				return fmt.Errorf("PlatformInfo provider of target %#v does not contain field \"exec_properties\"", canonicalPlatformLabel.String())
			}
			var execProperties map[string]string
			if err := unpack.Dict(unpack.String, unpack.String).UnpackInto(nil, execPropertiesValue, &execProperties); err != nil {
				return fmt.Errorf("invalid value for field \"exec_properties\" of PlatformInfo provider of target %#v", canonicalPlatformLabel.String())
			}

			executionPlatform := &model_analysis_pb.RegisteredExecutionPlatforms_Value_ExecutionPlatform{
				Constraints:    make([]*model_analysis_pb.RegisteredExecutionPlatforms_Value_ExecutionPlatform_Constraint, 0, len(constraints)),
				ExecProperties: make([]*model_analysis_pb.ExecProperty, 0, len(execProperties)),
			}
			for _, setting := range slices.SortedFunc(
				maps.Keys(constraints),
				func(a, b label.CanonicalLabel) int { return strings.Compare(a.String(), b.String()) },
			) {
				executionPlatform.Constraints = append(executionPlatform.Constraints, &model_analysis_pb.RegisteredExecutionPlatforms_Value_ExecutionPlatform_Constraint{
					Setting: setting.String(),
					Value:   constraints[setting].String(),
				})
			}
			for _, name := range slices.Sorted(maps.Keys(execProperties)) {
				executionPlatform.ExecProperties = append(executionPlatform.ExecProperties, &model_analysis_pb.ExecProperty{
					Name:  name,
					Value: execProperties[name],
				})
			}
			*h.executionPlatforms = append(*h.executionPlatforms, executionPlatform)
			return nil
		}
	}
	return nil
}

func (registeredExecutionPlatformExtractingModuleDotBazelHandler) RegisterToolchains(toolchainLabels []label.ApparentLabel, devDependency bool) error {
	return nil
}

func (registeredExecutionPlatformExtractingModuleDotBazelHandler) UseExtension(extensionBzlFile label.ApparentLabel, extensionName label.StarlarkIdentifier, devDependency, isolate bool) (pg_starlark.ModuleExtensionProxy, error) {
	return pg_starlark.NullModuleExtensionProxy, nil
}

func (registeredExecutionPlatformExtractingModuleDotBazelHandler) UseRepoRule(repoRuleBzlFile label.ApparentLabel, repoRuleName string) (pg_starlark.RepoRuleProxy, error) {
	return func(name label.ApparentRepo, devDependency bool, attrs map[string]starlark.Value) error {
		return nil
	}, nil
}

func (c *baseComputer) ComputeRegisteredExecutionPlatformsValue(ctx context.Context, key *model_analysis_pb.RegisteredExecutionPlatforms_Key, e RegisteredExecutionPlatformsEnvironment) (PatchedRegisteredExecutionPlatformsValue, error) {
	var executionPlatforms []*model_analysis_pb.RegisteredExecutionPlatforms_Value_ExecutionPlatform
	if err := c.visitModuleDotBazelFilesBreadthFirst(ctx, e, func(moduleInstance label.ModuleInstance, ignoreDevDependencies bool) pg_starlark.ChildModuleDotBazelHandler {
		return &registeredExecutionPlatformExtractingModuleDotBazelHandler{
			environment:           e,
			moduleInstance:        moduleInstance,
			ignoreDevDependencies: ignoreDevDependencies,
			valueDecodingOptions: c.getValueDecodingOptions(ctx, func(canonicalLabel label.CanonicalLabel) (starlark.Value, error) {
				return model_starlark.NewLabel(canonicalLabel), nil
			}),
			executionPlatforms: &executionPlatforms,
		}
	}); err != nil {
		return PatchedRegisteredExecutionPlatformsValue{}, err
	}

	if len(executionPlatforms) == 0 {
		return PatchedRegisteredExecutionPlatformsValue{}, errors.New("cailed to find registered execution platforms in any of the MODULE.bazel files")
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredExecutionPlatforms_Value{
		ExecutionPlatforms: executionPlatforms,
	}), nil
}
