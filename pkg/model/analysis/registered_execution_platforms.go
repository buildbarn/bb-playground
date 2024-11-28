package analysis

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_starlark "github.com/buildbarn/bb-playground/pkg/model/starlark"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_starlark_pb "github.com/buildbarn/bb-playground/pkg/proto/model/starlark"
	pg_starlark "github.com/buildbarn/bb-playground/pkg/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"

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
	computer              *baseComputer
	context               context.Context
	environment           RegisteredExecutionPlatformsEnvironment
	moduleInstance        label.ModuleInstance
	ignoreDevDependencies bool
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

			// Extract constraints and PKIX public key
			// fields from the PlatformInfo providers. These
			// are the only two fields that need to be
			// considered when using platforms for executing
			// actions.
			var constraints []*model_analysis_pb.RegisteredExecutionPlatforms_Value_ExecutionPlatform_Constraint
			var execPKIXPublicKey []byte
			for _, field := range platformInfoProvider.Message.Fields {
				switch field.Name {
				case "constraints":
					dict, ok := field.Value.GetKind().(*model_starlark_pb.Value_Dict)
					if !ok {
						return fmt.Errorf("constraints field of PlatformInfo of execution platform %#v is not a dict", canonicalPlatformLabel.String())
					}
					var iterErr error
					c := map[string]string{}
					for entry := range model_starlark.AllDictLeafEntries(
						h.context,
						model_parser.NewStorageBackedParsedObjectReader(
							h.computer.objectDownloader,
							h.computer.getValueObjectEncoder(),
							model_parser.NewMessageObjectParser[object.LocalReference, model_starlark_pb.Dict](),
						),
						model_core.Message[*model_starlark_pb.Dict]{
							Message:            dict.Dict,
							OutgoingReferences: platformInfoProvider.OutgoingReferences,
						},
						&iterErr,
					) {
						key, ok := entry.Message.Key.GetKind().(*model_starlark_pb.Value_Label)
						if !ok {
							return fmt.Errorf("key of constraints field of PlatformInfo of execution platform %#v is not a label", canonicalPlatformLabel.String())
						}
						value, ok := entry.Message.Value.GetKind().(*model_starlark_pb.Value_Label)
						if !ok {
							return fmt.Errorf("value of constraints field of PlatformInfo of execution platform %#v is not a label", canonicalPlatformLabel.String())
						}
						c[key.Label] = value.Label
					}
					if iterErr != nil {
						return fmt.Errorf("failed to iterate dict of constraints field of PlatformInfo of execution platform %#v: %w", canonicalPlatformLabel.String(), iterErr)
					}
					constraints = make([]*model_analysis_pb.RegisteredExecutionPlatforms_Value_ExecutionPlatform_Constraint, 0, len(c))
					for _, setting := range slices.Sorted(maps.Keys(c)) {
						constraints = append(constraints, &model_analysis_pb.RegisteredExecutionPlatforms_Value_ExecutionPlatform_Constraint{
							Setting: setting,
							Value:   c[setting],
						})
					}
				case "exec_pkix_public_key":
					str, ok := field.Value.GetKind().(*model_starlark_pb.Value_Str)
					if !ok {
						return fmt.Errorf("exec_pkix_public_key field of PlatformInfo of execution platform %#v is not a string", canonicalPlatformLabel.String())
					}
					execPKIXPublicKey, err = base64.StdEncoding.DecodeString(str.Str)
					if err != nil {
						return fmt.Errorf("exec_pkix_public_key field of PlatformInfo of execution platform %#v: %w", canonicalPlatformLabel.String(), err)
					}
				}
			}
			if constraints == nil {
				return fmt.Errorf("PlatformInfo of execution platform %#v does not contain field constraints", canonicalPlatformLabel.String())
			}
			if len(execPKIXPublicKey) == 0 {
				return fmt.Errorf("exec_pkix_public_key field of PlatformInfo of execution platform %#v is not set to a non-empty string", canonicalPlatformLabel.String())
			}

			*h.executionPlatforms = append(*h.executionPlatforms, &model_analysis_pb.RegisteredExecutionPlatforms_Value_ExecutionPlatform{
				Constraints:       constraints,
				ExecPkixPublicKey: execPKIXPublicKey,
			})
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
			computer:              c,
			context:               ctx,
			environment:           e,
			moduleInstance:        moduleInstance,
			ignoreDevDependencies: ignoreDevDependencies,
			executionPlatforms:    &executionPlatforms,
		}
	}); err != nil {
		return PatchedRegisteredExecutionPlatformsValue{}, err
	}

	if len(executionPlatforms) == 0 {
		return PatchedRegisteredExecutionPlatformsValue{}, errors.New("failed to find registered execution platforms in any of the MODULE.bazel files")
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredExecutionPlatforms_Value{
		ExecutionPlatforms: executionPlatforms,
	}), nil
}
