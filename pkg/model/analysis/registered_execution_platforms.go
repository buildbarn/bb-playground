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

type registeredExecutionPlatformExtractingModuleDotBazelHandler struct {
	environment          RegisteredExecutionPlatformsEnvironment
	moduleInstance       label.ModuleInstance
	valueDecodingOptions *model_starlark.ValueDecodingOptions
	executionPlatforms   *[]*model_analysis_pb.RegisteredExecutionPlatforms_Value_Success_ExecutionPlatform
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
		switch result := configuredTargetValue.Message.Result.(type) {
		case *model_analysis_pb.ConfiguredTarget_Value_Success_:
			// Look up the PlatformInfo provider.
			const platformInfoProviderIdentifier = "@@builtins_core+//:exports.bzl%PlatformInfo"
			providerInstances := result.Success.ProviderInstances
			providerIndex := sort.Search(
				len(providerInstances),
				func(i int) bool { return providerInstances[i].ProviderIdentifier >= platformInfoProviderIdentifier },
			)
			if providerIndex >= len(providerInstances) || providerInstances[providerIndex].ProviderIdentifier != platformInfoProviderIdentifier {
				return fmt.Errorf("target %#v did not yield provider %#v", canonicalPlatformLabel.String(), platformInfoProviderIdentifier)
			}

			// Decode it.
			// TODO: Would there be a way for us to directly
			// extract the values without decoding to
			// Starlark types first?
			strukt, err := model_starlark.DecodeStruct(
				model_core.Message[*model_starlark_pb.Struct]{
					Message:            providerInstances[providerIndex],
					OutgoingReferences: configuredTargetValue.OutgoingReferences,
				},
				h.valueDecodingOptions)
			if err != nil {
				return fmt.Errorf("failed to decode provider %#v of target %#v: %w", platformInfoProviderIdentifier, canonicalPlatformLabel.String(), err)
			}
			structFields := starlark.StringDict{}
			strukt.ToStringDict(structFields)

			// Extract constraints and exec_properties fields.
			constraintsValue, ok := structFields["constraints"]
			if !ok {
				return fmt.Errorf("provider %#v of target %#v does not contain field \"constraints\"", platformInfoProviderIdentifier, canonicalPlatformLabel.String())
			}
			var constraints map[label.CanonicalLabel]label.CanonicalLabel
			if err := unpack.Dict(model_starlark.LabelUnpackerInto, model_starlark.LabelUnpackerInto).UnpackInto(nil, constraintsValue, &constraints); err != nil {
				return fmt.Errorf("invalid value for field \"constraints\" of provider %#v of target %#v", platformInfoProviderIdentifier, canonicalPlatformLabel.String())
			}

			execPropertiesValue, ok := structFields["exec_properties"]
			if !ok {
				return fmt.Errorf("provider %#v of target %#v does not contain field \"exec_properties\"", platformInfoProviderIdentifier, canonicalPlatformLabel.String())
			}
			var execProperties map[string]string
			if err := unpack.Dict(unpack.String, unpack.String).UnpackInto(nil, execPropertiesValue, &execProperties); err != nil {
				return fmt.Errorf("invalid value for field \"exec_properties\" of provider %#v of target %#v", platformInfoProviderIdentifier, canonicalPlatformLabel.String())
			}

			executionPlatform := &model_analysis_pb.RegisteredExecutionPlatforms_Value_Success_ExecutionPlatform{
				Constraints:    make([]*model_analysis_pb.RegisteredExecutionPlatforms_Value_Success_ExecutionPlatform_Constraint, 0, len(constraints)),
				ExecProperties: make([]*model_analysis_pb.RegisteredExecutionPlatforms_Value_Success_ExecutionPlatform_ExecProperty, 0, len(execProperties)),
			}
			for _, setting := range slices.SortedFunc(
				maps.Keys(constraints),
				func(a, b label.CanonicalLabel) int { return strings.Compare(a.String(), b.String()) },
			) {
				executionPlatform.Constraints = append(executionPlatform.Constraints, &model_analysis_pb.RegisteredExecutionPlatforms_Value_Success_ExecutionPlatform_Constraint{
					Setting: setting.String(),
					Value:   constraints[setting].String(),
				})
			}
			for _, name := range slices.Sorted(maps.Keys(execProperties)) {
				executionPlatform.ExecProperties = append(executionPlatform.ExecProperties, &model_analysis_pb.RegisteredExecutionPlatforms_Value_Success_ExecutionPlatform_ExecProperty{
					Name:  name,
					Value: execProperties[name],
				})
			}
			*h.executionPlatforms = append(*h.executionPlatforms, executionPlatform)
			return nil
		case *model_analysis_pb.ConfiguredTarget_Value_Failure:
			return errors.New(result.Failure)
		default:
			return errors.New("configured target value has an unknown result type")
		}
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

	var executionPlatforms []*model_analysis_pb.RegisteredExecutionPlatforms_Value_Success_ExecutionPlatform
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
			valueDecodingOptions: c.getValueDecodingOptions(ctx, func(canonicalLabel label.CanonicalLabel) (starlark.Value, error) {
				return model_starlark.NewLabel(canonicalLabel), nil
			}),
			executionPlatforms: &executionPlatforms,
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

	if len(executionPlatforms) == 0 {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredExecutionPlatforms_Value{
			Result: &model_analysis_pb.RegisteredExecutionPlatforms_Value_Failure{
				Failure: "Failed to find registered execution platforms in any of the MODULE.bazel files",
			},
		}), nil
	}

	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.RegisteredExecutionPlatforms_Value{
		Result: &model_analysis_pb.RegisteredExecutionPlatforms_Value_Success_{
			Success: &model_analysis_pb.RegisteredExecutionPlatforms_Value_Success{
				ExecutionPlatforms: executionPlatforms,
			},
		},
	}), nil
}
