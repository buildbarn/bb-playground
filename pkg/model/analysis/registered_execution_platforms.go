package analysis

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"slices"

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

var platformInfoProviderIdentifier = label.MustNewCanonicalStarlarkIdentifier("@@builtins_core+//:exports.bzl%PlatformInfo")

type registeredExecutionPlatformExtractingModuleDotBazelHandler struct {
	computer              *baseComputer
	context               context.Context
	environment           RegisteredExecutionPlatformsEnvironment
	moduleInstance        label.ModuleInstance
	ignoreDevDependencies bool
	executionPlatforms    *[]*model_analysis_pb.ExecutionPlatform
}

func (registeredExecutionPlatformExtractingModuleDotBazelHandler) BazelDep(name label.Module, version *label.ModuleVersion, maxCompatibilityLevel int, repoName label.ApparentRepo, devDependency bool) error {
	return nil
}

func (registeredExecutionPlatformExtractingModuleDotBazelHandler) Module(name label.Module, version *label.ModuleVersion, compatibilityLevel int, repoName label.ApparentRepo, bazelCompatibility []string) error {
	return nil
}

func (h *registeredExecutionPlatformExtractingModuleDotBazelHandler) RegisterExecutionPlatforms(platformTargetPatterns []label.ApparentTargetPattern, devDependency bool) error {
	if !devDependency || !h.ignoreDevDependencies {
		for _, apparentPlatformTargetPattern := range platformTargetPatterns {
			canonicalPlatformTargetPattern, err := resolveApparent(h.environment, h.moduleInstance.GetBareCanonicalRepo(), apparentPlatformTargetPattern)
			if err != nil {
				return err
			}
			var iterErr error
			for canonicalPlatformLabel := range h.computer.expandCanonicalTargetPattern(h.context, h.environment, canonicalPlatformTargetPattern, &iterErr) {
				canonicalPlatformLabelStr := canonicalPlatformLabel.String()
				platformInfoProvider, err := getProviderFromConfiguredTarget(h.environment, canonicalPlatformLabelStr, platformInfoProviderIdentifier)
				if err != nil {
					return fmt.Errorf("failed to obtain PlatformInfo provider for target %#v: %w", canonicalPlatformLabelStr, err)
				}

				// Extract constraints and PKIX public key
				// fields from the PlatformInfo providers. These
				// are the only two fields that need to be
				// considered when using platforms for executing
				// actions.
				var constraints []*model_analysis_pb.Constraint
				var execPKIXPublicKey []byte
				for _, field := range platformInfoProvider.Message {
					switch field.Name {
					case "constraints":
						dict, ok := field.Value.GetKind().(*model_starlark_pb.Value_Dict)
						if !ok {
							return fmt.Errorf("constraints field of PlatformInfo of execution platform %#v is not a dict", canonicalPlatformLabelStr)
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
								return fmt.Errorf("key of constraints field of PlatformInfo of execution platform %#v is not a label", canonicalPlatformLabelStr)
							}
							value, ok := entry.Message.Value.GetKind().(*model_starlark_pb.Value_Label)
							if !ok {
								return fmt.Errorf("value of constraints field of PlatformInfo of execution platform %#v is not a label", canonicalPlatformLabelStr)
							}
							c[key.Label] = value.Label
						}
						if iterErr != nil {
							return fmt.Errorf("failed to iterate dict of constraints field of PlatformInfo of execution platform %#v: %w", canonicalPlatformLabelStr, iterErr)
						}
						constraints = make([]*model_analysis_pb.Constraint, 0, len(c))
						for _, setting := range slices.Sorted(maps.Keys(c)) {
							constraints = append(constraints, &model_analysis_pb.Constraint{
								Setting: setting,
								Value:   c[setting],
							})
						}
					case "exec_pkix_public_key":
						str, ok := field.Value.GetKind().(*model_starlark_pb.Value_Str)
						if !ok {
							return fmt.Errorf("exec_pkix_public_key field of PlatformInfo of execution platform %#v is not a string", canonicalPlatformLabelStr)
						}
						execPKIXPublicKey, err = base64.StdEncoding.DecodeString(str.Str)
						if err != nil {
							return fmt.Errorf("exec_pkix_public_key field of PlatformInfo of execution platform %#v: %w", canonicalPlatformLabelStr, err)
						}
					}
				}
				if constraints == nil {
					return fmt.Errorf("PlatformInfo of execution platform %#v does not contain field constraints", canonicalPlatformLabelStr)
				}
				if len(execPKIXPublicKey) == 0 {
					return fmt.Errorf("exec_pkix_public_key field of PlatformInfo of execution platform %#v is not set to a non-empty string", canonicalPlatformLabelStr)
				}

				*h.executionPlatforms = append(*h.executionPlatforms, &model_analysis_pb.ExecutionPlatform{
					Constraints:       constraints,
					ExecPkixPublicKey: execPKIXPublicKey,
				})
			}
			if iterErr != nil {
				return fmt.Errorf("failed to expand target pattern %#v: %w", canonicalPlatformTargetPattern.String(), iterErr)
			}
			return nil
		}
	}
	return nil
}

func (registeredExecutionPlatformExtractingModuleDotBazelHandler) RegisterToolchains(toolchainTargetPatterns []label.ApparentTargetPattern, devDependency bool) error {
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
	var executionPlatforms []*model_analysis_pb.ExecutionPlatform
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
