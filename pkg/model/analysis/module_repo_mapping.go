package analysis

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	pg_starlark "github.com/buildbarn/bb-playground/pkg/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"

	"go.starlark.net/starlark"
)

type repoMappingCapturingModuleDotBazelHandler struct {
	moduleInstance              label.ModuleInstance
	modulesWithMultipleVersions map[label.Module]OverrideVersions
	includeDevDependencies      bool

	repos map[label.ApparentRepo]*label.CanonicalRepo
}

func (h *repoMappingCapturingModuleDotBazelHandler) setRepo(from label.ApparentRepo, to *label.CanonicalRepo) error {
	if _, ok := h.repos[from]; ok {
		return fmt.Errorf("multiple repos declared with name %#v", from.String())
	}
	h.repos[from] = to
	return nil
}

func (h *repoMappingCapturingModuleDotBazelHandler) BazelDep(name label.Module, version *label.ModuleVersion, maxCompatibilityLevel int, repoName label.ApparentRepo, devDependency bool) error {
	if devDependency && !h.includeDevDependencies {
		return nil
	}
	versions := h.modulesWithMultipleVersions[name]
	var nearestVersion *label.ModuleVersion
	if len(versions) > 0 {
		v, err := versions.LookupNearestVersion(nearestVersion)
		if err != nil {
			return err
		}
		nearestVersion = &v
	}
	canonicalRepo := name.ToModuleInstance(nearestVersion).GetBareCanonicalRepo()
	return h.setRepo(repoName, &canonicalRepo)
}

func (h *repoMappingCapturingModuleDotBazelHandler) Module(name label.Module, version *label.ModuleVersion, compatibilityLevel int, repoName label.ApparentRepo, bazelCompatibility []string) error {
	canonicalRepo := h.moduleInstance.GetBareCanonicalRepo()
	return h.setRepo(repoName, &canonicalRepo)
}

func (repoMappingCapturingModuleDotBazelHandler) RegisterExecutionPlatforms(platformLabels []label.ApparentLabel, devDependency bool) error {
	return nil
}

func (repoMappingCapturingModuleDotBazelHandler) RegisterToolchains(toolchainLabels []label.ApparentLabel, devDependency bool) error {
	return nil
}

func (h *repoMappingCapturingModuleDotBazelHandler) UseExtension(extensionBzlFile label.ApparentLabel, extensionName string, devDependency, isolate bool) (pg_starlark.ModuleExtensionProxy, error) {
	if !devDependency || h.includeDevDependencies {
		return repoMappingCapturingModuleExtensionProxy{
			handler: h,
		}, nil
	}
	return pg_starlark.NullModuleExtensionProxy, nil
}

func (h *repoMappingCapturingModuleDotBazelHandler) UseRepoRule(repoRuleBzlFile label.ApparentLabel, repoRuleName string) (pg_starlark.RepoRuleProxy, error) {
	return func(name label.ApparentRepo, devDependency bool, attrs map[string]starlark.Value) error {
		if devDependency && !h.includeDevDependencies {
			return nil
		}
		canonicalRepo := h.moduleInstance.GetCanonicalRepoWithModuleExtension(label.MustNewStarlarkIdentifier("_repo_rules"), name)
		return h.setRepo(name, &canonicalRepo)
	}, nil
}

type repoMappingCapturingModuleExtensionProxy struct {
	handler *repoMappingCapturingModuleDotBazelHandler
}

func (repoMappingCapturingModuleExtensionProxy) Tag(className string, attrs map[string]starlark.Value) error {
	return nil
}

func (p repoMappingCapturingModuleExtensionProxy) UseRepo(repos map[label.ApparentRepo]label.ApparentRepo) error {
	for repo := range repos {
		if err := p.handler.setRepo(repo, nil); err != nil {
			return err
		}
	}
	return nil
}

func (c *baseComputer) ComputeModuleRepoMappingValue(ctx context.Context, key *model_analysis_pb.ModuleRepoMapping_Key, e ModuleRepoMappingEnvironment) (PatchedModuleRepoMappingValue, error) {
	moduleInstance, err := label.NewModuleInstance(key.ModuleInstance)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRepoMapping_Value{
			Result: &model_analysis_pb.ModuleRepoMapping_Value_Failure{
				Failure: fmt.Sprintf("Invalid module instance %#v: %s", key.ModuleInstance, err.Error()),
			},
		}), nil
	}

	fileReader, err := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if err != nil {
		if !errors.Is(err, evaluation.ErrMissingDependency) {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRepoMapping_Value{
				Result: &model_analysis_pb.ModuleRepoMapping_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}
		return PatchedModuleRepoMappingValue{}, err
	}

	modulesWithMultipleVersions, err := e.GetModulesWithMultipleVersionsObjectValue(&model_analysis_pb.ModulesWithMultipleVersionsObject_Key{})
	if err != nil {
		if !errors.Is(err, evaluation.ErrMissingDependency) {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRepoMapping_Value{
				Result: &model_analysis_pb.ModuleRepoMapping_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}
		return PatchedModuleRepoMappingValue{}, err
	}

	moduleFileLabel := moduleInstance.GetBareCanonicalRepo().
		GetRootPackage().
		AppendTargetName(moduleDotBazelTargetName)
	moduleFileProperties := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
		CanonicalRepo: moduleInstance.String(),
		Path:          moduleDotBazelFilename,
	})
	if !moduleFileProperties.IsSet() {
		return PatchedModuleRepoMappingValue{}, evaluation.ErrMissingDependency
	}
	var moduleFileContents model_core.Message[*model_filesystem_pb.FileContents]
	switch moduleFilePropertiesResult := moduleFileProperties.Message.Result.(type) {
	case *model_analysis_pb.FileProperties_Value_Exists:
		moduleFileContents = model_core.Message[*model_filesystem_pb.FileContents]{
			Message:            moduleFilePropertiesResult.Exists.Contents,
			OutgoingReferences: moduleFileProperties.OutgoingReferences,
		}
	case *model_analysis_pb.FileProperties_Value_DoesNotExist:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRepoMapping_Value{
			Result: &model_analysis_pb.ModuleRepoMapping_Value_Failure{
				Failure: fmt.Sprintf("File %#v does not exist", moduleFileLabel.String()),
			},
		}), nil
	case *model_analysis_pb.FileProperties_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRepoMapping_Value{
			Result: &model_analysis_pb.ModuleRepoMapping_Value_Failure{
				Failure: fmt.Sprintf("Failed to obtain properties of %#v: %s", moduleFileLabel.String(), moduleFilePropertiesResult.Failure),
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRepoMapping_Value{
			Result: &model_analysis_pb.ModuleRepoMapping_Value_Failure{
				Failure: "File properties value has an unknown result type",
			},
		}), nil
	}

	moduleFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
		moduleFileContents,
		c.buildSpecificationReference.GetReferenceFormat(),
	)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRepoMapping_Value{
			Result: &model_analysis_pb.ModuleRepoMapping_Value_Failure{
				Failure: fmt.Sprintf("Invalid file contents entry for file %#v: %s", moduleFileLabel.String(), err),
			},
		}), nil
	}

	moduleFileData, err := fileReader.FileReadAll(ctx, moduleFileContentsEntry, 1<<20)
	if err != nil {
		return PatchedModuleRepoMappingValue{}, err
	}

	handler := repoMappingCapturingModuleDotBazelHandler{
		moduleInstance:              moduleInstance,
		modulesWithMultipleVersions: modulesWithMultipleVersions,
		// TODO: Set this to the right value!
		includeDevDependencies: true,

		repos: map[label.ApparentRepo]*label.CanonicalRepo{},
	}
	if err := pg_starlark.ParseModuleDotBazel(
		string(moduleFileData),
		moduleFileLabel,
		nil,
		pg_starlark.NewOverrideIgnoringRootModuleDotBazelHandler(&handler),
	); err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRepoMapping_Value{
			Result: &model_analysis_pb.ModuleRepoMapping_Value_Failure{
				Failure: err.Error(),
			},
		}), nil
	}

	// Add "bazel_tools" implicitly.
	repos := handler.repos
	bazelTools := label.MustNewApparentRepo("bazel_tools")
	if _, ok := repos[bazelTools]; !ok {
		canonicalRepo := label.MustNewCanonicalRepo("bazel_tools+")
		repos[bazelTools] = &canonicalRepo
	}

	mappings := make([]*model_analysis_pb.ModuleRepoMapping_Value_Success_Mapping, 0, len(repos))
	for _, apparentRepo := range slices.SortedFunc(
		maps.Keys(repos),
		func(a, b label.ApparentRepo) int { return strings.Compare(a.String(), b.String()) },
	) {
		canonicalRepo := ""
		if r := repos[apparentRepo]; r != nil {
			canonicalRepo = r.String()
		}
		mappings = append(mappings, &model_analysis_pb.ModuleRepoMapping_Value_Success_Mapping{
			ApparentRepo:  apparentRepo.String(),
			CanonicalRepo: canonicalRepo,
		})
	}
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRepoMapping_Value{
		Result: &model_analysis_pb.ModuleRepoMapping_Value_Success_{
			Success: &model_analysis_pb.ModuleRepoMapping_Value_Success{
				Mappings: mappings,
			},
		},
	}), nil
}
