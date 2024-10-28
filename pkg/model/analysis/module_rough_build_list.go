package analysis

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"

	"github.com/buildbarn/bb-playground/pkg/ds"
	"github.com/buildbarn/bb-playground/pkg/evaluation"
	"github.com/buildbarn/bb-playground/pkg/label"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_filesystem_pb "github.com/buildbarn/bb-playground/pkg/proto/model/filesystem"
	pg_starlark "github.com/buildbarn/bb-playground/pkg/starlark"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-storage/pkg/util"

	"go.starlark.net/starlark"
)

const moduleDotBazelFilename = "MODULE.bazel"

var moduleDotBazelTargetName = label.MustNewTargetName(moduleDotBazelFilename)

type bazelDepCapturingModuleDotBazelHandler struct {
	includeDevDependencies bool

	compatibilityLevel int
	dependencies       map[label.Module]*label.ModuleVersion
}

func (h *bazelDepCapturingModuleDotBazelHandler) BazelDep(name label.Module, version *label.ModuleVersion, maxCompatibilityLevel int, repoName label.ApparentRepo, devDependency bool) error {
	if !devDependency || h.includeDevDependencies {
		if _, ok := h.dependencies[name]; ok {
			return fmt.Errorf("module depends on module %#v multiple times", name.String())
		}
		h.dependencies[name] = version
	}
	return nil
}

func (h *bazelDepCapturingModuleDotBazelHandler) Module(name label.Module, version *label.ModuleVersion, compatibilityLevel int, repoName label.ApparentRepo, bazelCompatibility []string) error {
	h.compatibilityLevel = compatibilityLevel
	return nil
}

func (bazelDepCapturingModuleDotBazelHandler) RegisterExecutionPlatforms(platformLabels []label.ApparentLabel, devDependency bool) error {
	return nil
}

func (bazelDepCapturingModuleDotBazelHandler) RegisterToolchains(toolchainLabels []label.ApparentLabel, devDependency bool) error {
	return nil
}

func (bazelDepCapturingModuleDotBazelHandler) UseExtension(extensionBzlFile label.ApparentLabel, extensionName string, devDependency, isolate bool) (pg_starlark.ModuleExtensionProxy, error) {
	return pg_starlark.NullModuleExtensionProxy, nil
}

func (bazelDepCapturingModuleDotBazelHandler) UseRepoRule(repoRuleBzlFile label.ApparentLabel, repoRuleName string) (pg_starlark.RepoRuleProxy, error) {
	return func(name label.ApparentRepo, devDependency bool, attrs map[string]starlark.Value) error {
		return nil
	}, nil
}

type moduleToCheck struct {
	name    label.Module
	version label.ModuleVersion
}

type roughBuildList struct {
	ds.Slice[*model_analysis_pb.BuildList_Module]
}

func (l roughBuildList) Less(i, j int) bool {
	ei, ej := l.Slice[i], l.Slice[j]
	if ni, nj := ei.Name, ej.Name; ni < nj {
		return true
	} else if ni > nj {
		return false
	}

	// Compare by version, falling back to string comparison if
	// their canonical values are the same.
	if cmp := label.MustNewModuleVersion(ei.Version).Compare(label.MustNewModuleVersion(ej.Version)); cmp < 0 {
		return true
	} else if cmp > 0 {
		return false
	}
	return ei.Version < ej.Version
}

type OverrideVersions []label.ModuleVersion

func (ov OverrideVersions) LookupNearestVersion(version *label.ModuleVersion) (label.ModuleVersion, error) {
	var badVersion label.ModuleVersion
	if version == nil {
		return badVersion, errors.New("module does not specify a version number, while multiple_version_override() requires a version number")
	}
	index := sort.Search(len(ov), func(i int) bool {
		return ov[i].Compare(*version) >= 0
	})
	if index >= len(ov) {
		return badVersion, fmt.Errorf("module depends on version %s, which exceeds maximum version %s", *version, ov[len(ov)-1])
	}
	return ov[index], nil
}

func parseOverridesList(overridesList *model_analysis_pb.OverridesList) (map[label.Module]OverrideVersions, error) {
	modules := make(map[label.Module]OverrideVersions, len(overridesList.Modules))
	for _, module := range overridesList.Modules {
		moduleNameStr := module.Name
		moduleName, err := label.NewModule(moduleNameStr)
		if err != nil {
			return nil, fmt.Errorf("invalid module name %#v: %w", moduleNameStr, err)
		}

		versions := make([]label.ModuleVersion, 0, len(module.Versions))
		for _, versionStr := range module.Versions {
			version, err := label.NewModuleVersion(versionStr)
			if err != nil {
				return nil, fmt.Errorf("Invalid version %#v for module %#v: %w", versionStr, moduleNameStr, err)
			}
			versions = append(versions, version)
		}
		modules[moduleName] = versions
	}
	return modules, nil
}

func getModuleDotBazelURL(registryURL string, module label.Module, moduleVersion label.ModuleVersion) (string, error) {
	return url.JoinPath(registryURL, "modules", module.String(), moduleVersion.String(), moduleDotBazelFilename)
}

func (c *baseComputer) ComputeModuleRoughBuildListValue(ctx context.Context, key *model_analysis_pb.ModuleRoughBuildList_Key, e ModuleRoughBuildListEnvironment) (PatchedModuleRoughBuildListValue, error) {
	rootModuleNameValue := e.GetRootModuleNameValue(&model_analysis_pb.RootModuleName_Key{})
	modulesWithOverridesValue := e.GetModulesWithOverridesValue(&model_analysis_pb.ModulesWithOverrides_Key{})
	registryURLsValue := e.GetModuleRegistryUrlsValue(&model_analysis_pb.ModuleRegistryUrls_Key{})
	if !rootModuleNameValue.IsSet() || !modulesWithOverridesValue.IsSet() || !registryURLsValue.IsSet() {
		return PatchedModuleRoughBuildListValue{}, evaluation.ErrMissingDependency
	}

	// Obtain the root module name. Traversal of the modules needs
	// to start there.
	rootModuleName, err := label.NewModule(rootModuleNameValue.Message.RootModuleName)
	if err != nil {
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
			Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
				Failure: err.Error(),
			},
		}), nil
	}

	// Obtain the list of modules for which overrides are in place.
	// For these we should not attempt to load MODULE.bazel files
	// from Bazel Central Registry (BCR).
	var modulesWithOverrides map[label.Module]OverrideVersions
	switch modulesWithOverridesResult := modulesWithOverridesValue.Message.Result.(type) {
	case *model_analysis_pb.ModulesWithOverrides_Value_Success:
		modulesWithOverrides, err = parseOverridesList(modulesWithOverridesResult.Success)
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
				Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}
	case *model_analysis_pb.ModulesWithOverrides_Value_Failure:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
			Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
				Failure: modulesWithOverridesResult.Failure,
			},
		}), nil
	default:
		return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
			Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
				Failure: "Module has override value has an unknown result type",
			},
		}), nil
	}

	fileReader, err := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if err != nil {
		if !errors.Is(err, evaluation.ErrMissingDependency) {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
				Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}
		return PatchedModuleRoughBuildListValue{}, err
	}

	includeDevDependencies := true
	modulesToCheck := []moduleToCheck{{
		name:    rootModuleName,
		version: label.MustNewModuleVersion("0"),
	}}
	modulesSeen := map[moduleToCheck]struct{}{}
	missingDependencies := false
	registryURLs := registryURLsValue.Message.RegistryUrls
	var buildList roughBuildList

ProcessModule:
	for len(modulesToCheck) > 0 {
		module := modulesToCheck[0]
		modulesToCheck = modulesToCheck[1:]
		var moduleFileContents model_core.Message[*model_filesystem_pb.FileContents]
		var buildListEntry *model_analysis_pb.BuildList_Module
		if versions, ok := modulesWithOverrides[module.name]; ok {
			// An override for the module exists. This means
			// that we can access its sources directly and
			// load the MODULE.bazel file contained within.
			moduleRepo := module.name.String() + "+"
			if len(versions) > 1 {
				moduleRepo += module.version.String()
			}
			moduleFileProperties := e.GetFilePropertiesValue(&model_analysis_pb.FileProperties_Key{
				CanonicalRepo: moduleRepo,
				Path:          moduleDotBazelFilename,
			})
			if !moduleFileProperties.IsSet() {
				missingDependencies = true
				continue ProcessModule
			}

			switch moduleFilePropertiesResult := moduleFileProperties.Message.Result.(type) {
			case *model_analysis_pb.FileProperties_Value_Exists:
				moduleFileContents = model_core.Message[*model_filesystem_pb.FileContents]{
					Message:            moduleFilePropertiesResult.Exists.Contents,
					OutgoingReferences: moduleFileProperties.OutgoingReferences,
				}
			case *model_analysis_pb.FileProperties_Value_DoesNotExist:
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
					Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
						Failure: fmt.Sprintf("%#v does not exist", moduleDotBazelFilename),
					},
				}), nil
			case *model_analysis_pb.FileProperties_Value_Failure:
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
					Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
						Failure: moduleFilePropertiesResult.Failure,
					},
				}), nil
			default:
				return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
					Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
						Failure: "File properties value has an unknown result type",
					},
				}), nil
			}
		} else {
			// No override exists. Download the MODULE.bazel
			// file from Bazel Central Registry (BCR). We
			// don't want to download the full sources just
			// yet, as there is no guarantee that this is
			// the definitive version to load.
			for _, registryURL := range registryURLs {
				moduleFileURL, err := getModuleDotBazelURL(registryURL, module.name, module.version)
				if err != nil {
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
						Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
							Failure: fmt.Sprintf("Failed to construct URL for module %s with version %s in registry %#v: %s", module.name, module.version, registryURL),
						},
					}), nil
				}
				httpFileContents := e.GetHttpFileContentsValue(&model_analysis_pb.HttpFileContents_Key{Url: moduleFileURL})
				if !httpFileContents.IsSet() {
					missingDependencies = true
					continue ProcessModule
				}

				switch httpFileContentsResult := httpFileContents.Message.Result.(type) {
				case *model_analysis_pb.HttpFileContents_Value_Exists_:
					moduleFileContents = model_core.Message[*model_filesystem_pb.FileContents]{
						Message:            httpFileContentsResult.Exists.Contents,
						OutgoingReferences: httpFileContents.OutgoingReferences,
					}
					buildListEntry = &model_analysis_pb.BuildList_Module{
						Name:        module.name.String(),
						Version:     module.version.String(),
						RegistryUrl: registryURL,
					}
					goto GotModuleFileContents
				case *model_analysis_pb.HttpFileContents_Value_DoesNotExist:
					// Attempt next registry.
				case *model_analysis_pb.HttpFileContents_Value_Failure:
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
						Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
							Failure: fmt.Sprintf("Failed to fetch %#v: %s", moduleFileURL, httpFileContentsResult.Failure),
						},
					}), nil
				default:
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
						Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
							Failure: "HTTP file contents value has an unknown result type",
						},
					}), nil
				}
			}

			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
				Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
					Failure: fmt.Sprintf("Module %s with version %s cannot be found in any of the provided registries", module.name, module.version),
				},
			}), nil
		}

	GotModuleFileContents:
		// Load the contents of MODULE.bazel and extract all
		// calls to bazel_dep().
		moduleFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
			moduleFileContents,
			c.buildSpecificationReference.GetReferenceFormat(),
		)
		if err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
				Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
					Failure: util.StatusWrap(err, "Invalid file contents").Error(),
				},
			}), nil
		}

		moduleFileData, err := fileReader.FileReadAll(ctx, moduleFileContentsEntry, 1<<20)
		if err != nil {
			return PatchedModuleRoughBuildListValue{}, err
		}

		handler := bazelDepCapturingModuleDotBazelHandler{
			includeDevDependencies: includeDevDependencies,
			dependencies:           map[label.Module]*label.ModuleVersion{},
		}
		includeDevDependencies = false
		if err := pg_starlark.ParseModuleDotBazel(
			string(moduleFileData),
			module.name.
				ToModuleInstance(nil).
				GetBareCanonicalRepo().
				GetRootPackage().
				AppendTargetName(moduleDotBazelTargetName),
			nil,
			pg_starlark.NewOverrideIgnoringRootModuleDotBazelHandler(&handler),
		); err != nil {
			return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
				Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
					Failure: err.Error(),
				},
			}), nil
		}

		if buildListEntry != nil {
			buildListEntry.CompatibilityLevel = int32(handler.compatibilityLevel)
			buildList.Slice = append(buildList.Slice, buildListEntry)
		}

		for dependencyName, dependencyVersion := range handler.dependencies {
			dependency := moduleToCheck{name: dependencyName}
			if versions, ok := modulesWithOverrides[dependencyName]; !ok {
				// No override exists, meaning we need
				// to check an exact version.
				if dependencyVersion == nil {
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
						Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
							Failure: fmt.Sprintf("Module %s depends on module %s without specifying a version number, while no override is in place", module.name, dependencyName),
						},
					}), nil
				}
				dependency.version = *dependencyVersion
			} else if len(versions) > 0 {
				// multiple_version_override() is used
				// for the current module. Round up to
				// the nearest higher version number.
				v, err := versions.LookupNearestVersion(dependencyVersion)
				if err != nil {
					return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
						Result: &model_analysis_pb.ModuleRoughBuildList_Value_Failure{
							Failure: fmt.Sprintf("Dependency of module %s on module %s: %s", module.name, dependencyName, err),
						},
					}), nil
				}
				dependency.version = v
			}
			if _, ok := modulesSeen[dependency]; !ok {
				modulesSeen[dependency] = struct{}{}
				modulesToCheck = append(modulesToCheck, dependency)
			}
		}
	}

	if missingDependencies {
		return PatchedModuleRoughBuildListValue{}, evaluation.ErrMissingDependency
	}

	sort.Sort(buildList)
	return model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_analysis_pb.ModuleRoughBuildList_Value{
		Result: &model_analysis_pb.ModuleRoughBuildList_Value_Success{
			Success: &model_analysis_pb.BuildList{
				Modules: buildList.Slice,
			},
		},
	}), nil
}
