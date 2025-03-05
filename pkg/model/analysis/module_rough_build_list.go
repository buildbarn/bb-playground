package analysis

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"

	"github.com/buildbarn/bonanza/pkg/ds"
	"github.com/buildbarn/bonanza/pkg/evaluation"
	"github.com/buildbarn/bonanza/pkg/label"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_filesystem_pb "github.com/buildbarn/bonanza/pkg/proto/model/filesystem"
	pg_starlark "github.com/buildbarn/bonanza/pkg/starlark"
	"github.com/buildbarn/bonanza/pkg/storage/dag"

	"go.starlark.net/starlark"
)

const moduleDotBazelFilename = "MODULE.bazel"

var moduleDotBazelTargetName = label.MustNewTargetName(moduleDotBazelFilename)

type bazelDepCapturingModuleDotBazelHandler struct {
	ignoreDevDependencies bool

	compatibilityLevel int
	dependencies       map[label.Module]*label.ModuleVersion
}

func (h *bazelDepCapturingModuleDotBazelHandler) BazelDep(name label.Module, version *label.ModuleVersion, maxCompatibilityLevel int, repoName label.ApparentRepo, devDependency bool) error {
	if !devDependency || !h.ignoreDevDependencies {
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

func (bazelDepCapturingModuleDotBazelHandler) RegisterExecutionPlatforms(platformTargetPatterns []label.ApparentTargetPattern, devDependency bool) error {
	return nil
}

func (bazelDepCapturingModuleDotBazelHandler) RegisterToolchains(toolchainTargetPatterns []label.ApparentTargetPattern, devDependency bool) error {
	return nil
}

func (bazelDepCapturingModuleDotBazelHandler) UseExtension(extensionBzlFile label.ApparentLabel, extensionName label.StarlarkIdentifier, devDependency, isolate bool) (pg_starlark.ModuleExtensionProxy, error) {
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
	ds.Slice[*model_analysis_pb.BuildListModule]
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

func parseOverridesList(overridesList []*model_analysis_pb.OverridesListModule) (map[label.Module]OverrideVersions, error) {
	modules := make(map[label.Module]OverrideVersions, len(overridesList))
	for _, module := range overridesList {
		moduleNameStr := module.Name
		moduleName, err := label.NewModule(moduleNameStr)
		if err != nil {
			return nil, fmt.Errorf("invalid module name %#v: %w", moduleNameStr, err)
		}

		versions := make([]label.ModuleVersion, 0, len(module.Versions))
		for _, versionStr := range module.Versions {
			version, err := label.NewModuleVersion(versionStr)
			if err != nil {
				return nil, fmt.Errorf("invalid version %#v for module %#v: %w", versionStr, moduleNameStr, err)
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

func (c *baseComputer[TReference]) ComputeModuleRoughBuildListValue(ctx context.Context, key *model_analysis_pb.ModuleRoughBuildList_Key, e ModuleRoughBuildListEnvironment[TReference]) (PatchedModuleRoughBuildListValue, error) {
	rootModuleValue := e.GetRootModuleValue(&model_analysis_pb.RootModule_Key{})
	modulesWithOverridesValue := e.GetModulesWithOverridesValue(&model_analysis_pb.ModulesWithOverrides_Key{})
	registryURLsValue := e.GetModuleRegistryUrlsValue(&model_analysis_pb.ModuleRegistryUrls_Key{})
	fileReader, gotFileReader := e.GetFileReaderValue(&model_analysis_pb.FileReader_Key{})
	if !rootModuleValue.IsSet() || !modulesWithOverridesValue.IsSet() || !registryURLsValue.IsSet() || !gotFileReader {
		return PatchedModuleRoughBuildListValue{}, evaluation.ErrMissingDependency
	}

	// Obtain the root module name. Traversal of the modules needs
	// to start there.
	rootModuleName, err := label.NewModule(rootModuleValue.Message.RootModuleName)
	if err != nil {
		return PatchedModuleRoughBuildListValue{}, fmt.Errorf("invalid root module name %#v: %w", rootModuleValue.Message.RootModuleName, err)
	}

	// Obtain the list of modules for which overrides are in place.
	// For these we should not attempt to load MODULE.bazel files
	// from Bazel Central Registry (BCR).
	modulesWithOverrides, err := parseOverridesList(modulesWithOverridesValue.Message.OverridesList)
	if err != nil {
		return PatchedModuleRoughBuildListValue{}, err
	}

	ignoreDevDependencies := rootModuleValue.Message.IgnoreRootModuleDevDependencies
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
		var moduleFileContents model_core.Message[*model_filesystem_pb.FileContents, TReference]
		var buildListEntry *model_analysis_pb.BuildListModule
		if versions, ok := modulesWithOverrides[module.name]; ok {
			// An override for the module exists. This means
			// that we can access its sources directly and
			// load the MODULE.bazel file contained within.
			var moduleInstance label.ModuleInstance
			if len(versions) > 1 {
				moduleInstance = module.name.ToModuleInstance(&module.version)
			} else {
				moduleInstance = module.name.ToModuleInstance(nil)
			}
			moduleFileContentsValue := e.GetModuleDotBazelContentsValue(&model_analysis_pb.ModuleDotBazelContents_Key{
				ModuleInstance: moduleInstance.String(),
			})
			if !moduleFileContentsValue.IsSet() {
				missingDependencies = true
				continue ProcessModule
			}
			moduleFileContents = model_core.NewNestedMessage(moduleFileContentsValue, moduleFileContentsValue.Message.Contents)
		} else {
			// No override exists. Download the MODULE.bazel
			// file from Bazel Central Registry (BCR). We
			// don't want to download the full sources just
			// yet, as there is no guarantee that this is
			// the definitive version to load.
			for _, registryURL := range registryURLs {
				moduleFileURL, err := getModuleDotBazelURL(registryURL, module.name, module.version)
				if err != nil {
					return PatchedModuleRoughBuildListValue{}, fmt.Errorf("failed to construct URL for module %s with version %s in registry %#v: %w", module.name, module.version, registryURL, err)
				}
				httpFileContents := e.GetHttpFileContentsValue(&model_analysis_pb.HttpFileContents_Key{Urls: []string{moduleFileURL}})
				if !httpFileContents.IsSet() {
					missingDependencies = true
					continue ProcessModule
				}
				if httpFileContents.Message.Exists != nil {
					moduleFileContents = model_core.NewNestedMessage(httpFileContents, httpFileContents.Message.Exists.Contents)
					buildListEntry = &model_analysis_pb.BuildListModule{
						Name:        module.name.String(),
						Version:     module.version.String(),
						RegistryUrl: registryURL,
					}
					goto GotModuleFileContents
				}
			}
			return PatchedModuleRoughBuildListValue{}, fmt.Errorf("module %s with version %s cannot be found in any of the provided registries", module.name, module.version)
		}

	GotModuleFileContents:
		// Load the contents of MODULE.bazel and extract all
		// calls to bazel_dep().
		moduleFileContentsEntry, err := model_filesystem.NewFileContentsEntryFromProto(
			moduleFileContents,
		)
		if err != nil {
			return PatchedModuleRoughBuildListValue{}, fmt.Errorf("invalid file contents: %w", err)
		}

		moduleFileData, err := fileReader.FileReadAll(ctx, moduleFileContentsEntry, 1<<20)
		if err != nil {
			return PatchedModuleRoughBuildListValue{}, err
		}

		handler := bazelDepCapturingModuleDotBazelHandler{
			ignoreDevDependencies: ignoreDevDependencies,
			dependencies:          map[label.Module]*label.ModuleVersion{},
		}
		ignoreDevDependencies = true
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
			return PatchedModuleRoughBuildListValue{}, err
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
					return PatchedModuleRoughBuildListValue{}, fmt.Errorf("module %s depends on module %s without specifying a version number, while no override is in place", module.name, dependencyName)
				}
				dependency.version = *dependencyVersion
			} else if len(versions) > 0 {
				// multiple_version_override() is used
				// for the current module. Round up to
				// the nearest higher version number.
				v, err := versions.LookupNearestVersion(dependencyVersion)
				if err != nil {
					return PatchedModuleRoughBuildListValue{}, fmt.Errorf("dependency of module %s on module %s: %s", module.name, dependencyName, err)
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
		BuildList: buildList.Slice,
	}), nil
}
