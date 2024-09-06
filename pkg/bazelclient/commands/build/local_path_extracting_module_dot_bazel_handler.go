package build

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/buildbarn/bb-playground/pkg/label"
	pg_starlark "github.com/buildbarn/bb-playground/pkg/starlark"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"go.starlark.net/starlark"
)

type LocalPathExtractingModuleDotBazelHandler struct {
	modulePaths    map[label.Module]path.Parser
	rootModulePath path.Parser
	rootModuleName *label.Module
}

// NewLocalPathExtractingModuleDotBazelHandler is capable of capturing
// the paths contained in local_path_override() directives of a
// MODULE.bazel file. These paths are needed by the client to
// determine
func NewLocalPathExtractingModuleDotBazelHandler(modulePaths map[label.Module]path.Parser, rootModulePath path.Parser) *LocalPathExtractingModuleDotBazelHandler {
	return &LocalPathExtractingModuleDotBazelHandler{
		modulePaths:    modulePaths,
		rootModulePath: rootModulePath,
	}
}

func (h *LocalPathExtractingModuleDotBazelHandler) GetRootModuleName() (label.Module, error) {
	if h.rootModuleName == nil {
		var badModule label.Module
		return badModule, errors.New("MODULE.bazel of root module does not contain a module() declaration")
	}
	return *h.rootModuleName, nil
}

func (LocalPathExtractingModuleDotBazelHandler) ArchiveOverride(moduleName label.Module, urls []*url.URL, integrity string, stripPrefix path.Parser, patchOptions *pg_starlark.PatchOptions) error {
	return nil
}

func (LocalPathExtractingModuleDotBazelHandler) BazelDep(name label.Module, version string, maxCompatibilityLevel int, repoName label.ApparentRepo, devDependency bool) error {
	return nil
}

func (LocalPathExtractingModuleDotBazelHandler) GitOverride(moduleName label.Module, remote *url.URL, commit string, patchOptions *pg_starlark.PatchOptions, initSubmodules bool, stripPrefix path.Parser) error {
	return nil
}

func (h *LocalPathExtractingModuleDotBazelHandler) LocalPathOverride(moduleName label.Module, path path.Parser) error {
	if _, ok := h.modulePaths[moduleName]; ok {
		return fmt.Errorf("multiple local_path_override() or module() declarations for module with name %#v", moduleName.String())
	}
	h.modulePaths[moduleName] = path
	return nil
}

func (h *LocalPathExtractingModuleDotBazelHandler) Module(name label.Module, version string, compatibilityLevel int, repoName label.ApparentRepo, bazelCompatibility []string) error {
	if h.rootModuleName != nil {
		return errors.New("multiple module() declarations")
	}
	h.rootModuleName = &name
	return h.LocalPathOverride(name, h.rootModulePath)
}

func (LocalPathExtractingModuleDotBazelHandler) MultipleVersionOverride(moduleName label.Module, versions []string, registry *url.URL) error {
	return nil
}

func (LocalPathExtractingModuleDotBazelHandler) RegisterExecutionPlatforms(platformLabels []label.Label, devDependency bool) error {
	return nil
}

func (LocalPathExtractingModuleDotBazelHandler) RegisterToolchains(toolchainLabels []label.Label, devDependency bool) error {
	return nil
}

func (LocalPathExtractingModuleDotBazelHandler) SingleVersionOverride(moduleName label.Module, version string, registry *url.URL, patchOptions *pg_starlark.PatchOptions) error {
	return nil
}

func (LocalPathExtractingModuleDotBazelHandler) UseExtension(extensionBzlFile label.Label, extensionName string, devDependency, isolate bool) (pg_starlark.ModuleExtensionProxy, error) {
	return nullModuleExtensionProxy{}, nil
}

func (LocalPathExtractingModuleDotBazelHandler) UseRepoRule(repoRuleBzlFile label.Label, repoRuleName string) (pg_starlark.RepoRuleProxy, error) {
	return func(name label.ApparentRepo, devDependency bool, attrs map[string]starlark.Value) error {
		return nil
	}, nil
}

type nullModuleExtensionProxy struct{}

func (nullModuleExtensionProxy) Tag(className string, attrs map[string]starlark.Value) error {
	return nil
}

func (nullModuleExtensionProxy) UseRepo(repos map[label.ApparentRepo]label.ApparentRepo) error {
	return nil
}