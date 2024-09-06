package starlark

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/buildbarn/bb-playground/pkg/label"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"go.starlark.net/starlark"
)

// ModuleExtensionProxy is called into by ParseModuleDotBazel() whenever
// module extension tags are declared, or if use_repo() is called
// against a given module extension.
type ModuleExtensionProxy interface {
	Tag(className string, attrs map[string]starlark.Value) error
	UseRepo(repos map[label.ApparentRepo]label.ApparentRepo) error
}

// RepoRuleProxy is called into by ParseModuleDotBazel() whenever a
// repository rule is invoked.
type RepoRuleProxy func(name label.ApparentRepo, devDependency bool, attrs map[string]starlark.Value) error

// PatchOptions contains the common set of properties that are accepted
// by MODULE.bazel's archive_override(), git_override() and
// single_version_override().
type PatchOptions struct {
	Patches    []label.Label
	PatchCmds  []string
	PatchStrip int
}

// ModuleDotBazelHandler is call into by ParseModuleDotBazel() for each
// top-level declaration.
type ModuleDotBazelHandler interface {
	ArchiveOverride(moduleName label.Module, urls []*url.URL, integrity string, stripPrefix path.Parser, patchOptions *PatchOptions) error
	BazelDep(name label.Module, version string, maxCompatibilityLevel int, repoName label.ApparentRepo, devDependency bool) error
	GitOverride(moduleName label.Module, remote *url.URL, commit string, patchOptions *PatchOptions, initSubmodules bool, stripPrefix path.Parser) error
	LocalPathOverride(moduleName label.Module, path path.Parser) error
	Module(name label.Module, version string, compatibilityLevel int, repoName label.ApparentRepo, bazelCompatibility []string) error
	MultipleVersionOverride(moduleName label.Module, versions []string, registry *url.URL) error
	RegisterExecutionPlatforms(platformLabels []label.Label, devDependency bool) error
	RegisterToolchains(toolchainLabels []label.Label, devDependency bool) error
	SingleVersionOverride(moduleName label.Module, version string, registry *url.URL, patchOptions *PatchOptions) error
	UseExtension(extensionBzlFile label.Label, extensionName string, devDependency, isolate bool) (ModuleExtensionProxy, error)
	UseRepoRule(repoRuleBzlFile label.Label, repoRuleName string) (RepoRuleProxy, error)
}

type moduleExtensionProxyValue struct {
	name  string
	proxy ModuleExtensionProxy
}

var _ starlark.HasAttrs = &moduleExtensionProxyValue{}

func (v *moduleExtensionProxyValue) String() string {
	return v.name
}

func (v *moduleExtensionProxyValue) Type() string {
	return "module_extension_proxy"
}

func (v *moduleExtensionProxyValue) Freeze() {}

func (v *moduleExtensionProxyValue) Truth() starlark.Bool {
	return starlark.True
}

func (v *moduleExtensionProxyValue) Hash() (uint32, error) {
	return starlark.String(v.name).Hash()
}

func (v *moduleExtensionProxyValue) Attr(name string) (starlark.Value, error) {
	return starlark.NewBuiltin(name, func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if len(args) > 0 {
			return nil, errors.New("module tags do not take positional arguments")
		}
		attrs := make(map[string]starlark.Value, len(kwargs))
		for _, kwarg := range kwargs {
			attrs[string(kwarg[0].(starlark.String))] = kwarg[1]
		}
		return starlark.None, v.proxy.Tag(name, attrs)
	}), nil
}

func (v *moduleExtensionProxyValue) AttrNames() []string {
	// We don't allow iteration over attributes, because that
	// prevents evaluation without analyzing module extensions up
	// front.
	return nil
}

// Parse a MODULE.bazel file, and call into ModuleDotBazelHandler for
// every observed declaration.
func ParseModuleDotBazel(contents string, localPathFormat path.Format, handler ModuleDotBazelHandler) error {
	_, err := starlark.ExecFile(
		&starlark.Thread{
			Name: "main",
			Print: func(_ *starlark.Thread, msg string) {
				// TODO: Provide logging sink.
				fmt.Println(msg)
			},
		},
		"MODULE.bazel",
		contents,
		starlark.StringDict{
			"archive_override": starlark.NewBuiltin("archive_override", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var moduleName label.Module
				var urls []*url.URL
				integrity := ""
				var stripPrefix path.Parser = &path.EmptyBuilder
				var patchOptions PatchOptions
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"module_name", unpack.Bind(&moduleName, unpack.Module),
					"urls", unpack.Bind(&urls, unpack.List(unpack.URL)),
					"integrity?", unpack.Bind(&integrity, unpack.String),
					"strip_prefix?", unpack.Bind(&stripPrefix, unpack.PathParser(path.UNIXFormat)),
					"patches?", unpack.Bind(&patchOptions.Patches, unpack.List(unpack.Label)),
					"patch_cmds?", unpack.Bind(&patchOptions.PatchCmds, unpack.List(unpack.String)),
					"patch_strip?", unpack.Bind(&patchOptions.PatchStrip, unpack.Int),
				); err != nil {
					return nil, err
				}
				return starlark.None, handler.ArchiveOverride(
					moduleName,
					urls,
					integrity,
					stripPrefix,
					&patchOptions,
				)
			}),
			"bazel_dep": starlark.NewBuiltin("bazel_dep", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var name label.Module
				version := ""
				maxCompatibilityLevel := -1
				var repoName *label.ApparentRepo
				devDependency := false
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"name", unpack.Bind(&name, unpack.Module),
					"version?", unpack.Bind(&version, unpack.String),
					"max_compatibility_level?", unpack.Bind(&maxCompatibilityLevel, unpack.Int),
					"repo_name?", unpack.Bind(&repoName, unpack.IfNonEmptyString(unpack.Pointer(unpack.ApparentRepo))),
					"dev_dependency?", unpack.Bind(&devDependency, unpack.Bool),
				); err != nil {
					return nil, err
				}
				if repoName == nil {
					r := name.ToApparentRepo()
					repoName = &r
				}
				return starlark.None, handler.BazelDep(
					name,
					version,
					maxCompatibilityLevel,
					*repoName,
					devDependency,
				)
			}),
			"git_override": starlark.NewBuiltin("git_override", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var moduleName label.Module
				var remote *url.URL
				commit := ""
				var patchOptions PatchOptions
				initSubmodules := false
				var stripPrefix path.Parser = &path.EmptyBuilder
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"module_name", unpack.Bind(&moduleName, unpack.Module),
					"remote", unpack.Bind(&remote, unpack.URL),
					"commit?", unpack.Bind(&commit, unpack.String),
					"patches?", unpack.Bind(&patchOptions.Patches, unpack.List(unpack.Label)),
					"patch_cmds?", unpack.Bind(&patchOptions.PatchCmds, unpack.List(unpack.String)),
					"patch_strip?", unpack.Bind(&patchOptions.PatchStrip, unpack.Int),
					"init_submodules?", unpack.Bind(&initSubmodules, unpack.Bool),
					"strip_prefix?", unpack.Bind(&stripPrefix, unpack.PathParser(path.UNIXFormat)),
				); err != nil {
					return nil, err
				}
				return starlark.None, handler.GitOverride(
					moduleName,
					remote,
					commit,
					&patchOptions,
					initSubmodules,
					stripPrefix,
				)
			}),
			"include": starlark.NewBuiltin("include", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				return nil, errors.New("include() is not permitted, as it prevents modules from being reused")
			}),
			"local_path_override": starlark.NewBuiltin("local_path_override", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				// Skip local_path_override() directives
				// if the local path format isn't known.
				if localPathFormat == nil {
					return starlark.None, nil
				}

				var moduleName label.Module
				var path path.Parser
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"module_name", unpack.Bind(&moduleName, unpack.Module),
					"path", unpack.Bind(&path, unpack.PathParser(localPathFormat)),
				); err != nil {
					return nil, err
				}
				return starlark.None, handler.LocalPathOverride(
					moduleName,
					path,
				)
			}),
			"module": starlark.NewBuiltin("module", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var name label.Module
				version := ""
				compatibilityLevel := 0
				var repoName *label.ApparentRepo
				var bazelCompatibility []string
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"name", unpack.Bind(&name, unpack.Module),
					"version?", unpack.Bind(&version, unpack.String),
					"compatibility_level?", unpack.Bind(&compatibilityLevel, unpack.Int),
					"repo_name?", unpack.Bind(&repoName, unpack.IfNonEmptyString(unpack.Pointer(unpack.ApparentRepo))),
					"bazel_compatibility?", unpack.Bind(&bazelCompatibility, unpack.List(unpack.String)),
				); err != nil {
					return nil, err
				}
				if repoName == nil {
					r := name.ToApparentRepo()
					repoName = &r
				}
				return starlark.None, handler.Module(
					name,
					version,
					compatibilityLevel,
					*repoName,
					bazelCompatibility,
				)
			}),
			"multiple_version_override": starlark.NewBuiltin("multiple_version_override", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var moduleName label.Module
				var versions []string
				var registry *url.URL
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"module_name", unpack.Bind(&moduleName, unpack.Module),
					"versions", unpack.Bind(&versions, unpack.List(unpack.String)),
					"registry?", unpack.Bind(&registry, unpack.IfNonEmptyString(unpack.URL)),
				); err != nil {
					return nil, err
				}
				return starlark.None, handler.MultipleVersionOverride(
					moduleName,
					versions,
					registry,
				)
			}),
			"register_execution_platforms": starlark.NewBuiltin("register_execution_platforms", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var platformLabels []label.Label
				if err := unpack.List(unpack.Label)(args, &platformLabels); err != nil {
					return nil, err
				}
				devDependency := false
				if err := starlark.UnpackArgs(
					b.Name(), nil, kwargs,
					"dev_dependency?", unpack.Bind(&devDependency, unpack.Bool),
				); err != nil {
					return nil, err
				}
				return starlark.None, handler.RegisterExecutionPlatforms(
					platformLabels,
					devDependency,
				)
			}),
			"register_toolchains": starlark.NewBuiltin("register_toolchains", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var toolchainLabels []label.Label
				if err := unpack.List(unpack.Label)(args, &toolchainLabels); err != nil {
					return nil, err
				}
				devDependency := false
				if err := starlark.UnpackArgs(
					b.Name(), nil, kwargs,
					"dev_dependency?", unpack.Bind(&devDependency, unpack.Bool),
				); err != nil {
					return nil, err
				}
				return starlark.None, handler.RegisterToolchains(
					toolchainLabels,
					devDependency,
				)
			}),
			"single_version_override": starlark.NewBuiltin("single_version_override", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var moduleName label.Module
				var version string
				var registry *url.URL
				var patchOptions PatchOptions
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"module_name", unpack.Bind(&moduleName, unpack.Module),
					"version?", unpack.Bind(&version, unpack.String),
					"registry?", unpack.Bind(&registry, unpack.IfNonEmptyString(unpack.URL)),
					"patches?", unpack.Bind(&patchOptions.Patches, unpack.List(unpack.Label)),
					"patch_cmds?", unpack.Bind(&patchOptions.PatchCmds, unpack.List(unpack.String)),
					"patch_strip?", unpack.Bind(&patchOptions.PatchStrip, unpack.Int),
				); err != nil {
					return nil, err
				}
				return starlark.None, handler.SingleVersionOverride(
					moduleName,
					version,
					registry,
					&patchOptions,
				)
			}),
			"use_extension": starlark.NewBuiltin("use_extension", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) > 2 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at most 2", b.Name(), len(args))
				}
				var extensionBzlFile label.Label
				var extensionName string
				devDependency := false
				isolate := false
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"extension_bzl_file", unpack.Bind(&extensionBzlFile, unpack.Label),
					"extension_name", unpack.Bind(&extensionName, unpack.String),
					"dev_dependency?", unpack.Bind(&devDependency, unpack.Bool),
					"isolate?", unpack.Bind(&isolate, unpack.Bool),
				); err != nil {
					return nil, err
				}
				moduleExtensionProxy, err := handler.UseExtension(
					extensionBzlFile,
					extensionName,
					devDependency,
					isolate,
				)
				if err != nil {
					return nil, err
				}
				return &moduleExtensionProxyValue{
					name:  extensionName,
					proxy: moduleExtensionProxy,
				}, nil
			}),
			"use_repo": starlark.NewBuiltin("use_repo", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if len(args) < 1 {
					return nil, fmt.Errorf("%s: got %d positional arguments, want at least 1", b.Name(), len(args))
				}

				proxyValue := args.Index(0)
				proxyObject, ok := proxyValue.(*moduleExtensionProxyValue)
				if !ok {
					return nil, fmt.Errorf("%s: for parameter 0: got %s, want module_extension_proxy", b.Name(), proxyValue.Type())
				}

				repos := map[label.ApparentRepo]label.ApparentRepo{}
				for i := 1; i < len(args); i++ {
					var repo label.ApparentRepo
					if err := unpack.ApparentRepo(args[i], &repo); err != nil {
						return nil, fmt.Errorf("%s: for parameter %d: %w", b.Name(), i, err)
					}
					repos[repo] = repo
				}

				for _, kwarg := range kwargs {
					var key, value label.ApparentRepo
					if err := unpack.ApparentRepo(kwarg[0], &key); err != nil {
						return nil, fmt.Errorf("%s: for parameter %s: %w", b.Name(), kwarg[0].(starlark.String), err)
					}
					if err := unpack.ApparentRepo(kwarg[1], &value); err != nil {
						return nil, fmt.Errorf("%s: for parameter %s: %w", b.Name(), kwarg[0].(starlark.String), err)
					}
					if _, ok := repos[key]; ok {
						return nil, fmt.Errorf("%s: repository %s declared multiple times", b.Name(), kwarg[0].(starlark.String))
					}
					repos[key] = value
				}

				return starlark.None, proxyObject.proxy.UseRepo(repos)
			}),
			"use_repo_rule": starlark.NewBuiltin("use_repo_rule", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var repoRuleBzlFile label.Label
				var repoRuleName string
				if err := starlark.UnpackArgs(
					b.Name(), args, kwargs,
					"repo_rule_bzl_file", unpack.Bind(&repoRuleBzlFile, unpack.Label),
					"repo_rule_name", unpack.Bind(&repoRuleName, unpack.String),
				); err != nil {
					return nil, err
				}
				repoRuleProxy, err := handler.UseRepoRule(repoRuleBzlFile, repoRuleName)
				if err != nil {
					return nil, err
				}
				return starlark.NewBuiltin(repoRuleName, func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					if len(args) > 0 {
						return nil, fmt.Errorf("%s: got %d positional arguments, want 0", b.Name(), len(args))
					}

					var name *label.ApparentRepo
					var devDependency bool
					attrs := map[string]starlark.Value{}
					for _, kwarg := range kwargs {
						switch key := string(kwarg[0].(starlark.String)); key {
						case "name":
							if err := unpack.Pointer(unpack.ApparentRepo)(kwarg[1], &name); err != nil {
								return nil, fmt.Errorf("%s: for parameter %s: %w", b.Name(), key, err)
							}
						case "dev_dependency":
							if err := unpack.Bool(kwarg[1], &devDependency); err != nil {
								return nil, fmt.Errorf("%s: for parameter %s: %w", b.Name(), key, err)
							}
						default:
							attrs[key] = kwarg[1]
						}
					}

					if name == nil {
						return nil, fmt.Errorf("%s: missing name argument", b.Name())
					}
					return starlark.None, repoRuleProxy(*name, devDependency, attrs)
				}), nil
			}),
		},
	)
	return err
}
