package starlark_test

import (
	"net/url"
	"testing"

	"github.com/buildbarn/bb-playground/pkg/label"
	pg_starlark "github.com/buildbarn/bb-playground/pkg/starlark"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"

	"go.starlark.net/starlark"

	"go.uber.org/mock/gomock"
)

func TestParseModuleDotBazel(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("AllDirectives", func(t *testing.T) {
		handler := NewMockRootModuleDotBazelHandler(ctrl)

		url1, err := url.Parse("https://example.com/url1")
		require.NoError(t, err)
		url2, err := url.Parse("https://example.com/url2")
		require.NoError(t, err)
		gomock.InOrder(
			handler.EXPECT().ArchiveOverride(
				/* moduleName = */ label.MustNewModule("my_module_name"),
				/* urls = */ []*url.URL{
					url1,
					url2,
				},
				/* integrity = */ "",
				/* stripPrefix = */ gomock.Any(),
				/* patchOptions = */ &pg_starlark.PatchOptions{},
			).Do(func(moduleName label.Module, urls []*url.URL, integrity string, stripPrefix path.Parser, patchOptions *pg_starlark.PatchOptions) {
				stripPrefixBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(stripPrefix, scopeWalker))
				require.Equal(t, ".", stripPrefixBuilder.GetUNIXString())
			}),
			handler.EXPECT().ArchiveOverride(
				/* moduleName = */ label.MustNewModule("my_module_name"),
				/* urls = */ []*url.URL{
					url1,
					url2,
				},
				/* integrity = */ "",
				/* stripPrefix = */ gomock.Any(),
				/* patchOptions = */ &pg_starlark.PatchOptions{
					Patches:   []label.Label{},
					PatchCmds: []string{},
				},
			).Do(func(moduleName label.Module, urls []*url.URL, integrity string, stripPrefix path.Parser, patchOptions *pg_starlark.PatchOptions) {
				stripPrefixBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(stripPrefix, scopeWalker))
				require.Equal(t, ".", stripPrefixBuilder.GetUNIXString())
			}),
			handler.EXPECT().ArchiveOverride(
				/* moduleName = */ label.MustNewModule("my_module_name"),
				/* urls = */ []*url.URL{
					url1,
					url2,
				},
				/* integrity = */ "sha384-oqVuAfXRKap7fdgcCY5uykM6+R9GqQ8K/uxy9rx7HNQlGYl1kPzQho1wx4JwY8wC",
				/* stripPrefix = */ gomock.Any(),
				/* patchOptions = */ &pg_starlark.PatchOptions{
					Patches: []label.Label{
						label.MustNewLabel("//:patches/foo1.diff"),
						label.MustNewLabel("//:patches/foo2.diff"),
					},
					PatchCmds: []string{
						"ls -l",
						"rm -rf /",
					},
					PatchStrip: 3,
				},
			).Do(func(moduleName label.Module, urls []*url.URL, integrity string, stripPrefix path.Parser, patchOptions *pg_starlark.PatchOptions) {
				stripPrefixBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(stripPrefix, scopeWalker))
				require.Equal(t, "some/prefix", stripPrefixBuilder.GetUNIXString())
			}),
		)

		version1 := label.MustNewModuleVersion("1.2.3")
		gomock.InOrder(
			handler.EXPECT().BazelDep(
				/* name = */ label.MustNewModule("my_module_name"),
				/* version = */ nil,
				/* maxCompatibilityLevel = */ -1,
				/* repoName = */ label.MustNewApparentRepo("my_module_name"),
				/* devDependency = */ false,
			).Times(2),
			handler.EXPECT().BazelDep(
				/* name = */ label.MustNewModule("my_module_name"),
				/* version = */ &version1,
				/* maxCompatibilityLevel = */ 123,
				/* repoName = */ label.MustNewApparentRepo("my_repo_name"),
				/* devDependency = */ true,
			),
		)

		remote, err := url.Parse("https://github.com/my-project/my-project.git")
		require.NoError(t, err)
		gomock.InOrder(
			handler.EXPECT().GitOverride(
				/* moduleName = */ label.MustNewModule("my_module_name"),
				/* remote = */ remote,
				/* commit = */ "",
				/* patchOptions = */ &pg_starlark.PatchOptions{},
				/* initSubmodules = */ false,
				/* stripPrefix = */ gomock.Any(),
			).Do(func(moduleName label.Module, remote *url.URL, commit string, patchOptions *pg_starlark.PatchOptions, initSubmodules bool, stripPrefix path.Parser) {
				stripPrefixBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(stripPrefix, scopeWalker))
				require.Equal(t, ".", stripPrefixBuilder.GetUNIXString())
			}),
			handler.EXPECT().GitOverride(
				/* moduleName = */ label.MustNewModule("my_module_name"),
				/* remote = */ remote,
				/* commit = */ "",
				/* patchOptions = */ &pg_starlark.PatchOptions{
					Patches:   []label.Label{},
					PatchCmds: []string{},
				},
				/* initSubmodules = */ false,
				/* stripPrefix = */ gomock.Any(),
			).Do(func(moduleName label.Module, remote *url.URL, commit string, patchOptions *pg_starlark.PatchOptions, initSubmodules bool, stripPrefix path.Parser) {
				stripPrefixBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(stripPrefix, scopeWalker))
				require.Equal(t, ".", stripPrefixBuilder.GetUNIXString())
			}),
			handler.EXPECT().GitOverride(
				/* moduleName = */ label.MustNewModule("my_module_name"),
				/* remote = */ remote,
				/* commit = */ "1368bebd5776a80ea3161a07dafe8beb7c8c144c",
				/* patchOptions = */ &pg_starlark.PatchOptions{
					Patches: []label.Label{
						label.MustNewLabel("//:patches/foo1.diff"),
						label.MustNewLabel("//:patches/foo2.diff"),
					},
					PatchCmds: []string{
						"ls -l",
						"rm -rf /",
					},
					PatchStrip: 3,
				},
				/* initSubmodules = */ true,
				/* stripPrefix = */ gomock.Any(),
			).Do(func(moduleName label.Module, remote *url.URL, commit string, patchOptions *pg_starlark.PatchOptions, initSubmodules bool, stripPrefix path.Parser) {
				stripPrefixBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(stripPrefix, scopeWalker))
				require.Equal(t, "some/prefix", stripPrefixBuilder.GetUNIXString())
			}),
		)

		handler.EXPECT().LocalPathOverride(
			/* moduleName = */ label.MustNewModule("my_module_name"),
			/* path = */ gomock.Any(),
		).Do(func(moduleName label.Module, localPath path.Parser) {
			localPathBuilder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
			require.NoError(t, path.Resolve(localPath, scopeWalker))
			require.Equal(t, "/some/path", localPathBuilder.GetUNIXString())
		})

		version2 := label.MustNewModuleVersion("1.0.0")
		gomock.InOrder(
			handler.EXPECT().Module(
				/* name = */ label.MustNewModule("my_module_name"),
				/* version = */ nil,
				/* compatibilityLevel = */ 0,
				/* repoName = */ label.MustNewApparentRepo("my_module_name"),
				/* bazelCompatibility = */ gomock.Len(0),
			).Times(2),
			handler.EXPECT().Module(
				/* name = */ label.MustNewModule("my_module_name"),
				/* version = */ &version2,
				/* compatibilityLevel = */ 123,
				/* repoName = */ label.MustNewApparentRepo("my_repo_name"),
				/* bazelCompatibility = */ []string{
					">=6.4.0",
					"-7.0.0",
				},
			),
		)

		registry, err := url.Parse("https://raw.githubusercontent.com/my-org/bazel-central-registry/main/")
		require.NoError(t, err)
		gomock.InOrder(
			handler.EXPECT().MultipleVersionOverride(
				/* moduleName = */ label.MustNewModule("my_module_name"),
				/* versions = */ []label.ModuleVersion{
					label.MustNewModuleVersion("1.0.0"),
					label.MustNewModuleVersion("1.2.0"),
				},
				/* registry = */ nil,
			).Times(2),
			handler.EXPECT().MultipleVersionOverride(
				/* moduleName = */ label.MustNewModule("my_module_name"),
				/* versions = */ []label.ModuleVersion{
					label.MustNewModuleVersion("1.0.0"),
					label.MustNewModuleVersion("1.2.0"),
				},
				/* registry = */ registry,
			),
		)

		gomock.InOrder(
			handler.EXPECT().RegisterExecutionPlatforms(
				/* platformLabels = */ gomock.Len(0),
				/* devDependency = */ false,
			).Times(2),
			handler.EXPECT().RegisterExecutionPlatforms(
				/* platformLabels = */ []label.Label{
					label.MustNewLabel("//:default_host_platform"),
					label.MustNewLabel("//:remote_linux_platform"),
				},
				/* devDependency = */ true,
			),
		)

		gomock.InOrder(
			handler.EXPECT().RegisterToolchains(
				/* toolchainLabels = */ gomock.Len(0),
				/* devDependency = */ false,
			).Times(2),
			handler.EXPECT().RegisterToolchains(
				/* toolchainLabels = */ []label.Label{
					label.MustNewLabel("@bazel_tools//tools/python:autodetecting_toolchain"),
					label.MustNewLabel("@local_config_winsdk//:all"),
				},
				/* devDependency = */ true,
			),
		)

		gomock.InOrder(
			handler.EXPECT().SingleVersionOverride(
				/* moduleName = */ label.MustNewModule("my_module_name"),
				/* version = */ nil,
				/* registry = */ nil,
				/* patchOptions = */ &pg_starlark.PatchOptions{},
			),
			handler.EXPECT().SingleVersionOverride(
				/* moduleName = */ label.MustNewModule("my_module_name"),
				/* version = */ nil,
				/* registry = */ nil,
				/* patchOptions = */ &pg_starlark.PatchOptions{
					Patches:   []label.Label{},
					PatchCmds: []string{},
				},
			),
			handler.EXPECT().SingleVersionOverride(
				/* moduleName = */ label.MustNewModule("my_module_name"),
				/* version = */ &version2,
				/* registry = */ registry,
				/* patchOptions = */ &pg_starlark.PatchOptions{
					Patches: []label.Label{
						label.MustNewLabel("//:patches/foo1.diff"),
						label.MustNewLabel("//:patches/foo2.diff"),
					},
					PatchCmds: []string{
						"ls -l",
						"rm -rf /",
					},
					PatchStrip: 3,
				},
			),
		)

		proxy1 := NewMockModuleExtensionProxy(ctrl)
		proxy2 := NewMockModuleExtensionProxy(ctrl)
		proxy3 := NewMockModuleExtensionProxy(ctrl)
		gomock.InOrder(
			handler.EXPECT().UseExtension(
				/* extensionBzlFile */ label.MustNewLabel("//:extensions.bzl"),
				/* extensionName */ "foo",
				/* devDependency */ false,
				/* isolate */ false,
			).Return(proxy1, nil),
			handler.EXPECT().UseExtension(
				/* extensionBzlFile */ label.MustNewLabel("//:extensions.bzl"),
				/* extensionName */ "foo",
				/* devDependency */ false,
				/* isolate */ false,
			).Return(proxy2, nil),
			handler.EXPECT().UseExtension(
				/* extensionBzlFile */ label.MustNewLabel("//:extensions.bzl"),
				/* extensionName */ "foo",
				/* devDependency */ true,
				/* isolate */ true,
			).Return(proxy3, nil),
			proxy3.EXPECT().Tag("foo", map[string]starlark.Value{}),
			proxy3.EXPECT().Tag("bar", map[string]starlark.Value{
				"baz": starlark.String("qux"),
			}),
		)

		gomock.InOrder(
			proxy1.EXPECT().UseRepo(map[label.ApparentRepo]label.ApparentRepo{}),
			proxy2.EXPECT().UseRepo(map[label.ApparentRepo]label.ApparentRepo{
				label.MustNewApparentRepo("a"): label.MustNewApparentRepo("a"),
				label.MustNewApparentRepo("b"): label.MustNewApparentRepo("b"),
				label.MustNewApparentRepo("c"): label.MustNewApparentRepo("c"),
				label.MustNewApparentRepo("d"): label.MustNewApparentRepo("e"),
				label.MustNewApparentRepo("f"): label.MustNewApparentRepo("g"),
				label.MustNewApparentRepo("h"): label.MustNewApparentRepo("i"),
			}),
		)

		proxy4 := NewMockRepoRuleProxy(ctrl)
		gomock.InOrder(
			handler.EXPECT().UseRepoRule(
				/* repoRuleBzlFile = */ label.MustNewLabel("@bazel_tools//tools/build_defs/repo:http.bzl"),
				/* repoRuleName = */ "http_archive",
			).Return(proxy4.Call, nil),
			proxy4.EXPECT().Call(
				/* name = */ label.MustNewApparentRepo("my_repo_name"),
				/* devDependency = */ false,
				/* attrs = */ map[string]starlark.Value{},
			).Times(2),
			proxy4.EXPECT().Call(
				/* name = */ label.MustNewApparentRepo("my_repo_name"),
				/* devDependency = */ true,
				/* attrs = */ map[string]starlark.Value{
					"sha256": starlark.String("345277dfc4bc0569927c92ee924c7c5483faad42b3004dd9bb5a6806214d44e7"),
				},
			),
		)

		require.NoError(t, pg_starlark.ParseModuleDotBazel(
			`
archive_override(
    "my_module_name",
    [
        "https://example.com/url1",
        "https://example.com/url2",
    ],
)
archive_override(
    "my_module_name",
    [
        "https://example.com/url1",
        "https://example.com/url2",
    ],
    "",
    "",
    [],
    [],
    0,
)
archive_override(
    module_name = "my_module_name",
    urls = [
        "https://example.com/url1",
        "https://example.com/url2",
    ],
    integrity = "sha384-oqVuAfXRKap7fdgcCY5uykM6+R9GqQ8K/uxy9rx7HNQlGYl1kPzQho1wx4JwY8wC",
    strip_prefix = "some/prefix",
    patches = [
        "//:patches/foo1.diff",
        "//:patches/foo2.diff",
    ],
    patch_cmds = [
        "ls -l",
        "rm -rf /",
    ],
    patch_strip = 3,
)

bazel_dep(
    "my_module_name",
)
bazel_dep(
    "my_module_name",
    "",
    -1,
    "",
    False,
)
bazel_dep(
    name = "my_module_name",
    version = "1.2.3",
    max_compatibility_level = 123,
    repo_name = "my_repo_name",
    dev_dependency = True,
)

git_override(
    "my_module_name",
    "https://github.com/my-project/my-project.git",
)
git_override(
    "my_module_name",
    "https://github.com/my-project/my-project.git",
    "",
    [],
    [],
    0,
    False,
    "",
)
git_override(
    module_name = "my_module_name",
    remote = "https://github.com/my-project/my-project.git",
    commit = "1368bebd5776a80ea3161a07dafe8beb7c8c144c",
    patches = [
        "//:patches/foo1.diff",
        "//:patches/foo2.diff",
    ],
    patch_cmds = [
        "ls -l",
        "rm -rf /",
    ],
    patch_strip = 3,
    init_submodules = True,
    strip_prefix = "some/prefix",
)

local_path_override(
    module_name = "my_module_name",
    path = "/some/path",
)

module(
    "my_module_name",
)
module(
    "my_module_name",
    "",
    0,
    "",
    [],
)
module(
    name = "my_module_name",
    version = "1.0.0",
    compatibility_level = 123,
    repo_name = "my_repo_name",
    bazel_compatibility = [">=6.4.0", "-7.0.0"],
)

multiple_version_override(
    "my_module_name",
    ["1.0.0", "1.2.0"],
)
multiple_version_override(
    "my_module_name",
    ["1.0.0", "1.2.0"],
    "",
)
multiple_version_override(
    module_name = "my_module_name",
    versions = ["1.0.0", "1.2.0"],
    registry = "https://raw.githubusercontent.com/my-org/bazel-central-registry/main/",
)

register_execution_platforms()
register_execution_platforms(dev_dependency = False)
register_execution_platforms(
    "//:default_host_platform",
    "//:remote_linux_platform",
    dev_dependency = True,
)

register_toolchains()
register_toolchains(dev_dependency = False)
register_toolchains(
    "@bazel_tools//tools/python:autodetecting_toolchain",
    "@local_config_winsdk//:all",
    dev_dependency = True,
)

single_version_override(
    "my_module_name",
)
single_version_override(
    "my_module_name",
    "",
    "",
    [],
    [],
    0,
)
single_version_override(
    module_name = "my_module_name",
    version = "1.0.0",
    registry = "https://raw.githubusercontent.com/my-org/bazel-central-registry/main/",
    patches = [
        "//:patches/foo1.diff",
        "//:patches/foo2.diff",
    ],
    patch_cmds = [
        "ls -l",
        "rm -rf /",
    ],
    patch_strip = 3,
)

proxy1 = use_extension(
    "//:extensions.bzl",
    "foo",
)
proxy2 = use_extension(
    "//:extensions.bzl",
    "foo",
    dev_dependency = False,
    isolate = False,
)
proxy3 = use_extension(
    extension_bzl_file = "//:extensions.bzl",
    extension_name = "foo",
    dev_dependency = True,
    isolate = True,
)
proxy3.foo()
proxy3.bar(baz = "qux")

use_repo(
    proxy1,
)
use_repo(
    proxy2,
    "a",
    "b",
    "c",
    d = "e",
    f = "g",
    h = "i",
)

http_archive = use_repo_rule("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
http_archive(
    name = "my_repo_name",
)
http_archive(
    name = "my_repo_name",
    dev_dependency = False,
)
http_archive(
    name = "my_repo_name",
    dev_dependency = True,
    sha256 = "345277dfc4bc0569927c92ee924c7c5483faad42b3004dd9bb5a6806214d44e7",
)
`,
			path.UNIXFormat,
			handler,
		))
	})

	t.Run("UnknownLocalPathFormat", func(t *testing.T) {
		// If the local path format is not known, we can't parse
		// local_path_override()'s path argument. In that case,
		// LocalPathOverride() should be called with path set to
		// nil.
		handler := NewMockRootModuleDotBazelHandler(ctrl)
		handler.EXPECT().LocalPathOverride(
			/* moduleName = */ label.MustNewModule("my_module_name"),
			/* path = */ nil,
		)

		require.NoError(t, pg_starlark.ParseModuleDotBazel(
			`
local_path_override(
    module_name = "my_module_name",
    path = "/some/path",
)
`,
			/* localPathFormat = */ nil,
			handler,
		))
	})
}
