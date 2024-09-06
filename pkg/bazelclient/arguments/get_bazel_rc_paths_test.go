package arguments_test

import (
	"testing"

	"github.com/buildbarn/bb-playground/pkg/bazelclient/arguments"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"
)

func TestGetBazelRCPaths(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		bazelRCPaths, err := arguments.GetBazelRCPaths(
			&arguments.StartupFlags{
				HomeRc:           true,
				IgnoreAllRcFiles: false,
				SystemRc:         true,
				WorkspaceRc:      true,
			},
			path.UNIXFormat,
			path.UNIXFormat.NewParser("/home/alice/myproject"),
			path.UNIXFormat.NewParser("/home/alice"),
			path.UNIXFormat.NewParser("/home/alice/myproject/some/package"),
		)
		require.NoError(t, err)
		require.Len(t, bazelRCPaths, 3)

		p, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
		require.NoError(t, path.Resolve(bazelRCPaths[0].Path, scopeWalker))
		require.Equal(t, "/etc/bazel.bazelrc", p.GetUNIXString())
		require.False(t, bazelRCPaths[0].Required)

		p, scopeWalker = path.EmptyBuilder.Join(path.VoidScopeWalker)
		require.NoError(t, path.Resolve(bazelRCPaths[1].Path, scopeWalker))
		require.Equal(t, "/home/alice/myproject/.bazelrc", p.GetUNIXString())
		require.False(t, bazelRCPaths[1].Required)

		p, scopeWalker = path.EmptyBuilder.Join(path.VoidScopeWalker)
		require.NoError(t, path.Resolve(bazelRCPaths[2].Path, scopeWalker))
		require.Equal(t, "/home/alice/.bazelrc", p.GetUNIXString())
		require.False(t, bazelRCPaths[2].Required)
	})

	t.Run("AdditionalBazelRCFiles", func(t *testing.T) {
		// Paths specified through --bazelrc should go at the
		// very end. It may be specified multiple times. Entries
		// after /dev/null should be ignored.
		bazelRCPaths, err := arguments.GetBazelRCPaths(
			&arguments.StartupFlags{
				Bazelrc: []string{
					"myconfig1",
					"myconfig2",
					"/dev/null",
					"myconfig3",
					"myconfig4",
				},
				HomeRc:           true,
				IgnoreAllRcFiles: false,
				SystemRc:         true,
				WorkspaceRc:      true,
			},
			path.UNIXFormat,
			path.UNIXFormat.NewParser("/home/alice/myproject"),
			path.UNIXFormat.NewParser("/home/alice"),
			path.UNIXFormat.NewParser("/home/alice/myproject/some/package"),
		)
		require.NoError(t, err)
		require.Len(t, bazelRCPaths, 5)

		p, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
		require.NoError(t, path.Resolve(bazelRCPaths[0].Path, scopeWalker))
		require.Equal(t, "/etc/bazel.bazelrc", p.GetUNIXString())
		require.False(t, bazelRCPaths[0].Required)

		p, scopeWalker = path.EmptyBuilder.Join(path.VoidScopeWalker)
		require.NoError(t, path.Resolve(bazelRCPaths[1].Path, scopeWalker))
		require.Equal(t, "/home/alice/myproject/.bazelrc", p.GetUNIXString())
		require.False(t, bazelRCPaths[1].Required)

		p, scopeWalker = path.EmptyBuilder.Join(path.VoidScopeWalker)
		require.NoError(t, path.Resolve(bazelRCPaths[2].Path, scopeWalker))
		require.Equal(t, "/home/alice/.bazelrc", p.GetUNIXString())
		require.False(t, bazelRCPaths[2].Required)

		p, scopeWalker = path.EmptyBuilder.Join(path.VoidScopeWalker)
		require.NoError(t, path.Resolve(bazelRCPaths[3].Path, scopeWalker))
		require.Equal(t, "/home/alice/myproject/some/package/myconfig1", p.GetUNIXString())
		require.True(t, bazelRCPaths[3].Required)

		p, scopeWalker = path.EmptyBuilder.Join(path.VoidScopeWalker)
		require.NoError(t, path.Resolve(bazelRCPaths[4].Path, scopeWalker))
		require.Equal(t, "/home/alice/myproject/some/package/myconfig2", p.GetUNIXString())
		require.True(t, bazelRCPaths[4].Required)
	})

	t.Run("IgnoreAllRCFiles", func(t *testing.T) {
		bazelRCPaths, err := arguments.GetBazelRCPaths(
			&arguments.StartupFlags{
				Bazelrc:          []string{"myconfig"},
				HomeRc:           true,
				IgnoreAllRcFiles: true,
				SystemRc:         true,
				WorkspaceRc:      true,
			},
			path.UNIXFormat,
			path.UNIXFormat.NewParser("/home/alice/myproject"),
			path.UNIXFormat.NewParser("/home/alice"),
			path.UNIXFormat.NewParser("/home/alice/myproject/some/package"),
		)
		require.NoError(t, err)
		require.Empty(t, bazelRCPaths)
	})
}
