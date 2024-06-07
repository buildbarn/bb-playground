package arguments_test

import (
	"testing"

	"github.com/buildbarn/bb-playground/pkg/bazelclient/arguments"
	"github.com/stretchr/testify/require"
)

func TestGetBazelRCPaths(t *testing.T) {
	t.Run("NoWorkspaceNoHomeDirectory", func(t *testing.T) {
		require.Equal(
			t,
			[]arguments.BazelRCPath{
				{Path: "/etc/bazel.bazelrc", Required: false},
			},
			arguments.GetBazelRCPaths(
				&arguments.StartupFlags{
					HomeRc:           true,
					IgnoreAllRcFiles: false,
					SystemRc:         true,
					WorkspaceRc:      true,
				},
				/* workspacePath = */ nil,
				/* homeDirectoryPath = */ nil,
			),
		)
	})

	t.Run("Default", func(t *testing.T) {
		workspacePath := "/home/alice/myproject"
		homeDirectoryPath := "/home/alice"
		require.Equal(
			t,
			[]arguments.BazelRCPath{
				{Path: "/etc/bazel.bazelrc", Required: false},
				{Path: "/home/alice/myproject/.bazelrc", Required: false},
				{Path: "/home/alice/.bazelrc", Required: false},
			},
			arguments.GetBazelRCPaths(
				&arguments.StartupFlags{
					HomeRc:           true,
					IgnoreAllRcFiles: false,
					SystemRc:         true,
					WorkspaceRc:      true,
				},
				&workspacePath,
				&homeDirectoryPath,
			),
		)
	})

	t.Run("AdditionalBazelRCFiles", func(t *testing.T) {
		// Paths specified through --bazelrc should go at the
		// very end. It may be specified multiple times. Entries
		// after /dev/null should be ignored.
		workspacePath := "/home/alice/myproject"
		homeDirectoryPath := "/home/alice"
		require.Equal(
			t,
			[]arguments.BazelRCPath{
				{Path: "/etc/bazel.bazelrc", Required: false},
				{Path: "/home/alice/myproject/.bazelrc", Required: false},
				{Path: "/home/alice/.bazelrc", Required: false},
				{Path: "myconfig1", Required: true},
				{Path: "myconfig2", Required: true},
			},
			arguments.GetBazelRCPaths(
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
				&workspacePath,
				&homeDirectoryPath,
			),
		)
	})

	t.Run("IgnoreAllRCFiles", func(t *testing.T) {
		workspacePath := "/home/alice/myproject"
		homeDirectoryPath := "/home/alice"
		require.Empty(t, arguments.GetBazelRCPaths(
			&arguments.StartupFlags{
				Bazelrc:          []string{"myconfig"},
				HomeRc:           true,
				IgnoreAllRcFiles: true,
				SystemRc:         true,
				WorkspaceRc:      true,
			},
			&workspacePath,
			&homeDirectoryPath,
		))
	})
}
