package arguments_test

import (
	"testing"

	"github.com/buildbarn/bonanza/pkg/bazelclient/arguments"
	"github.com/stretchr/testify/require"
)

func TestParseStartupFlags(t *testing.T) {
	t.Run("NoArguments", func(t *testing.T) {
		// If no arguments are provided, Bazel should look for
		// bazelrc files in all the regular locations.
		flags, argsConsumed, err := arguments.ParseStartupFlags(nil)
		require.NoError(t, err)
		require.Equal(t, &arguments.StartupFlags{
			HomeRc:           true,
			IgnoreAllRcFiles: false,
			SystemRc:         true,
			WorkspaceRc:      true,
		}, flags)
		require.Equal(t, 0, argsConsumed)
	})

	t.Run("NoStartupFlags", func(t *testing.T) {
		// Flag parsing should stop once a command name is
		// encountered.
		flags, argsConsumed, err := arguments.ParseStartupFlags([]string{
			"build",
			"--keep_going",
			"//my/package/...",
		})
		require.NoError(t, err)
		require.Equal(t, &arguments.StartupFlags{
			HomeRc:           true,
			IgnoreAllRcFiles: false,
			SystemRc:         true,
			WorkspaceRc:      true,
		}, flags)
		require.Equal(t, 0, argsConsumed)
	})

	t.Run("UnknownFlagName", func(t *testing.T) {
		_, _, err := arguments.ParseStartupFlags([]string{
			"--blazerc=/home/bob/.blazerc",
			"build",
			"//my/package/...",
		})
		require.EqualError(t, err, "flag --blazerc not recognized")
	})

	t.Run("MissingArgument", func(t *testing.T) {
		_, _, err := arguments.ParseStartupFlags([]string{
			"--bazelrc",
		})
		require.EqualError(t, err, "flag --bazelrc expects a value")
	})

	t.Run("ValidFlags", func(t *testing.T) {
		flags, argsConsumed, err := arguments.ParseStartupFlags([]string{
			"--bazelrc",
			"/home/alice/.bazelrc",
			"--bazelrc=/home/bob/.bazelrc",
			"--home_rc=1",
			"--nosystem_rc",
			"--workspace_rc=0",
			"test",
			"//...",
		})
		require.NoError(t, err)
		require.Equal(t, &arguments.StartupFlags{
			Bazelrc: []string{
				"/home/alice/.bazelrc",
				"/home/bob/.bazelrc",
			},
			HomeRc:           true,
			IgnoreAllRcFiles: false,
			SystemRc:         false,
			WorkspaceRc:      false,
		}, flags)
		require.Equal(t, 6, argsConsumed)
	})
}
