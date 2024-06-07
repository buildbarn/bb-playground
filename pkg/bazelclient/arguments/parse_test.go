package arguments_test

import (
	"testing"

	"github.com/buildbarn/bb-playground/pkg/bazelclient/arguments"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Run("NoArguments", func(t *testing.T) {
		// If no arguments are provided, Bazel defaults to
		// displaying help output.
		command, err := arguments.Parse(nil)
		require.NoError(t, err)
		require.Equal(t, &arguments.HelpCommand{
			HelpFlags: arguments.HelpFlags{
				HelpVerbosity: arguments.HelpVerbosity_Medium,
			},
		}, command)
	})

	t.Run("CommandNotRecognized", func(t *testing.T) {
		_, err := arguments.Parse([]string{
			"bquery",
			"--noinclude_aspects",
		})
		require.EqualError(t, err, "command \"bquery\" not recognized")
	})

	t.Run("Build", func(t *testing.T) {
		t.Run("KeepGoingLong", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"build",
				"--keep_going",
				"//...",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.BuildCommand{
				BuildFlags: arguments.BuildFlags{
					KeepGoing: true,
				},
				Arguments: []string{"//..."},
			}, command)
		})

		t.Run("KeepGoingLong0", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"build",
				"--keep_going=0",
				"//...",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.BuildCommand{
				Arguments: []string{"//..."},
			}, command)
		})

		t.Run("KeepGoingLong1", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"build",
				"--keep_going=1",
				"//...",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.BuildCommand{
				BuildFlags: arguments.BuildFlags{
					KeepGoing: true,
				},
				Arguments: []string{"//..."},
			}, command)
		})

		t.Run("KeepGoingLongFalse", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"build",
				"--keep_going=false",
				"//...",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.BuildCommand{
				Arguments: []string{"//..."},
			}, command)
		})

		t.Run("KeepGoingLongTrue", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"build",
				"--keep_going=true",
				"//...",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.BuildCommand{
				BuildFlags: arguments.BuildFlags{
					KeepGoing: true,
				},
				Arguments: []string{"//..."},
			}, command)
		})

		t.Run("KeepGoingLongNo", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"build",
				"--keep_going=no",
				"//...",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.BuildCommand{
				Arguments: []string{"//..."},
			}, command)
		})

		t.Run("KeepGoingLongYes", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"build",
				"--keep_going=yes",
				"//...",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.BuildCommand{
				BuildFlags: arguments.BuildFlags{
					KeepGoing: true,
				},
				Arguments: []string{"//..."},
			}, command)
		})

		t.Run("KeepGoingLongOther", func(t *testing.T) {
			_, err := arguments.Parse([]string{
				"build",
				"--keep_going=maybe",
				"//...",
			})
			require.EqualError(t, err, "flag --keep_going only accepts \"true\", \"false\", \"yes\", \"no\", \"1\" or \"0\", not \"maybe\"")
		})

		t.Run("KeepGoingShort", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"build",
				"-k",
				"//...",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.BuildCommand{
				BuildFlags: arguments.BuildFlags{
					KeepGoing: true,
				},
				Arguments: []string{"//..."},
			}, command)
		})

		t.Run("NoKeepGoingLong", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"build",
				"-k",
				"--nokeep_going",
				"//...",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.BuildCommand{
				Arguments: []string{"//..."},
			}, command)
		})

		t.Run("NoKeepGoingShort", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"build",
				"-k",
				"-k-",
				"//...",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.BuildCommand{
				Arguments: []string{"//..."},
			}, command)
		})

		t.Run("NoKeepGoingUnexpectedValue", func(t *testing.T) {
			_, err := arguments.Parse([]string{
				"build",
				"-k",
				"--nokeep_going=123",
				"//...",
			})
			require.EqualError(t, err, "flag --nokeep_going does not take a value")
		})

		t.Run("PositiveNegativePatterns", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"build",
				"--",
				"//...",
				"-//foo/...",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.BuildCommand{
				Arguments: []string{
					"//...",
					"-//foo/...",
				},
			}, command)
		})
	})

	t.Run("Clean", func(t *testing.T) {
		t.Run("HelpVerbosityNotApplicable", func(t *testing.T) {
			_, err := arguments.Parse([]string{
				"clean",
				"--help_verbosity",
				"short",
			})
			require.EqualError(t, err, "flag --help_verbosity does not apply to this command")
		})

		t.Run("ShortNotApplicable", func(t *testing.T) {
			_, err := arguments.Parse([]string{
				"clean",
				"--short",
			})
			require.EqualError(t, err, "flag --short does not apply to this command")
		})
	})

	t.Run("Help", func(t *testing.T) {
		t.Run("NoFlags", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"help",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.HelpCommand{
				HelpFlags: arguments.HelpFlags{
					HelpVerbosity: arguments.HelpVerbosity_Medium,
				},
			}, command)
		})

		t.Run("Short", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"help",
				"--short",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.HelpCommand{
				HelpFlags: arguments.HelpFlags{
					HelpVerbosity: arguments.HelpVerbosity_Short,
				},
			}, command)
		})

		t.Run("ShortUnexpectedValue", func(t *testing.T) {
			_, err := arguments.Parse([]string{
				"help",
				"--short=123",
			})
			require.EqualError(t, err, "flag --short does not take a value")
		})

		t.Run("DashL", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"help",
				"-l",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.HelpCommand{
				HelpFlags: arguments.HelpFlags{
					HelpVerbosity: arguments.HelpVerbosity_Long,
				},
			}, command)
		})

		t.Run("HelpVerbosity", func(t *testing.T) {
			t.Run("Equals", func(t *testing.T) {
				command, err := arguments.Parse([]string{
					"help",
					"--help_verbosity=short",
				})
				require.NoError(t, err)
				require.Equal(t, &arguments.HelpCommand{
					HelpFlags: arguments.HelpFlags{
						HelpVerbosity: arguments.HelpVerbosity_Short,
					},
				}, command)
			})

			t.Run("Space", func(t *testing.T) {
				command, err := arguments.Parse([]string{
					"help",
					"--help_verbosity",
					"long",
				})
				require.NoError(t, err)
				require.Equal(t, &arguments.HelpCommand{
					HelpFlags: arguments.HelpFlags{
						HelpVerbosity: arguments.HelpVerbosity_Long,
					},
				}, command)
			})

			t.Run("MissingValue", func(t *testing.T) {
				_, err := arguments.Parse([]string{
					"help",
					"--help_verbosity",
				})
				require.EqualError(t, err, "flag --help_verbosity expects a value")
			})

			t.Run("UnknownValue", func(t *testing.T) {
				_, err := arguments.Parse([]string{
					"help",
					"--help_verbosity",
					"large",
				})
				require.EqualError(t, err, "flag --help_verbosity only accepts \"long\", \"medium\" or \"short\", not \"large\"")
			})
		})

		t.Run("WithCommandName", func(t *testing.T) {
			command, err := arguments.Parse([]string{
				"help",
				"build",
			})
			require.NoError(t, err)
			require.Equal(t, &arguments.HelpCommand{
				HelpFlags: arguments.HelpFlags{
					HelpVerbosity: arguments.HelpVerbosity_Medium,
				},
				Arguments: []string{"build"},
			}, command)
		})
	})
}
