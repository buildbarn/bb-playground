package label_test

import (
	"testing"

	"github.com/buildbarn/bb-playground/pkg/label"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonicalRepo(t *testing.T) {
	t.Run("GetModule", func(t *testing.T) {
		for input, output := range map[string]string{
			"com_github_buildbarn_bb_storage+":                    "com_github_buildbarn_bb_storage",
			"com_github_buildbarn_bb_storage+1.0":                 "com_github_buildbarn_bb_storage",
			"gazelle++go_deps+bazel_gazelle_go_repository_config": "gazelle",
		} {
			assert.Equal(t, output, label.MustNewCanonicalRepo(input).GetModuleInstance().GetModule().String())
		}
	})

	t.Run("GetModuleVersion", func(t *testing.T) {
		t.Run("WithVersion", func(t *testing.T) {
			for input, output := range map[string]string{
				"com_github_buildbarn_bb_storage+1.0":                       "1.0",
				"com_github_buildbarn_bb_storage+1.0-rc.3":                  "1.0-rc.3",
				"gazelle+0.39.1+go_deps+bazel_gazelle_go_repository_config": "0.39.1",
			} {
				moduleVersion, ok := label.MustNewCanonicalRepo(input).GetModuleInstance().GetModuleVersion()
				require.True(t, ok)
				assert.Equal(t, output, moduleVersion.String())
			}
		})

		t.Run("WithoutVersion", func(t *testing.T) {
			for _, input := range []string{
				"com_github_buildbarn_bb_storage+",
				"gazelle++go_deps+bazel_gazelle_go_repository_config",
			} {
				_, ok := label.MustNewCanonicalRepo(input).GetModuleInstance().GetModuleVersion()
				assert.False(t, ok)
			}
		})
	})

	t.Run("GetModuleExtension", func(t *testing.T) {
		t.Run("False", func(t *testing.T) {
			for _, input := range []string{
				"com_github_buildbarn_bb_storage+",
				"com_github_buildbarn_bb_storage+1.0",
			} {
				_, _, ok := label.MustNewCanonicalRepo(input).GetModuleExtension()
				assert.False(t, ok)
			}
		})

		t.Run("True", func(t *testing.T) {
			moduleExtension, apparentRepo, ok := label.MustNewCanonicalRepo("gazelle++go_deps+bazel_gazelle_go_repository_config").GetModuleExtension()
			require.True(t, ok)
			assert.Equal(t, "gazelle++go_deps", moduleExtension.String())
			assert.Equal(t, "bazel_gazelle_go_repository_config", apparentRepo.String())

			moduleExtension, apparentRepo, ok = label.MustNewCanonicalRepo("gazelle+0.39.1+go_deps+bazel_gazelle_go_repository_config").GetModuleExtension()
			require.True(t, ok)
			assert.Equal(t, "gazelle+0.39.1+go_deps", moduleExtension.String())
			assert.Equal(t, "bazel_gazelle_go_repository_config", apparentRepo.String())
		})
	})
}
