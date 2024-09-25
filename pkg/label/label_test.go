package label_test

import (
	"testing"

	"github.com/buildbarn/bb-playground/pkg/label"
	"github.com/stretchr/testify/require"
)

func TestLabel(t *testing.T) {
	t.Run("GetCanonicalRepo", func(t *testing.T) {
		for _, labelString := range []string{
			"//hello",
			"@rules_go//hello",
		} {
			_, ok := label.MustNewLabel(labelString).GetCanonicalRepo()
			require.False(t, ok)
		}

		for from, to := range map[string]string{
			"@@rules_go+//go/toolchain:netbsd":                                   "rules_go+",
			"@@rules_python~~python~python_3_11_aarch64-apple-darwin//:includes": "rules_python~~python~python_3_11_aarch64-apple-darwin",
		} {
			canonicalRepo, ok := label.MustNewLabel(from).GetCanonicalRepo()
			require.True(t, ok)
			require.Equal(t, to, canonicalRepo.String())
		}
	})

	t.Run("GetPackageLabel", func(t *testing.T) {
		for from, to := range map[string]string{
			"//cmd/hello_world:go_default_library": "//cmd/hello_world",
			"//:gazelle":                           "//",
			"@rules_go//go/platform:linux_arm64":   "@rules_go//go/platform",
			"@rules_go//:stdlib":                   "@rules_go",
		} {
			l := label.MustNewLabel(from)
			require.Equal(t, to, l.GetPackageLabel().String())
			require.Equal(t, to, l.GetPackageLabel().GetPackageLabel().String())
		}
	})

	t.Run("GetPackagePath", func(t *testing.T) {
		for _, labelString := range []string{
			"hello_world",
			":hello_world",
			"hello_world:hello_world",
		} {
			_, ok := label.MustNewLabel(labelString).GetPackagePath()
			require.False(t, ok)
		}

		for from, to := range map[string]string{
			"//cmd/hello_world":                    "cmd/hello_world",
			"//cmd/hello_world:go_default_library": "cmd/hello_world",
			"//:gazelle":                           "",
			"@rules_go//go/platform:linux_arm64":   "go/platform",
			"@rules_go":                            "",
			"@rules_go//:stdlib":                   "",
		} {
			packagePath, ok := label.MustNewLabel(from).GetPackagePath()
			require.True(t, ok)
			require.Equal(t, to, packagePath)
		}
	})
}
