package label_test

import (
	"testing"

	"github.com/buildbarn/bb-playground/pkg/label"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonicalPackage(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		for _, input := range []string{
			"@@com_github_buildbarn_bb_storage+",
			"@@com_github_buildbarn_bb_storage+//cmd",
			"@@com_github_buildbarn_bb_storage+//cmd/hello_world",
			`@@com_github_buildbarn_bb_storage+//cmd/! "#$%&'()*+,-.;<=>?@[]^_{|}` + "`",
		} {
			canonicalPackage := label.MustNewCanonicalPackage(input)
			assert.Equal(t, input, canonicalPackage.String())
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		for _, input := range []string{
			"",
			"hello",
			"//cmd/hello_world",
			"@repo//cmd/hello_world",
			"@//cmd/hello_world",
			"@@//cmd/hello_world",
			"@@com_github_buildbarn_bb_storage+//",
			"@@com_github_buildbarn_bb_storage+//cmd/hello_world:target",
			"@@com_github_buildbarn_bb_storage+:target",
			"@@com_github_buildbarn_bb_storage+//cmd//hello_world",
			"@@com_github_buildbarn_bb_storage+//cmd/./hello_world",
			"@@com_github_buildbarn_bb_storage+//cmd/../hello_world",
			"@@com_github_buildbarn_bb_storage+//cmd/.../hello_world",
			"@@com_github_buildbarn_bb_storage+//cmd/..../hello_world",
			"@@com_github_buildbarn_bb_storage+///cmd/hello_world",
			"@@com_github_buildbarn_bb_storage+//cmd/hello_world/",
		} {
			_, err := label.NewCanonicalPackage(input)
			assert.ErrorContains(t, err, "canonical package name must match ", input)
		}
	})

	t.Run("GetCanonicalRepo", func(t *testing.T) {
		for _, input := range []string{
			"@@com_github_buildbarn_bb_storage+",
			"@@com_github_buildbarn_bb_storage+//cmd",
			"@@com_github_buildbarn_bb_storage+//cmd/hello_world",
		} {
			canonicalPackage := label.MustNewCanonicalPackage(input)
			assert.Equal(t, "com_github_buildbarn_bb_storage+", canonicalPackage.GetCanonicalRepo().String())
		}
	})

	t.Run("GetPackagePath", func(t *testing.T) {
		for input, output := range map[string]string{
			"@@com_github_buildbarn_bb_storage+":                  "",
			"@@com_github_buildbarn_bb_storage+//cmd":             "cmd",
			"@@com_github_buildbarn_bb_storage+//cmd/hello_world": "cmd/hello_world",
		} {
			canonicalPackage := label.MustNewCanonicalPackage(input)
			assert.Equal(t, output, canonicalPackage.GetPackagePath())
		}
	})

	t.Run("AppendLabel", func(t *testing.T) {
		t.Run("AtRoot", func(t *testing.T) {
			base := label.MustNewCanonicalPackage("@@example+")
			for input, output := range map[string]string{
				":foo":         "@@example+//:foo",
				"bar:wiz":      "@@example+//bar:wiz",
				"bar/wiz":      "@@example+//:bar/wiz",
				"bar:all":      "@@example+//bar:all",
				":all":         "@@example+//:all",
				"//baz":        "@@example+//baz",
				"//:example+":  "@@example+",
				"@foo/bar/baz": "@@example+//:@foo/bar/baz",
				"@foo:@bar":    "@@example+//@foo:@bar",
			} {
				newLabel, err := base.AppendLabel(input)
				require.NoError(t, err)
				assert.Equal(t, output, newLabel.String())
			}
		})
		t.Run("InsidePackage", func(t *testing.T) {
			base := label.MustNewCanonicalPackage("@@example+//foo")
			for input, output := range map[string]string{
				":foo":                "@@example+//foo",
				"bar:wiz":             "@@example+//foo/bar:wiz",
				"bar/wiz":             "@@example+//foo:bar/wiz",
				"bar:all":             "@@example+//foo/bar:all",
				":all":                "@@example+//foo:all",
				"//baz":               "@@example+//baz",
				"//:example+":         "@@example+",
				"@@other1+":           "@@other1+",
				"@@other1+//:other1+": "@@other1+",
				"@@other1+//:foo":     "@@other1+//:foo",
				"@other2":             "@other2",
				"@other2//:other2":    "@other2",
				"@other2//:foo":       "@other2//:foo",
			} {
				newLabel, err := base.AppendLabel(input)
				require.NoError(t, err)
				assert.Equal(t, output, newLabel.String())
			}
		})
	})
}
