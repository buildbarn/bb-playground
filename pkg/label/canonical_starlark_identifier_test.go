package label_test

import (
	"testing"

	"github.com/buildbarn/bb-playground/pkg/label"
	"github.com/stretchr/testify/require"
)

func TestCanonicalStarlarkIdentifier(t *testing.T) {
	t.Run("ToModuleExtension", func(t *testing.T) {
		for _, input := range []string{
			"@@bazel_features+//private:extensions.bzl%version_extension",
			"@@bazel_features+//private%version_extension",
			"@@bazel_features++foo+bar//private:extensions.bzl%version_extension",
		} {
			require.Equal(
				t,
				"bazel_features++version_extension",
				label.MustNewCanonicalStarlarkIdentifier(input).
					ToModuleExtension().
					String(),
			)
		}
	})
}
