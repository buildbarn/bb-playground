package label_test

import (
	"testing"

	"github.com/buildbarn/bonanza/pkg/label"
	"github.com/stretchr/testify/assert"
)

func TestModuleVersion(t *testing.T) {
	t.Run("CompareTotalOrder", func(t *testing.T) {
		// All version number strings found in Bazel's
		// VersionTest.java and in the Semantic Versioning
		// specification, listed in sorted order.
		versions := [][]string{
			{"1.0-2"},
			{"1.0-3"},
			{"1.0--"},
			{"1.0----"},
			{"1.0-are"},
			{"1.0-pre", "1.0-pre+build-kek.lol"},
			{"1.0-pre.2"},
			{"1.0-pre.3"},
			{"1.0-pre.10"},
			{"1.0-pre.99"},
			{"1.0-pre.10a"},
			{"1.0-pre.2a"},
			{"1.0-pre.foo"},
			{"1.0-pre.patch.3"},
			{"1.0-pre.patch.4"},
			{"1.0", "1.0+build", "1.0+build-notpre", "1.0+build2", "1.0+build3"},
			{"1.0.0-0.3.7"},
			{"1.0.0-alpha", "1.0.0-alpha+001"},
			{"1.0.0-alpha.1"},
			{"1.0.0-alpha.beta"},
			{"1.0.0-beta", "1.0.0-beta+exp.sha.5114f85"},
			{"1.0.0-beta.2"},
			{"1.0.0-beta.11"},
			{"1.0.0-rc.1"},
			{"1.0.0-x.7.z.92"},
			{"1.0.0-x-y-z.--"},
			{"1.0.0", "1.0.0+20130313144700", "1.0.0+21AF26D3----117B344092BD"},
			{"1.0.1"},
			{"1.0.patch.2"},
			{"1.0.patch.3"},
			{"1.0.patch.10"},
			{"1.0.patch10"},
			{"1.0.patch3"},
			{"1.9"},
			{"1.9.0"},
			{"1.10.0"},
			{"1.11.0"},
			{"2.0"},
			{"2.0.0"},
			{"2.1.0"},
			{"2.1.1"},
			{"3.0"},
			{"4"},
			{"11.0"},
			{"a"},
			{"abc"},
			{"abd"},
		}
		for i := 0; i < len(versions); i++ {
			for _, si := range versions[i] {
				vi := label.MustNewModuleVersion(si)
				for j := 0; j < len(versions); j++ {
					for _, sj := range versions[j] {
						vj := label.MustNewModuleVersion(sj)
						if cmp := vi.Compare(vj); i < j {
							assert.Negative(t, cmp, "%s is not less than %s", vi, vj)
						} else if i > j {
							assert.Positive(t, cmp, "%s is not greater than %s", vi, vj)
						} else {
							assert.Zero(t, cmp, "%s is not equal to %s", vi, vj)
						}
					}
				}
			}
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		for _, version := range []string{
			// The empty version should be encoded as nil.
			"",
			// From Bazel's VersionTest.java.
			"-abc",
			"1_2",
			"ßážëł",
			"1.0-pre?",
			"1.0-pre///",
			"1..0",
			"1.0-pre..erp",
			// We don't permit leading zeros, as that would
			// cause different string representations to
			// have the same precedence.
			"1.01",
		} {
			_, err := label.NewModuleVersion(version)
			assert.ErrorContains(t, err, "module version must match ", version)
		}
	})
}
