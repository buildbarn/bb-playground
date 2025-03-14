package glob_test

import (
	"testing"

	"github.com/buildbarn/bonanza/pkg/glob"
	"github.com/stretchr/testify/require"
)

func TestCompile(t *testing.T) {
	t.Run("Failure", func(t *testing.T) {
		t.Run("Empty", func(t *testing.T) {
			_, err := glob.Compile([]string{""}, nil)
			require.EqualError(t, err, "invalid \"includes\" pattern \"\": pathname components cannot be empty")
		})

		t.Run("TooManyStars", func(t *testing.T) {
			_, err := glob.Compile([]string{"***"}, nil)
			require.EqualError(t, err, "invalid \"includes\" pattern \"***\": \"***\" has no special meaning")
		})

		t.Run("StarStarWithinFilename1", func(t *testing.T) {
			_, err := glob.Compile([]string{"a**"}, nil)
			require.EqualError(t, err, "invalid \"includes\" pattern \"a**\": \"**\" can not be placed inside a component")
		})

		t.Run("StarStarWithinFilename2", func(t *testing.T) {
			_, err := glob.Compile([]string{"**a"}, nil)
			require.EqualError(t, err, "invalid \"includes\" pattern \"**a\": \"**\" can not be placed inside a component")
		})

		t.Run("RedundantStarStarsMiddle", func(t *testing.T) {
			_, err := glob.Compile([]string{"a/**/**/b"}, nil)
			require.EqualError(t, err, "invalid \"includes\" pattern \"a/**/**/b\": redundant \"**\"")
		})

		t.Run("RedundantStarStarsEnd", func(t *testing.T) {
			_, err := glob.Compile([]string{"a/**/**"}, nil)
			require.EqualError(t, err, "invalid \"includes\" pattern \"a/**/**\": redundant \"**\"")
		})
	})

	t.Run("Success", func(t *testing.T) {
		t.Run("Empty", func(t *testing.T) {
			nfa, err := glob.Compile(nil, nil)
			require.NoError(t, err)
			require.Equal(t, []byte{0x80}, nfa)
		})

		t.Run("LiteralFilename", func(t *testing.T) {
			nfa, err := glob.Compile([]string{"hello"}, nil)
			require.NoError(t, err)
			require.Equal(t, []byte{
				0x90, 'h',
				0x90, 'e',
				0x90, 'l',
				0x90, 'l',
				0x90, 'o',
				0x81,
			}, nfa)
		})

		t.Run("Star", func(t *testing.T) {
			nfa, err := glob.Compile([]string{"*"}, nil)
			require.NoError(t, err)
			require.Equal(t, []byte{
				0x84,
				0x81,
			}, nfa)
		})

		t.Run("MultipleStars", func(t *testing.T) {
			nfa, err := glob.Compile([]string{"*a*b*c*"}, nil)
			require.NoError(t, err)
			require.Equal(t, []byte{
				0x84, // "*".
				0x90, 'a',
				0x84, // "*a*".
				0x90, 'b',
				0x84, // "*a*b*".
				0x90, 'c',
				0x84, // "*a*b*c*".
				0x81,
			}, nfa)
		})

		t.Run("MultipleStarStars", func(t *testing.T) {
			nfa, err := glob.Compile([]string{"a/**/b/**/c"}, nil)
			require.NoError(t, err)
			require.Equal(t, []byte{
				0x90, 'a',
				0x90, '/',
				0x88, // "a/**/".
				0x90, 'b',
				0x90, '/',
				0x88, // "a/**/b/**/".
				0x90, 'c',
				0x81,
			}, nfa)
		})

		t.Run("StarStar", func(t *testing.T) {
			// We only provide a state transition for "**/",
			// not a bare "**". Solve this by converting it
			// to "**/*. For consistency with Bazel, the
			// root directory should not be matched.
			nfa, err := glob.Compile([]string{"**"}, nil)
			require.NoError(t, err)
			require.Equal(t, []byte{
				0x88, // "**/".
				0x84, // "**/*".
				0x81,
			}, nfa)
		})

		t.Run("StarStarAtEnd", func(t *testing.T) {
			// If a pattern ends with "/**", we translate it
			// to "/**/*". For consistency with Bazel, the
			// final directory should also be matched.
			nfa, err := glob.Compile([]string{"hello/**"}, nil)
			require.NoError(t, err)
			require.Equal(t, []byte{
				0x90, 'h',
				0x90, 'e',
				0x90, 'l',
				0x90, 'l',
				0x90, 'o',
				0x91, '/',
				0x88, // "hello/**/".
				0x84, // "hello/**/*".
				0x81,
			}, nfa)
		})

		t.Run("Prefix", func(t *testing.T) {
			nfa, err := glob.Compile([]string{
				"good",
				"goodbye",
			}, nil)
			require.NoError(t, err)
			require.Equal(t, []byte{
				0x90, 'g',
				0x90, 'o',
				0x90, 'o',
				0x90, 'd',
				0x91, 'b',
				0x90, 'y',
				0x90, 'e',
				0x81,
			}, nfa)
		})

		t.Run("Branch", func(t *testing.T) {
			nfa, err := glob.Compile(
				[]string{"goodbye"},
				[]string{"good work"},
			)
			require.NoError(t, err)
			require.Equal(t, []byte{
				0x90, 'g',
				0x90, 'o',
				0x90, 'o',
				0x90, 'd',
				0xa0, ' ', 'b',
				0x90, 'w',
				0x90, 'y',
				0x90, 'o',
				0x90, 'e',
				0x90, 'r',
				0x81, // End of "goodbye".
				0x90, 'k',
				0x82, // End of "good work".
			}, nfa)
		})
	})
}
