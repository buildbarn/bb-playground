package search_test

import (
	"bytes"
	"testing"

	"github.com/buildbarn/bb-playground/pkg/search"
	"github.com/stretchr/testify/require"
)

func TestMultiSearchAndReplacer(t *testing.T) {
	t.Run("NoNeedles", func(t *testing.T) {
		searcher, err := search.NewMultiSearchAndReplacer(nil)
		require.NoError(t, err)

		src := bytes.NewBufferString("Hello world")
		dst := bytes.NewBuffer(nil)
		require.NoError(t, searcher.SearchAndReplace(dst, src, nil))
		require.Equal(t, "Hello world", dst.String())
	})

	t.Run("EmptyNeedle", func(t *testing.T) {
		// Don't permit using empty needles, as that could lead
		// to an infinite number of expansions.
		_, err := search.NewMultiSearchAndReplacer([][]byte{nil})
		require.EqualError(t, err, "needle at index 0 is empty")
	})

	t.Run("OverlappingNeedles", func(t *testing.T) {
		t.Run("Exact", func(t *testing.T) {
			_, err := search.NewMultiSearchAndReplacer([][]byte{
				[]byte("Hello"),
				[]byte("Hello"),
			})
			require.EqualError(t, err, "needle at index 0 is a prefix of needle at index 1")
		})

		t.Run("Prefix1", func(t *testing.T) {
			_, err := search.NewMultiSearchAndReplacer([][]byte{
				[]byte("Hello"),
				[]byte("Hello world"),
			})
			require.EqualError(t, err, "needle at index 0 is a prefix of needle at index 1")
		})

		t.Run("Prefix2", func(t *testing.T) {
			_, err := search.NewMultiSearchAndReplacer([][]byte{
				[]byte("Hello world"),
				[]byte("Hello"),
			})
			require.EqualError(t, err, "needle at index 1 is a prefix of needle at index 0")
		})

		t.Run("Suffix1", func(t *testing.T) {
			_, err := search.NewMultiSearchAndReplacer([][]byte{
				[]byte("world"),
				[]byte("Hello world"),
			})
			require.EqualError(t, err, "needle at index 0 is contained in needle at index 1")
		})

		t.Run("Suffix2", func(t *testing.T) {
			_, err := search.NewMultiSearchAndReplacer([][]byte{
				[]byte("Hello world"),
				[]byte("world"),
			})
			require.EqualError(t, err, "needle at index 1 is contained in needle at index 0")
		})

		t.Run("Contains1", func(t *testing.T) {
			_, err := search.NewMultiSearchAndReplacer([][]byte{
				[]byte("llo wor"),
				[]byte("Hello world"),
			})
			require.EqualError(t, err, "needle at index 0 is contained in needle at index 1")
		})

		t.Run("Contains2", func(t *testing.T) {
			_, err := search.NewMultiSearchAndReplacer([][]byte{
				[]byte("Hello world"),
				[]byte("llo wor"),
			})
			require.EqualError(t, err, "needle at index 1 is contained in needle at index 0")
		})
	})

	t.Run("SingleCharacter", func(t *testing.T) {
		searcher, err := search.NewMultiSearchAndReplacer([][]byte{
			[]byte("a"),
		})
		require.NoError(t, err)

		src := bytes.NewBufferString("ababbaababa")
		dst := bytes.NewBuffer(nil)
		require.NoError(t, searcher.SearchAndReplace(dst, src, [][]byte{
			[]byte("c"),
		}))
		require.Equal(t, "cbcbbccbcbc", dst.String())
	})

	t.Run("FailFunction", func(t *testing.T) {
		searcher, err := search.NewMultiSearchAndReplacer([][]byte{
			[]byte("hello"),
		})
		require.NoError(t, err)

		src := bytes.NewBufferString("hehehellololo")
		dst := bytes.NewBuffer(nil)
		require.NoError(t, searcher.SearchAndReplace(dst, src, [][]byte{
			[]byte("goodbye"),
		}))
		require.Equal(t, "hehegoodbyelolo", dst.String())
	})

	t.Run("PartialMatchAtEnd", func(t *testing.T) {
		searcher, err := search.NewMultiSearchAndReplacer([][]byte{
			[]byte("hello"),
		})
		require.NoError(t, err)

		src := bytes.NewBufferString("heaven and hell")
		dst := bytes.NewBuffer(nil)
		require.NoError(t, searcher.SearchAndReplace(dst, src, [][]byte{
			[]byte("goodbye"),
		}))
		require.Equal(t, "heaven and hell", dst.String())
	})
}
