package diff_test

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluekeyes/go-gitdiff/gitdiff"
	"github.com/buildbarn/bonanza/pkg/diff"
	"github.com/stretchr/testify/require"
)

func TestFindTextFragmentOffsets(t *testing.T) {
	for i := 1; i <= 3; i++ {
		t.Run(fmt.Sprintf("%04d", i), func(t *testing.T) {
			// Read the patch file.
			fp, err := os.Open(filepath.Join("testdata", fmt.Sprintf("%04d-patch", i)))
			require.NoError(t, err)
			files, _, err := gitdiff.Parse(fp)
			require.NoError(t, err)
			require.Len(t, files, 1)
			require.NoError(t, fp.Close())

			// Determine locations at which modifications should be
			// made to the input file.
			fragments := files[0].TextFragments
			fi, err := os.Open(filepath.Join("testdata", fmt.Sprintf("%04d-input", i)))
			require.NoError(t, err)
			offsetsBytes, err := diff.FindTextFragmentOffsetsBytes(fragments, bufio.NewReader(fi))
			require.NoError(t, err)
			log.Printf("%#v", offsetsBytes)

			_, err = fi.Seek(0, os.SEEK_SET)
			require.NoError(t, err)
			substituted := bytes.NewBuffer(nil)
			require.NoError(t, diff.ReplaceTextFragments(substituted, fi, fragments, offsetsBytes))

			expected, err := os.ReadFile(filepath.Join("testdata", fmt.Sprintf("%04d-output", i)))
			require.NoError(t, err)
			require.Equal(t, string(expected), string(substituted.Bytes()))
		})
	}
}
