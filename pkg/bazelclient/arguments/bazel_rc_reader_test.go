package arguments_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/buildbarn/bb-playground/pkg/bazelclient/arguments"
	"github.com/stretchr/testify/require"
)

func TestBazelRCReader(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		r := arguments.NewBazelRCReader(bytes.NewBuffer(nil))

		_, err := r.Read()
		require.Equal(t, io.EOF, err)
	})

	t.Run("Example", func(t *testing.T) {
		r := arguments.NewBazelRCReader(bytes.NewBufferString(`
build -j 300

# Some comment.
build:debug -c dbg

test --test_env=FOO=bar \
     --test_env=BAZ=qux \
     --test_env=A=b\
c

'' "" # Test empty strings.

hello#world
Single\ field\ with\ spaces

# Even though the Bazel documentation states that quoting is performed
# according to Bourne shell parsing rules, it seems single and double
# quote strings are parsed the same way.
build --action_env=SINGLE_QUOTE='\'' --action_env=DOUBLE_QUOTE="\""
`))

		record, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, []string{
			"build",
			"-j",
			"300",
		}, record)

		record, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []string{
			"build:debug",
			"-c",
			"dbg",
		}, record)

		record, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []string{
			"test",
			"--test_env=FOO=bar",
			"--test_env=BAZ=qux",
			"--test_env=A=bc",
		}, record)

		record, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []string{
			"",
			"",
		}, record)

		record, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []string{
			"hello",
		}, record)

		record, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []string{
			"Single field with spaces",
		}, record)

		record, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, []string{
			"build",
			"--action_env=SINGLE_QUOTE='",
			"--action_env=DOUBLE_QUOTE=\"",
		}, record)

		_, err = r.Read()
		require.Equal(t, io.EOF, err)
	})
}
