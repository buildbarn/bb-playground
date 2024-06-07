package object_test

import (
	"testing"

	object_pb "github.com/buildbarn/bb-playground/pkg/proto/storage/object"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/stretchr/testify/require"
)

func TestReferenceFormat(t *testing.T) {
	referenceFormat := object.MustNewReferenceFormat(object_pb.ReferenceFormat_SHA256_V1)

	t.Run("GetBogusReference", func(t *testing.T) {
		// GetBogusReference() must return a reference that can
		// be copied and reconstructed if needed.
		bogusReference1 := referenceFormat.GetBogusReference()
		bogusReference2, err := referenceFormat.NewLocalReference(bogusReference1.GetRawReference())
		require.NoError(t, err)
		require.Equal(t, bogusReference1, bogusReference2)
	})
}
