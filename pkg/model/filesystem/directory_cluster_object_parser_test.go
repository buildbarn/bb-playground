package filesystem_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/testutil"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDirectoryClusterObjectParser(t *testing.T) {
	objectParser := model_filesystem.NewDirectoryClusterObjectParser[object.LocalReference]()

	t.Run("InvalidMessage", func(t *testing.T) {
		_, _, err := objectParser.ParseObject(
			model_core.NewSimpleMessage[object.LocalReference](
				[]byte("Not a valid Protobuf message"),
			),
		)
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Failed to parse directory: "), err)
	})

	// TODO: Add more testing coverage.
}
