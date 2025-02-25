package commands

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bonanza/pkg/bazelclient/logging"
)

func ValidateInsideWorkspace(logger logging.Logger, commandName string, workspacePath path.Parser) {
	if workspacePath == nil {
		logger.Fatalf("The %#v command is only supported from within a workspace (below a directory having a MODULE.bazel file)", commandName)
	}
}
