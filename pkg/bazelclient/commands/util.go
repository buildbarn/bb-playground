package commands

import (
	"github.com/buildbarn/bb-playground/pkg/bazelclient/logging"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

func ValidateInsideWorkspace(logger logging.Logger, commandName string, workspacePath path.Parser) {
	if workspacePath == nil {
		logger.Fatalf("The %#v command is only supported from within a workspace (below a directory having a MODULE.bazel file)", commandName)
	}
}
