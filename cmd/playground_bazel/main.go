package main

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/buildbarn/bb-playground/pkg/bazelclient/arguments"
	commands_build "github.com/buildbarn/bb-playground/pkg/bazelclient/commands/build"
	commands_info "github.com/buildbarn/bb-playground/pkg/bazelclient/commands/info"
	commands_license "github.com/buildbarn/bb-playground/pkg/bazelclient/commands/license"
	commands_version "github.com/buildbarn/bb-playground/pkg/bazelclient/commands/version"
	"github.com/buildbarn/bb-playground/pkg/bazelclient/logging"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

func main() {
	// Logger we need to use before flags have been parsed. As
	// enabling/disabling colors is controlled via a flag, leave
	// colors disabled.
	startupLogger := logging.NewConsoleLogger(os.Stderr, &logging.NoEscapeSequences)

	rootDirectory, err := filesystem.NewLocalDirectory(&path.RootBuilder)
	if err != nil {
		startupLogger.Fatal("Failed to open root directory: ", err)
	}

	homeDirectory, err := os.UserHomeDir()
	if err != nil {
		startupLogger.Fatal("Failed to obtain user home directory:", err)
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		startupLogger.Fatal("Failed to obtain working directory: ", err)
	}

	var workspacePath path.Parser
	workspacePathStr := workingDirectory
	for {
		moduleBazelPath := filepath.Join(workspacePathStr, "MODULE.bazel")
		if _, err := os.Stat(moduleBazelPath); err == nil {
			workspacePath = path.LocalFormat.NewParser(workspacePathStr)
			break
		} else if !errors.Is(err, fs.ErrNotExist) {
			startupLogger.Fatal("Failed to obtain workspace path: ", err)
		}
		parent := filepath.Dir(workspacePathStr)
		if parent == workspacePathStr {
			break
		}
		workspacePathStr = parent
	}

	cmd, err := arguments.Parse(
		os.Args[1:],
		rootDirectory,
		path.LocalFormat,
		workspacePath,
		path.LocalFormat.NewParser(homeDirectory),
		path.LocalFormat.NewParser(workingDirectory),
	)
	if err != nil {
		startupLogger.Fatal(err)
	}

	switch typedCmd := cmd.(type) {
	case *arguments.BuildCommand:
		commands_build.DoBuild(typedCmd, workspacePath)
	case *arguments.HelpCommand:
		panic("HELP")
	case *arguments.InfoCommand:
		commands_info.DoInfo(typedCmd, workspacePath)
	case *arguments.LicenseCommand:
		commands_license.DoLicense()
	case *arguments.VersionCommand:
		commands_version.DoVersion(typedCmd)
	default:
		panic("unknown command type")
	}
}
