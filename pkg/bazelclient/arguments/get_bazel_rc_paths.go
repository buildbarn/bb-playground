package arguments

import (
	"path/filepath"
)

// BazelRCPath holds a path at which a bazelrc configuration file is
// stored, and whether the file is required to exist. All bazelrc files
// are bazelrc files are optional, except for the ones provided to the
// --bazelrc startup flag.
type BazelRCPath struct {
	Path     string
	Required bool
}

// GetBazelRCPaths returns the paths of the bazelrc configuration files
// that should be evaluated, based on the startup flags, the workspace
// path and the user's home directory. This logic is consistent with
// https://bazel.build/run/bazelrc#bazelrc-file-locations.
func GetBazelRCPaths(startupFlags *StartupFlags, workspacePath, homeDirectoryPath *string) []BazelRCPath {
	var paths []BazelRCPath
	if !startupFlags.IgnoreAllRcFiles {
		if startupFlags.SystemRc {
			paths = append(paths, BazelRCPath{
				Path:     "/etc/bazel.bazelrc",
				Required: false,
			})
		}
		if startupFlags.WorkspaceRc && workspacePath != nil {
			paths = append(paths, BazelRCPath{
				Path:     filepath.Join(*workspacePath, ".bazelrc"),
				Required: false,
			})
		}
		if startupFlags.HomeRc && homeDirectoryPath != nil {
			paths = append(paths, BazelRCPath{
				Path:     filepath.Join(*homeDirectoryPath, ".bazelrc"),
				Required: false,
			})
		}
		for _, path := range startupFlags.Bazelrc {
			if path == "/dev/null" {
				break
			}
			paths = append(paths, BazelRCPath{
				Path:     path,
				Required: true,
			})
		}
	}
	return paths
}
