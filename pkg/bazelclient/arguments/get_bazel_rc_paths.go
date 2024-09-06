package arguments

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// BazelRCPath holds a path at which a bazelrc configuration file is
// stored, and whether the file is required to exist. All bazelrc files
// are bazelrc files are optional, except for the ones provided to the
// --bazelrc startup flag.
type BazelRCPath struct {
	Path     path.Parser
	Required bool
}

var bazelRCFilename = path.UNIXFormat.NewParser(".bazelrc")

// GetBazelRCPaths returns the paths of the bazelrc configuration files
// that should be evaluated, based on the startup flags, the workspace
// path and the user's home directory. This logic is consistent with
// https://bazel.build/run/bazelrc#bazelrc-file-locations.
func GetBazelRCPaths(startupFlags *StartupFlags, startupFlagsPathFormat path.Format, workspacePath, homeDirectoryPath, workingDirectoryPath path.Parser) ([]BazelRCPath, error) {
	var paths []BazelRCPath
	if !startupFlags.IgnoreAllRcFiles {
		if startupFlags.SystemRc {
			paths = append(paths, BazelRCPath{
				Path:     path.UNIXFormat.NewParser("/etc/bazel.bazelrc"),
				Required: false,
			})
		}
		if startupFlags.WorkspaceRc && workspacePath != nil {
			bazelRCPath, scopeWalker := path.EmptyBuilder.Join(path.NewAbsoluteScopeWalker(path.VoidComponentWalker))
			if err := path.Resolve(workspacePath, scopeWalker); err != nil {
				return nil, err
			}
			bazelRCPath, scopeWalker = bazelRCPath.Join(path.VoidScopeWalker)
			if err := path.Resolve(bazelRCFilename, scopeWalker); err != nil {
				return nil, err
			}
			paths = append(paths, BazelRCPath{
				Path:     bazelRCPath,
				Required: false,
			})
		}
		if startupFlags.HomeRc {
			bazelRCPath, scopeWalker := path.EmptyBuilder.Join(path.NewAbsoluteScopeWalker(path.VoidComponentWalker))
			if err := path.Resolve(homeDirectoryPath, scopeWalker); err != nil {
				return nil, err
			}
			bazelRCPath, scopeWalker = bazelRCPath.Join(path.VoidScopeWalker)
			if err := path.Resolve(bazelRCFilename, scopeWalker); err != nil {
				return nil, err
			}
			paths = append(paths, BazelRCPath{
				Path:     bazelRCPath,
				Required: false,
			})
		}
		for _, flag := range startupFlags.Bazelrc {
			if flag == "/dev/null" {
				break
			}
			bazelRCPath, scopeWalker := path.EmptyBuilder.Join(path.NewAbsoluteScopeWalker(path.VoidComponentWalker))
			if err := path.Resolve(workingDirectoryPath, scopeWalker); err != nil {
				return nil, err
			}
			bazelRCPath, scopeWalker = bazelRCPath.Join(path.VoidScopeWalker)
			if err := path.Resolve(startupFlagsPathFormat.NewParser(flag), scopeWalker); err != nil {
				return nil, err
			}
			paths = append(paths, BazelRCPath{
				Path:     bazelRCPath,
				Required: true,
			})
		}
	}
	return paths, nil
}
