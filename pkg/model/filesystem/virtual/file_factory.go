package virtual

import (
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
)

type FileFactory interface {
	LookupFile(fileContents model_filesystem.FileContentsEntry, isExecutable bool) virtual.LinkableLeaf
}
