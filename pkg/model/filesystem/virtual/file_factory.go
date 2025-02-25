package virtual

import (
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
)

type FileFactory interface {
	LookupFile(fileContents model_filesystem.FileContentsEntry, isExecutable bool) virtual.LinkableLeaf
}
