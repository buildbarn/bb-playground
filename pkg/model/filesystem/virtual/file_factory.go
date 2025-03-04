package virtual

import (
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	model_filesystem "github.com/buildbarn/bonanza/pkg/model/filesystem"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

type FileFactory interface {
	LookupFile(fileContents model_filesystem.FileContentsEntry[object.LocalReference], isExecutable bool) virtual.LinkableLeaf
}
