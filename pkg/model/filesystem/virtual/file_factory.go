package virtual

import (
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
)

type FileFactory interface {
	LookupFile(reference object.LocalReference, totalSizeBytes uint64, isExecutable bool) virtual.Leaf
}
