package unpack

import (
	"fmt"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"go.starlark.net/starlark"
)

func PathParser(pathFormat path.Format) UnpackerInto[path.Parser] {
	return func(thread *starlark.Thread, v starlark.Value, dst *path.Parser) error {
		s, ok := starlark.AsString(v)
		if !ok {
			return fmt.Errorf("got %s, want string", v.Type())
		}
		*dst = pathFormat.NewParser(s)
		return nil
	}
}
