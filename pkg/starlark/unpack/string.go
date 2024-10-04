package unpack

import (
	"fmt"

	"go.starlark.net/starlark"
)

func String(thread *starlark.Thread, v starlark.Value, dst *string) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("got %s, want string", v.Type())
	}
	*dst = s
	return nil
}
