package unpack

import (
	"fmt"

	"go.starlark.net/starlark"
)

func Bool(v starlark.Value, dst *bool) error {
	s, ok := v.(starlark.Bool)
	if !ok {
		return fmt.Errorf("got %s, want bool", v.Type())
	}
	*dst = bool(s)
	return nil
}
