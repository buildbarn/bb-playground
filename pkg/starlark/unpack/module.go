package unpack

import (
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/label"

	"go.starlark.net/starlark"
)

func Module(v starlark.Value, dst *label.Module) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("got %s, want string", v.Type())
	}
	l, err := label.NewModule(s)
	if err != nil {
		return fmt.Errorf("invalid module: %w", err)
	}
	*dst = l
	return nil
}
