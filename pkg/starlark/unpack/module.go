package unpack

import (
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/label"

	"go.starlark.net/starlark"
)

func Module(thread *starlark.Thread, v starlark.Value, dst *label.Module) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("got %s, want string", v.Type())
	}
	m, err := label.NewModule(s)
	if err != nil {
		return fmt.Errorf("invalid module: %w", err)
	}
	*dst = m
	return nil
}
