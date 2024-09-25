package unpack

import (
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/label"

	"go.starlark.net/starlark"
)

func ModuleVersion(v starlark.Value, dst *label.ModuleVersion) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("got %s, want string", v.Type())
	}
	mv, err := label.NewModuleVersion(s)
	if err != nil {
		return fmt.Errorf("invalid module version: %w", err)
	}
	*dst = mv
	return nil
}
