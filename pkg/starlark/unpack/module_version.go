package unpack

import (
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/label"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type moduleVersionUnpackerInto struct{}

var ModuleVersion UnpackerInto[label.ModuleVersion] = moduleVersionUnpackerInto{}

func (moduleVersionUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *label.ModuleVersion) error {
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

func (ui moduleVersionUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var mv label.ModuleVersion
	if err := ui.UnpackInto(thread, v, &mv); err != nil {
		return nil, err
	}
	return starlark.String(mv.String()), nil
}

func (moduleVersionUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
