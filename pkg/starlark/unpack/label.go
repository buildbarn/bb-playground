package unpack

import (
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/label"

	"go.starlark.net/starlark"
)

func Label(v starlark.Value, dst *label.Label) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("got %s, want string", v.Type())
	}
	l, err := label.NewLabel(s)
	if err != nil {
		return fmt.Errorf("invalid label: %w", err)
	}
	*dst = l
	return nil
}
