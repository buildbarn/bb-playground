package unpack

import (
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/label"

	"go.starlark.net/starlark"
)

func ApparentRepo(v starlark.Value, dst *label.ApparentRepo) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("got %s, want string", v.Type())
	}
	r, err := label.NewApparentRepo(s)
	if err != nil {
		return fmt.Errorf("invalid apparent repo: %w", err)
	}
	*dst = r
	return nil
}
