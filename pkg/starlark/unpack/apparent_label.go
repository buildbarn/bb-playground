package unpack

import (
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/label"

	"go.starlark.net/starlark"
)

func ApparentLabel(thread *starlark.Thread, v starlark.Value, dst *label.ApparentLabel) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("got %s, want string", v.Type())
	}
	canonicalPackage := label.MustNewCanonicalLabel(thread.CallFrame(1).Pos.Filename()).GetCanonicalPackage()
	l, err := canonicalPackage.AppendLabel(s)
	if err != nil {
		return fmt.Errorf("invalid label: %w", err)
	}
	*dst = l
	return nil
}
