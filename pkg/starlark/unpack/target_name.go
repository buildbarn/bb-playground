package unpack

import (
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/label"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type targetNameUnpackerInto struct{}

var TargetName UnpackerInto[label.TargetName] = targetNameUnpackerInto{}

func (targetNameUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *label.TargetName) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("got %s, want string", v.Type())
	}
	tn, err := label.NewTargetName(s)
	if err != nil {
		return fmt.Errorf("invalid target name: %w", err)
	}
	*dst = tn
	return nil
}

func (ui targetNameUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var tn label.TargetName
	if err := ui.UnpackInto(thread, v, &tn); err != nil {
		return nil, err
	}
	return starlark.String(tn.String()), nil
}

func (targetNameUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
