package unpack

import (
	"fmt"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type boolUnpackerInto struct{}

var Bool UnpackerInto[bool] = boolUnpackerInto{}

func (boolUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *bool) error {
	s, ok := v.(starlark.Bool)
	if !ok {
		return fmt.Errorf("got %s, want bool", v.Type())
	}
	*dst = bool(s)
	return nil
}

func (boolUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	if _, ok := v.(starlark.Bool); !ok {
		return nil, fmt.Errorf("got %s, want bool", v.Type())
	}
	return v, nil
}

func (boolUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
