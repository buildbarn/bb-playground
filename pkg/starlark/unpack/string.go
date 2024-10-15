package unpack

import (
	"fmt"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type stringUnpackerInto struct{}

var String UnpackerInto[string] = stringUnpackerInto{}

func (stringUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *string) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("got %s, want string", v.Type())
	}
	*dst = s
	return nil
}

func (ui stringUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var s string
	if err := ui.UnpackInto(thread, v, &s); err != nil {
		return nil, err
	}
	return starlark.String(s), nil
}

func (stringUnpackerInto) GetConcatenationOperator() syntax.Token {
	return syntax.PLUS
}
