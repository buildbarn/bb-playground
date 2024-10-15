package unpack

import (
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type anyUnpackerInto struct{}

var Any UnpackerInto[starlark.Value] = anyUnpackerInto{}

func (anyUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *starlark.Value) error {
	*dst = v
	return nil
}

func (anyUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	return v, nil
}

func (anyUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
