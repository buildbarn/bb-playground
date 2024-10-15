package unpack

import (
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type Canonicalizer interface {
	Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error)
	GetConcatenationOperator() syntax.Token
}

type canonicalizeUnpackerInto struct {
	Canonicalizer
}

func Canonicalize(base Canonicalizer) UnpackerInto[starlark.Value] {
	return &canonicalizeUnpackerInto{
		Canonicalizer: base,
	}
}

func (ui *canonicalizeUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *starlark.Value) error {
	canonicalized, err := ui.Canonicalizer.Canonicalize(thread, v)
	if err != nil {
		return err
	}
	*dst = canonicalized
	return nil
}
