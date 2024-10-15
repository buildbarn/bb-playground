package unpack

import (
	"golang.org/x/exp/constraints"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type intUnpackerInto[T constraints.Integer] struct{}

func Int[T constraints.Integer]() UnpackerInto[T] {
	return intUnpackerInto[T]{}
}

func (intUnpackerInto[T]) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *T) error {
	return starlark.AsInt(v, dst)
}

func (intUnpackerInto[T]) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var dst T
	return v, starlark.AsInt(v, &dst)
}

func (intUnpackerInto[T]) GetConcatenationOperator() syntax.Token {
	// Even though addition isn't necessarily the same thing as
	// concatenation, Bazel seems to allow it.
	return syntax.PLUS
}
