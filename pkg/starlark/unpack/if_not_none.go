package unpack

import (
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type ifNotNoneUnpackerInto[T any] struct {
	base UnpackerInto[T]
}

// IfNotNone is a decorator for UnpackerInto that only calls into the
// underlying unpacker if the value is not None. This can be of use if
// None is used to denote that a value is unset.
func IfNotNone[T any](base UnpackerInto[T]) UnpackerInto[T] {
	return &ifNotNoneUnpackerInto[T]{
		base: base,
	}
}

func (ui *ifNotNoneUnpackerInto[T]) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *T) error {
	if v == starlark.None {
		return nil
	}
	return ui.base.UnpackInto(thread, v, dst)
}

func (ui *ifNotNoneUnpackerInto[T]) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	if v == starlark.None {
		return v, nil
	}
	return ui.base.Canonicalize(thread, v)
}

func (ui *ifNotNoneUnpackerInto[T]) GetConcatenationOperator() syntax.Token {
	return ui.base.GetConcatenationOperator()
}
