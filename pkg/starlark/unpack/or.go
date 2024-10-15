package unpack

import (
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type orUnpackerInto[T any] struct {
	unpackers []UnpackerInto[T]
}

func Or[T any](unpackers []UnpackerInto[T]) UnpackerInto[T] {
	return &orUnpackerInto[T]{
		unpackers: unpackers,
	}
}

func (ui *orUnpackerInto[T]) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *T) error {
	unpackers := ui.unpackers
	for {
		err := unpackers[0].UnpackInto(thread, v, dst)
		unpackers = unpackers[1:]
		if err == nil || len(unpackers) == 0 {
			return err
		}
	}
}

func (ui *orUnpackerInto[T]) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	unpackers := ui.unpackers
	for {
		canonicalized, err := unpackers[0].Canonicalize(thread, v)
		unpackers = unpackers[1:]
		if err == nil || len(unpackers) == 0 {
			return canonicalized, err
		}
	}
}

func (ui *orUnpackerInto[T]) GetConcatenationOperator() syntax.Token {
	o := ui.unpackers[0].GetConcatenationOperator()
	if o != 0 {
		for _, unpacker := range ui.unpackers[1:] {
			if unpacker.GetConcatenationOperator() != o {
				return 0
			}
		}
	}
	return o
}
