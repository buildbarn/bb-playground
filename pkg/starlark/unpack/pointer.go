package unpack

import (
	"go.starlark.net/starlark"
)

type pointerUnpackerInto[T any] struct {
	UnpackerInto[T]
}

func Pointer[T any](base UnpackerInto[T]) UnpackerInto[*T] {
	return &pointerUnpackerInto[T]{
		UnpackerInto: base,
	}
}

func (ui *pointerUnpackerInto[T]) UnpackInto(thread *starlark.Thread, v starlark.Value, dst **T) error {
	var instance T
	if err := ui.UnpackerInto.UnpackInto(thread, v, &instance); err != nil {
		return err
	}
	*dst = &instance
	return nil
}
