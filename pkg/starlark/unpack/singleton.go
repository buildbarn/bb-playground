package unpack

import (
	"go.starlark.net/starlark"
)

type singletonUnpackerInto[T any] struct {
	UnpackerInto[T]
}

func Singleton[T any](base UnpackerInto[T]) UnpackerInto[[]T] {
	return &singletonUnpackerInto[T]{
		UnpackerInto: base,
	}
}

func (ui *singletonUnpackerInto[T]) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *[]T) error {
	var instance [1]T
	if err := ui.UnpackerInto.UnpackInto(thread, v, &instance[0]); err != nil {
		return err
	}
	*dst = instance[:]
	return nil
}
