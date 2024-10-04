package unpack

import (
	"go.starlark.net/starlark"
)

func Pointer[T any](unpacker UnpackerInto[T]) UnpackerInto[*T] {
	return func(thread *starlark.Thread, v starlark.Value, dst **T) error {
		var instance T
		if err := unpacker(thread, v, &instance); err != nil {
			return err
		}
		*dst = &instance
		return nil
	}
}
