package unpack

import (
	"go.starlark.net/starlark"
)

// IfNotNone is a decorator for UnpackerInto that only calls into the
// underlying unpacker if the value is not None. This can be of use if
// None is used to denote that a value is unset.
func IfNotNone[T any](unpacker UnpackerInto[T]) UnpackerInto[T] {
	return func(thread *starlark.Thread, v starlark.Value, dst *T) error {
		if v == starlark.None {
			return nil
		}
		return unpacker(thread, v, dst)
	}
}
