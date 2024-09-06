package unpack

import (
	"go.starlark.net/starlark"
)

type UnpackerInto[T any] func(v starlark.Value, dst *T) error

type boundUnpacker[T any] struct {
	dst      *T
	unpacker UnpackerInto[T]
}

func Bind[T any](dst *T, unpacker UnpackerInto[T]) starlark.Unpacker {
	return &boundUnpacker[T]{
		dst:      dst,
		unpacker: unpacker,
	}
}

func (u *boundUnpacker[T]) Unpack(v starlark.Value) error {
	return u.unpacker(v, u.dst)
}
