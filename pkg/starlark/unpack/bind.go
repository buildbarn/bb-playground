package unpack

import (
	"go.starlark.net/starlark"
)

type UnpackerInto[T any] interface {
	Canonicalizer
	UnpackInto(thread *starlark.Thread, v starlark.Value, dst *T) error
}

type boundUnpacker[T any] struct {
	thread   *starlark.Thread
	dst      *T
	unpacker UnpackerInto[T]
}

func Bind[T any](thread *starlark.Thread, dst *T, unpacker UnpackerInto[T]) starlark.Unpacker {
	return &boundUnpacker[T]{
		thread:   thread,
		dst:      dst,
		unpacker: unpacker,
	}
}

func (u *boundUnpacker[T]) Unpack(v starlark.Value) error {
	return u.unpacker.UnpackInto(u.thread, v, u.dst)
}
