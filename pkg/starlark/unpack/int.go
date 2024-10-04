package unpack

import (
	"golang.org/x/exp/constraints"

	"go.starlark.net/starlark"
)

func Int[T constraints.Integer](thread *starlark.Thread, v starlark.Value, dst *T) error {
	return starlark.AsInt(v, dst)
}
