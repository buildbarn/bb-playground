package unpack

import (
	"golang.org/x/exp/constraints"

	"go.starlark.net/starlark"
)

func Int[T constraints.Integer](v starlark.Value, dst *T) error {
	return starlark.AsInt(v, dst)
}
