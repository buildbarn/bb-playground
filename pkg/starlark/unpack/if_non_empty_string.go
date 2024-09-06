package unpack

import (
	"fmt"

	"go.starlark.net/starlark"
)

// IfNonEmptyString is a decorator for UnpackerInto that only calls into
// the underlying unpacker if the value is a non-empty string. This can
// be of use if the default value of an argument is a string, and the
// empty string is used to denote that a value is unset.
func IfNonEmptyString[T any](unpacker UnpackerInto[T]) UnpackerInto[T] {
	return func(v starlark.Value, dst *T) error {
		s, ok := starlark.AsString(v)
		if !ok {
			return fmt.Errorf("got %s, want string", v.Type())
		}
		if s == "" {
			return nil
		}
		return unpacker(v, dst)
	}
}
