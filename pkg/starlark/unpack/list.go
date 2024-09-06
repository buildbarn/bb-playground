package unpack

import (
	"fmt"

	"go.starlark.net/starlark"
)

func List[T any](unpacker UnpackerInto[T]) UnpackerInto[[]T] {
	return func(v starlark.Value, dst *[]T) error {
		list, ok := v.(starlark.Indexable)
		if !ok {
			return fmt.Errorf("got %s, want list", v.Type())
		}

		count := list.Len()
		l := make([]T, count)
		for i := 0; i < count; i++ {
			if err := unpacker(list.Index(i), &l[i]); err != nil {
				return fmt.Errorf("at index %d: %w", i, err)
			}
		}
		*dst = l
		return nil
	}
}
