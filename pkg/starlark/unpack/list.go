package unpack

import (
	"fmt"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type listUnpackerInto[T any] struct {
	base UnpackerInto[T]
}

func List[T any](base UnpackerInto[T]) UnpackerInto[[]T] {
	return &listUnpackerInto[T]{
		base: base,
	}
}

func (ui *listUnpackerInto[T]) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *[]T) error {
	list, ok := v.(starlark.Indexable)
	if !ok {
		return fmt.Errorf("got %s, want list", v.Type())
	}

	count := list.Len()
	l := make([]T, count)
	for i := 0; i < count; i++ {
		if err := ui.base.UnpackInto(thread, list.Index(i), &l[i]); err != nil {
			return fmt.Errorf("at index %d: %w", i, err)
		}
	}
	*dst = l
	return nil
}

func (ui *listUnpackerInto[T]) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	list, ok := v.(starlark.Indexable)
	if !ok {
		return nil, fmt.Errorf("got %s, want list", v.Type())
	}

	count := list.Len()
	l := make([]starlark.Value, 0, count)
	for i := 0; i < count; i++ {
		value := list.Index(i)
		canonicalizedValue, err := ui.base.Canonicalize(thread, value)
		if err != nil {
			return nil, fmt.Errorf("at index %d: %w", i, err)
		}
		l = append(l, canonicalizedValue)
	}
	return starlark.NewList(l), nil
}

func (listUnpackerInto[T]) GetConcatenationOperator() syntax.Token {
	return syntax.PLUS
}
