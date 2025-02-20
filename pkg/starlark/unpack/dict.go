package unpack

import (
	"errors"
	"fmt"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type dictUnpackerInto[TKey comparable, TValue any] struct {
	keyUnpacker   UnpackerInto[TKey]
	valueUnpacker UnpackerInto[TValue]
}

func Dict[TKey comparable, TValue any](keyUnpacker UnpackerInto[TKey], valueUnpacker UnpackerInto[TValue]) UnpackerInto[map[TKey]TValue] {
	return &dictUnpackerInto[TKey, TValue]{
		keyUnpacker:   keyUnpacker,
		valueUnpacker: valueUnpacker,
	}
}

func (ui *dictUnpackerInto[TKey, TValue]) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *map[TKey]TValue) error {
	dict, ok := v.(interface {
		starlark.IterableMapping
		Len() int
	})
	if !ok {
		return fmt.Errorf("got %s, want dict", v.Type())
	}

	d := make(map[TKey]TValue, dict.Len())
	for key, value := range starlark.Entries(thread, dict) {
		var unpackedKey TKey
		if err := ui.keyUnpacker.UnpackInto(thread, key, &unpackedKey); err != nil {
			return err
		}
		var unpackedValue TValue
		if err := ui.valueUnpacker.UnpackInto(thread, value, &unpackedValue); err != nil {
			return err
		}
		if _, ok := d[unpackedKey]; ok {
			return errors.New("dict contains duplicate keys")
		}
		d[unpackedKey] = unpackedValue
	}
	*dst = d
	return nil
}

func (ui *dictUnpackerInto[TKey, TValue]) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	dict, ok := v.(interface {
		starlark.IterableMapping
		Len() int
	})
	if !ok {
		return nil, fmt.Errorf("got %s, want dict", v.Type())
	}

	d := starlark.NewDict(dict.Len())
	for key, value := range starlark.Entries(thread, dict) {
		canonicalizedKey, err := ui.keyUnpacker.Canonicalize(thread, key)
		if err != nil {
			return nil, err
		}
		canonicalizedValue, err := ui.valueUnpacker.Canonicalize(thread, value)
		if err != nil {
			return nil, err
		}
		d.SetKey(thread, canonicalizedKey, canonicalizedValue)
	}
	return d, nil
}

func (dictUnpackerInto[TKey, TValue]) GetConcatenationOperator() syntax.Token {
	return syntax.PIPE
}
