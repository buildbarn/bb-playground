package unpack

import (
	"errors"
	"fmt"

	"go.starlark.net/starlark"
)

func Dict[TKey comparable, TValue any](keyUnpacker UnpackerInto[TKey], valueUnpacker UnpackerInto[TValue]) UnpackerInto[map[TKey]TValue] {
	return func(thread *starlark.Thread, v starlark.Value, dst *map[TKey]TValue) error {
		dict, ok := v.(interface {
			starlark.IterableMapping
			Len() int
		})
		if !ok {
			return fmt.Errorf("got %s, want dict", v.Type())
		}

		d := make(map[TKey]TValue, dict.Len())
		for key, value := range starlark.Entries(dict) {
			var unpackedKey TKey
			if err := keyUnpacker(thread, key, &unpackedKey); err != nil {
				return err
			}
			var unpackedValue TValue
			if err := valueUnpacker(thread, value, &unpackedValue); err != nil {
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
}
