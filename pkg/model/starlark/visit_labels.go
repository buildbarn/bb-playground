package starlark

import (
	pg_label "github.com/buildbarn/bonanza/pkg/label"

	"go.starlark.net/starlark"
)

type HasLabels interface {
	VisitLabels(thread *starlark.Thread, path map[starlark.Value]struct{}, visitor func(pg_label.ResolvedLabel) error) error
}

func VisitLabels(thread *starlark.Thread, v starlark.Value, path map[starlark.Value]struct{}, visitor func(pg_label.ResolvedLabel) error) error {
	if _, ok := path[v]; !ok {
		path[v] = struct{}{}
		switch typedV := v.(type) {
		case HasLabels:
			// Type has its own logic for reporting labels.
			if err := typedV.VisitLabels(thread, path, visitor); err != nil {
				return err
			}

		// Composite types that require special handling.
		case *starlark.Dict:
			for key, value := range starlark.Entries(thread, typedV) {
				if err := VisitLabels(thread, key, path, visitor); err != nil {
					return err
				}
				if err := VisitLabels(thread, value, path, visitor); err != nil {
					return err
				}
			}
		case *starlark.List:
			for value := range starlark.Elements(typedV) {
				if err := VisitLabels(thread, value, path, visitor); err != nil {
					return err
				}
			}

		// Non-label scalars.
		case starlark.Bool:
		case starlark.Int:
		case starlark.String:
		case starlark.NoneType:

		default:
			panic("unsupported type " + v.Type())
		}
		delete(path, v)
	}
	return nil
}
