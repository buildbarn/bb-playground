package starlark

import (
	pg_label "github.com/buildbarn/bb-playground/pkg/label"

	"go.starlark.net/starlark"
)

type HasLabels interface {
	VisitLabels(thread *starlark.Thread, path map[starlark.Value]struct{}, visitor func(pg_label.ResolvedLabel))
}

func VisitLabels(thread *starlark.Thread, v starlark.Value, path map[starlark.Value]struct{}, visitor func(pg_label.ResolvedLabel)) {
	if _, ok := path[v]; !ok {
		path[v] = struct{}{}
		switch typedV := v.(type) {
		case HasLabels:
			// Type has its own logic for reporting labels.
			typedV.VisitLabels(thread, path, visitor)

		// Composite types that require special handling.
		case *starlark.Dict:
			for key, value := range starlark.Entries(thread, typedV) {
				VisitLabels(thread, key, path, visitor)
				VisitLabels(thread, value, path, visitor)
			}
		case *starlark.List:
			for value := range starlark.Elements(typedV) {
				VisitLabels(thread, value, path, visitor)
			}

		// Non-label scalars.
		case starlark.Bool:
		case starlark.String:
		case starlark.NoneType:

		default:
			panic("unsupported type " + v.Type())
		}
		delete(path, v)
	}
}
