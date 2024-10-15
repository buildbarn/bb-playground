package starlark

import (
	"maps"
	"slices"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"

	"go.starlark.net/starlark"
)

type LateNamedValue struct {
	Identifier *pg_label.CanonicalStarlarkIdentifier
}

func (lnv *LateNamedValue) AssignIdentifier(identifier pg_label.CanonicalStarlarkIdentifier) {
	if lnv.Identifier == nil {
		lnv.Identifier = &identifier
	}
}

type NamedGlobal interface {
	starlark.Value
	AssignIdentifier(identifier pg_label.CanonicalStarlarkIdentifier)
}

func NameAndExtractGlobals(globals starlark.StringDict, canonicalLabel pg_label.CanonicalLabel) {
	// Iterate over globals in sorted order. This ensures that if we
	// encounter constructs like these, we still assign names
	// deterministically:
	//
	//     x = rule(..)
	//     y = x
	for _, name := range slices.Sorted(maps.Keys(globals)) {
		if value, ok := globals[name].(NamedGlobal); ok {
			identifier := canonicalLabel.AppendStarlarkIdentifier(pg_label.MustNewStarlarkIdentifier(name))
			value.AssignIdentifier(identifier)
		}
	}
}
