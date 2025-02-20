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

func (lnv *LateNamedValue) equals(other *LateNamedValue) bool {
	// First check whether the values have the same memory address.
	// If values are restored from storage, then their addresses may
	// differ. Fall back to comparing their identifiers in that case.
	return lnv == other || (lnv.Identifier != nil && other.Identifier != nil && *lnv.Identifier == *other.Identifier)
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
