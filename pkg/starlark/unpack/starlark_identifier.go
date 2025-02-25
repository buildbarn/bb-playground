package unpack

import (
	"fmt"

	"github.com/buildbarn/bonanza/pkg/label"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type starlarkIdentifierUnpackerInto struct{}

var StarlarkIdentifier UnpackerInto[label.StarlarkIdentifier] = starlarkIdentifierUnpackerInto{}

func (starlarkIdentifierUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *label.StarlarkIdentifier) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("got %s, want string", v.Type())
	}
	i, err := label.NewStarlarkIdentifier(s)
	if err != nil {
		return fmt.Errorf("invalid Starlark identifier: %w", err)
	}
	*dst = i
	return nil
}

func (ui starlarkIdentifierUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var i label.StarlarkIdentifier
	if err := ui.UnpackInto(thread, v, &i); err != nil {
		return nil, err
	}
	return starlark.String(i.String()), nil
}

func (starlarkIdentifierUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
