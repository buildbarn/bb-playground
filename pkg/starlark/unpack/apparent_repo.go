package unpack

import (
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/label"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type apparentRepoUnpackerInto struct{}

var ApparentRepo UnpackerInto[label.ApparentRepo] = apparentRepoUnpackerInto{}

func (apparentRepoUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *label.ApparentRepo) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("got %s, want string", v.Type())
	}
	r, err := label.NewApparentRepo(s)
	if err != nil {
		return fmt.Errorf("invalid apparent repo: %w", err)
	}
	*dst = r
	return nil
}

func (ui apparentRepoUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var r label.ApparentRepo
	if err := ui.UnpackInto(thread, v, &r); err != nil {
		return nil, err
	}
	return starlark.String(r.String()), nil
}

func (apparentRepoUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
