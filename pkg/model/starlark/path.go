package starlark

import (
	"fmt"

	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type Path struct{}

var _ starlark.Value = Path{}

func NewPath() starlark.Value {
	return &Path{}
}

func (Path) String() string {
	return "TODO"
}

func (Path) Type() string {
	return "path"
}

func (Path) Freeze() {}

func (Path) Truth() starlark.Bool {
	panic("TODO")
}

func (Path) Hash() (uint32, error) {
	return 0, nil
}

type pathOrLabelOrStringUnpackerInto struct{}

func NewPathOrLabelOrStringUnpackerInto() unpack.UnpackerInto[Path] {
	return &pathOrLabelOrStringUnpackerInto{}
}

func (ui *pathOrLabelOrStringUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *Path) error {
	switch typedV := v.(type) {
	case starlark.String:
		*dst = Path{}
		return nil
	case label:
		*dst = Path{}
		return nil
	case Path:
		*dst = typedV
		return nil
	default:
		return fmt.Errorf("got %s, want path, Label or str", v.Type())
	}
}

func (ui *pathOrLabelOrStringUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var p Path
	if err := ui.UnpackInto(thread, v, &p); err != nil {
		return nil, err
	}
	return p, nil
}

func (pathOrLabelOrStringUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
