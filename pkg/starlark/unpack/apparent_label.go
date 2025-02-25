package unpack

import (
	"fmt"

	"github.com/buildbarn/bonanza/pkg/label"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type apparentLabelUnpackerInto struct{}

var ApparentLabel UnpackerInto[label.ApparentLabel] = apparentLabelUnpackerInto{}

func (apparentLabelUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *label.ApparentLabel) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("got %s, want string", v.Type())
	}
	canonicalPackage := label.MustNewCanonicalLabel(thread.CallFrame(1).Pos.Filename()).GetCanonicalPackage()
	l, err := canonicalPackage.AppendLabel(s)
	if err != nil {
		return fmt.Errorf("invalid label: %w", err)
	}
	*dst = l
	return nil
}

func (ui apparentLabelUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var l label.ApparentLabel
	if err := ui.UnpackInto(thread, v, &l); err != nil {
		return nil, err
	}
	return starlark.String(l.String()), nil
}

func (apparentLabelUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
