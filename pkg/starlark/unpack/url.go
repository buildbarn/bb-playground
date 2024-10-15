package unpack

import (
	"fmt"
	"net/url"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type urlUnpackerInto struct{}

var URL UnpackerInto[*url.URL] = urlUnpackerInto{}

func (urlUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst **url.URL) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("got %s, want string", v.Type())
	}
	u, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	*dst = u
	return nil
}

func (ui urlUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var u *url.URL
	if err := ui.UnpackInto(thread, v, &u); err != nil {
		return nil, err
	}
	return starlark.String(u.String()), nil
}

func (urlUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
