package unpack

import (
	"fmt"
	"net/url"

	"go.starlark.net/starlark"
)

func URL(v starlark.Value, dst **url.URL) error {
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
