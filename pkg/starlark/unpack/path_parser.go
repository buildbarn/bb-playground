package unpack

import (
	"fmt"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type pathParserUnpackerInto struct {
	pathFormat path.Format
}

func PathParser(pathFormat path.Format) UnpackerInto[path.Parser] {
	return &pathParserUnpackerInto{
		pathFormat: pathFormat,
	}
}

func (ui *pathParserUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst *path.Parser) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("got %s, want string", v.Type())
	}
	*dst = ui.pathFormat.NewParser(s)
	return nil
}

func (ui *pathParserUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	s, ok := starlark.AsString(v)
	if !ok {
		return nil, fmt.Errorf("got %s, want string", v.Type())
	}
	builder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	if err := path.Resolve(ui.pathFormat.NewParser(s), scopeWalker); err != nil {
		return nil, err
	}
	s, err := ui.pathFormat.GetString(builder)
	if err != nil {
		return nil, err
	}
	return starlark.String(s), nil
}

func (pathParserUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
