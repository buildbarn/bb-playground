package starlark

import (
	"fmt"
	"strings"

	pg_label "github.com/buildbarn/bb-playground/pkg/label"
	"github.com/buildbarn/bb-playground/pkg/starlark/unpack"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

type Path struct {
	parent    *Path
	component path.Component
}

var (
	_ path.Parser    = &Path{}
	_ starlark.Value = &Path{}
)

func (p *Path) Append(component path.Component) *Path {
	return &Path{
		parent:    p,
		component: component,
	}
}

func (p *Path) ParseScope(scopeWalker path.ScopeWalker) (path.ComponentWalker, path.RelativeParser, error) {
	next, err := scopeWalker.OnAbsolute()
	if err != nil {
		return nil, nil, err
	}

	// Obtain all components of the path.
	count := 0
	for pCount := p; pCount != nil; pCount = pCount.parent {
		count++
	}
	components := make([]path.Component, count)
	for count > 0 {
		count--
		components[count] = p.component
		p = p.parent
	}

	return next, pathComponentParser{
		components: components,
	}, nil
}

func (p *Path) writeToStringBuilder(sb *strings.Builder) {
	if p.parent != nil {
		p.parent.writeToStringBuilder(sb)
	}
	sb.WriteByte('/')
	sb.WriteString(p.component.String())
}

func (p *Path) String() string {
	if p == nil {
		return "/"
	}
	var sb strings.Builder
	p.writeToStringBuilder(&sb)
	return sb.String()
}

func (*Path) Type() string {
	return "path"
}

func (*Path) Freeze() {}

func (*Path) Truth() starlark.Bool {
	return starlark.True
}

func (*Path) Hash() (uint32, error) {
	return 0, nil
}

type pathComponentParser struct {
	components []path.Component
}

func (pcp pathComponentParser) ParseFirstComponent(componentWalker path.ComponentWalker, mustBeDirectory bool) (next path.GotDirectoryOrSymlink, remainder path.RelativeParser, err error) {
	// Stop parsing if there are no components left.
	if len(pcp.components) == 0 {
		return path.GotDirectory{Child: componentWalker}, nil, nil
	}
	name := pcp.components[0]
	remainder = pathComponentParser{
		components: pcp.components[1:],
	}

	// Call one of OnDirectory() or OnTerminal(), depending on the
	// component's location in the path.
	if len(pcp.components) > 1 || mustBeDirectory {
		r, err := componentWalker.OnDirectory(name)
		return r, remainder, err
	}
	r, err := componentWalker.OnTerminal(name)
	if err != nil || r == nil {
		return nil, nil, err
	}
	return r, nil, nil
}

type PathResolver struct {
	CurrentPath *Path
}

var (
	_ path.ScopeWalker     = &PathResolver{}
	_ path.ComponentWalker = &PathResolver{}
)

func (r *PathResolver) OnAbsolute() (path.ComponentWalker, error) {
	r.CurrentPath = nil
	return r, nil
}

func (r *PathResolver) OnRelative() (path.ComponentWalker, error) {
	return r, nil
}

func (r *PathResolver) OnDriveLetter(drive rune) (path.ComponentWalker, error) {
	r.CurrentPath = nil
	return r, nil
}

func (r *PathResolver) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	r.CurrentPath = r.CurrentPath.Append(name)
	return path.GotDirectory{
		Child:        r,
		IsReversible: true,
	}, nil
}

func (r *PathResolver) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	r.CurrentPath = r.CurrentPath.Append(name)
	return nil, nil
}

func (r *PathResolver) OnUp() (path.ComponentWalker, error) {
	if r.CurrentPath != nil {
		r.CurrentPath = r.CurrentPath.parent
	}
	return r, nil
}

type RepoPathResolver func(canonicalRepo pg_label.CanonicalRepo) (*Path, error)

type pathOrLabelOrStringUnpackerInto struct {
	repoPathResolver RepoPathResolver
	workingDirectory *Path
}

func NewPathOrLabelOrStringUnpackerInto(repoPathResolver RepoPathResolver, workingDirectory *Path) unpack.UnpackerInto[*Path] {
	return &pathOrLabelOrStringUnpackerInto{
		repoPathResolver: repoPathResolver,
		workingDirectory: workingDirectory,
	}
}

func (ui *pathOrLabelOrStringUnpackerInto) UnpackInto(thread *starlark.Thread, v starlark.Value, dst **Path) error {
	switch typedV := v.(type) {
	case starlark.String:
		r := PathResolver{
			CurrentPath: ui.workingDirectory,
		}
		if err := path.Resolve(path.UNIXFormat.NewParser(string(typedV)), &r); err != nil {
			return err
		}
		*dst = r.CurrentPath
		return nil
	case label:
		p, err := ui.repoPathResolver(typedV.value.GetCanonicalRepo())
		if err != nil {
			return err
		}
		for _, component := range strings.Split(typedV.value.GetRepoRelativePath(), "/") {
			p = p.Append(path.MustNewComponent(component))
		}
		*dst = p
		return nil
	case *Path:
		*dst = typedV
		return nil
	default:
		return fmt.Errorf("got %s, want path, Label or str", v.Type())
	}
}

func (ui *pathOrLabelOrStringUnpackerInto) Canonicalize(thread *starlark.Thread, v starlark.Value) (starlark.Value, error) {
	var p *Path
	if err := ui.UnpackInto(thread, v, &p); err != nil {
		return nil, err
	}
	return p, nil
}

func (pathOrLabelOrStringUnpackerInto) GetConcatenationOperator() syntax.Token {
	return 0
}
