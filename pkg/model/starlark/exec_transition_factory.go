package starlark

import (
	"go.starlark.net/starlark"
)

type execTransitionFactory struct {
	execGroup string
}

func NewExecTransitionFactory(execGroup string) starlark.Value {
	return &execTransitionFactory{
		execGroup: execGroup,
	}
}

func (l *execTransitionFactory) String() string {
	return "<config.exec>"
}

func (l *execTransitionFactory) Type() string {
	return "config.exec"
}

func (l *execTransitionFactory) Freeze() {}

func (l *execTransitionFactory) Truth() starlark.Bool {
	return starlark.True
}

func (l *execTransitionFactory) Hash() (uint32, error) {
	return starlark.String(l.execGroup).Hash()
}
