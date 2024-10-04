package starlark

import (
	"go.starlark.net/starlark"
)

type execGroup struct{}

func NewExecGroup() starlark.Value {
	return &execGroup{}
}

func (l *execGroup) String() string {
	return "<exec_group>"
}

func (l *execGroup) Type() string {
	return "exec_group"
}

func (l *execGroup) Freeze() {}

func (l *execGroup) Truth() starlark.Bool {
	return starlark.True
}

func (l *execGroup) Hash() (uint32, error) {
	// TODO
	return 0, nil
}
