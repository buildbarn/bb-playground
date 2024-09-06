package label

import (
	"errors"
	"regexp"
)

type Module struct {
	value string
}

const validModulePattern = "[a-z]([a-z0-9._-]*[a-z0-9])?"

var validModuleRegexp = regexp.MustCompile(validModulePattern)

var invalidModulePattern = errors.New("module name must match " + validModulePattern)

func NewModule(value string) (Module, error) {
	if !validModuleRegexp.MatchString(value) {
		return Module{}, invalidModulePattern
	}
	return Module{value: value}, nil
}

func MustNewModule(value string) Module {
	m, err := NewModule(value)
	if err != nil {
		panic(err)
	}
	return m
}

func (m Module) String() string {
	return m.value
}

func (m Module) ToApparentRepo() ApparentRepo {
	// Every module name is also a valid apparent repo name.
	return ApparentRepo{value: m.value}
}
