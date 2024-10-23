package label

import (
	"errors"
	"regexp"
	"strings"
)

// ModuleInstance is a pair of a module name and an optional module
// version. It is the leading part of a canonical repo name.
type ModuleInstance struct {
	value string
}

const validModuleInstancePattern = validModulePattern + `\+` + `(` + canonicalModuleVersionPattern + `)?`

var validModuleInstanceRegexp = regexp.MustCompile("^" + validModuleInstancePattern + "$")

var invalidModuleInstancePattern = errors.New("module instance must match " + validModuleInstancePattern)

func NewModuleInstance(value string) (ModuleInstance, error) {
	if !validModuleInstanceRegexp.MatchString(value) {
		return ModuleInstance{}, invalidModuleInstancePattern
	}
	return ModuleInstance{value: value}, nil
}

func MustNewModuleInstance(value string) ModuleInstance {
	mi, err := NewModuleInstance(value)
	if err != nil {
		panic(err)
	}
	return mi
}

func (mi ModuleInstance) String() string {
	return mi.value
}

func (mi ModuleInstance) GetModule() Module {
	return Module{value: mi.value[:strings.IndexByte(mi.value, '+')]}
}

func (mi ModuleInstance) GetModuleVersion() (ModuleVersion, bool) {
	version := mi.value[strings.IndexByte(mi.value, '+')+1:]
	if version == "" {
		return ModuleVersion{}, false
	}
	return ModuleVersion{value: version}, true
}

func (mi ModuleInstance) GetBareCanonicalRepo() CanonicalRepo {
	return CanonicalRepo{value: mi.value}
}

func (mi ModuleInstance) GetCanonicalRepoWithModuleExtension(extensionName StarlarkIdentifier, repo ApparentRepo) CanonicalRepo {
	return CanonicalRepo{value: mi.value + "+" + extensionName.value + "+" + repo.value}
}
