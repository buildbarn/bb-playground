package label

import (
	"errors"
	"regexp"
	"strings"
)

// ModuleExtension is a Starlark identifier that corresponds to the name
// of the module extension object declared in a .bzl file, prefixed with
// the name of the module instance containing the module extension. Its
// value is used as a prefix for the names of repos declared by the
// module extension.
type ModuleExtension struct {
	value string
}

const validModuleExtensionPattern = validModuleInstancePattern + `\+` + validStarlarkIdentifierPattern

var validModuleExtensionRegexp = regexp.MustCompile("^" + validModuleExtensionPattern + "$")

var invalidModuleExtensionPattern = errors.New("module extension must match " + validModuleExtensionPattern)

func NewModuleExtension(value string) (ModuleExtension, error) {
	if !validModuleExtensionRegexp.MatchString(value) {
		return ModuleExtension{}, invalidModuleExtensionPattern
	}
	return ModuleExtension{value: value}, nil
}

func MustNewModuleExtension(value string) ModuleExtension {
	me, err := NewModuleExtension(value)
	if err != nil {
		panic(err)
	}
	return me
}

func (me ModuleExtension) String() string {
	return me.value
}

func (me ModuleExtension) GetModuleInstance() ModuleInstance {
	return ModuleInstance{value: me.value[:strings.LastIndexByte(me.value, '+')]}
}

func (me ModuleExtension) GetCanonicalRepoWithModuleExtension(repo ApparentRepo) CanonicalRepo {
	return CanonicalRepo{value: me.value + "+" + repo.value}
}
