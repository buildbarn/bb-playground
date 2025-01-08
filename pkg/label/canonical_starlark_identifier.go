package label

import (
	"errors"
	"regexp"
	"strings"
)

type CanonicalStarlarkIdentifier struct {
	value string
}

const (
	validCanonicalStarlarkIdentifierPattern = validCanonicalLabelPattern + `%[a-zA-Z_]\w*`
)

var validCanonicalStarlarkIdentifierRegexp = regexp.MustCompile("^" + validCanonicalStarlarkIdentifierPattern + "$")

var invalidCanonicalStarlarkIdentifierPattern = errors.New("canonical Starlark identifier must match " + validCanonicalStarlarkIdentifierPattern)

func NewCanonicalStarlarkIdentifier(value string) (CanonicalStarlarkIdentifier, error) {
	if !validCanonicalStarlarkIdentifierRegexp.MatchString(value) {
		return CanonicalStarlarkIdentifier{}, invalidCanonicalStarlarkIdentifierPattern
	}

	identifierOffset := strings.LastIndexByte(value, '%')
	return CanonicalStarlarkIdentifier{
		value: removeLabelTargetNameIfRedundant(value[:identifierOffset]) + value[identifierOffset:],
	}, nil
}

func MustNewCanonicalStarlarkIdentifier(value string) CanonicalStarlarkIdentifier {
	i, err := NewCanonicalStarlarkIdentifier(value)
	if err != nil {
		panic(err)
	}
	return i
}

func (i CanonicalStarlarkIdentifier) String() string {
	return i.value
}

func (i CanonicalStarlarkIdentifier) GetCanonicalLabel() CanonicalLabel {
	return CanonicalLabel{
		value: i.value[:strings.LastIndexByte(i.value, '%')],
	}
}

func (i CanonicalStarlarkIdentifier) GetStarlarkIdentifier() StarlarkIdentifier {
	return StarlarkIdentifier{
		value: i.value[strings.LastIndexByte(i.value, '%')+1:],
	}
}

// ToModuleExtension converts a Starlark identifier of a module
// extension declared in a .bzl file and converts it to a string that
// can be prefixed to repos that are declared by the module extension.
func (i CanonicalStarlarkIdentifier) ToModuleExtension() ModuleExtension {
	v := i.value[2:]
	versionIndex := strings.IndexByte(v, '+') + 1
	versionEnd := strings.IndexAny(v[versionIndex:], "+/")
	return ModuleExtension{value: v[:versionIndex+versionEnd] + `+` + v[strings.LastIndexByte(v, '%')+1:]}
}
