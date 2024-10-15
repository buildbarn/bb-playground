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
		value: removeTargetNameIfRedundant(value[:identifierOffset]) + value[identifierOffset:],
	}, nil
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
