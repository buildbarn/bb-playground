package label

import (
	"errors"
	"regexp"
)

type StarlarkIdentifier struct {
	value string
}

const (
	validStarlarkIdentifierPattern = `[a-zA-Z_]\w*`
)

var validStarlarkIdentifierRegexp = regexp.MustCompile("^" + validStarlarkIdentifierPattern + "$")

var invalidStarlarkIdentifierPattern = errors.New("Starlark identifier must match " + validStarlarkIdentifierPattern)

func NewStarlarkIdentifier(value string) (StarlarkIdentifier, error) {
	if !validStarlarkIdentifierRegexp.MatchString(value) {
		return StarlarkIdentifier{}, invalidStarlarkIdentifierPattern
	}
	return StarlarkIdentifier{value: value}, nil
}

func MustNewStarlarkIdentifier(value string) StarlarkIdentifier {
	identifier, err := NewStarlarkIdentifier(value)
	if err != nil {
		panic(err)
	}
	return identifier
}

func (i StarlarkIdentifier) String() string {
	return i.value
}

func (i StarlarkIdentifier) IsPublic() bool {
	return i.value[0] != '_'
}
