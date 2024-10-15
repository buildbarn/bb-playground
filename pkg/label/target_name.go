package label

import (
	"errors"
	"regexp"
)

// TargetName corresponds to the name of an addressable and/or buildable
// target within a package.
type TargetName struct {
	value string
}

var validTargetNameRegexp = regexp.MustCompile("^" + validTargetNamePattern + "$")

var invalidTargetNamePattern = errors.New("Target name must match " + validTargetNamePattern)

func NewTargetName(value string) (TargetName, error) {
	if !validTargetNameRegexp.MatchString(value) {
		return TargetName{}, invalidTargetNamePattern
	}
	return TargetName{value: value}, nil
}

func MustNewTargetName(value string) TargetName {
	identifier, err := NewTargetName(value)
	if err != nil {
		panic(err)
	}
	return identifier
}

func (tn TargetName) String() string {
	return tn.value
}
