package label

import (
	"errors"
	"regexp"
	"strings"
)

type CanonicalRepo struct {
	value string
}

const validCanonicalRepoPattern = validModulePattern + `\+`

var validCanonicalRepoRegexp = regexp.MustCompile("^" + validCanonicalRepoPattern + "$")

var invalidCanonicalRepoPattern = errors.New("canonical repo must match " + validCanonicalRepoPattern)

func NewCanonicalRepo(value string) (CanonicalRepo, error) {
	if !validCanonicalRepoRegexp.MatchString(value) {
		return CanonicalRepo{}, invalidCanonicalRepoPattern
	}
	return CanonicalRepo{value: value}, nil
}

func (r CanonicalRepo) String() string {
	return r.value
}

// GetModule returns the module to which this repo belongs.
func (r CanonicalRepo) GetModule() Module {
	return Module{value: r.value[:strings.IndexByte(r.value, '+')]}
}
