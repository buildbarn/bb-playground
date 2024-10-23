package label

import (
	"errors"
	"regexp"
	"strings"
)

type CanonicalRepo struct {
	value string
}

const validCanonicalRepoPattern = validModuleInstancePattern +
	`(\+` + validStarlarkIdentifierPattern + `\+` + validApparentRepoPattern + `)?`

var validCanonicalRepoRegexp = regexp.MustCompile("^" + validCanonicalRepoPattern + "$")

var invalidCanonicalRepoPattern = errors.New("canonical repo must match " + validCanonicalRepoPattern)

func NewCanonicalRepo(value string) (CanonicalRepo, error) {
	if !validCanonicalRepoRegexp.MatchString(value) {
		return CanonicalRepo{}, invalidCanonicalRepoPattern
	}
	return CanonicalRepo{value: value}, nil
}

func MustNewCanonicalRepo(value string) CanonicalRepo {
	r, err := NewCanonicalRepo(value)
	if err != nil {
		panic(err)
	}
	return r
}

func (r CanonicalRepo) String() string {
	return r.value
}

func (r CanonicalRepo) GetModuleInstance() ModuleInstance {
	versionIndex := strings.IndexByte(r.value, '+') + 1
	if offset := strings.IndexByte(r.value[versionIndex:], '+'); offset >= 0 {
		return ModuleInstance{value: r.value[:versionIndex+offset]}
	}
	return ModuleInstance{value: r.value}
}

func (r CanonicalRepo) GetRootPackage() CanonicalPackage {
	return CanonicalPackage{value: "@@" + r.value}
}

func (r CanonicalRepo) HasModuleExtension() bool {
	version := r.value[strings.IndexByte(r.value, '+')+1:]
	return strings.IndexByte(version, '+') >= 0
}
