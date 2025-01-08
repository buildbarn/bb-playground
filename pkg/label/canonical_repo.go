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

func (r CanonicalRepo) GetModuleExtension() (ModuleExtension, ApparentRepo, bool) {
	repoIndex := strings.LastIndexByte(r.value, '+')
	moduleExtension := r.value[:repoIndex]
	if strings.IndexByte(moduleExtension, '+') < 0 {
		return ModuleExtension{}, ApparentRepo{}, false
	}
	return ModuleExtension{value: moduleExtension},
		ApparentRepo{value: r.value[repoIndex+1:]},
		true
}

func (r CanonicalRepo) applyToLabelOrTargetPattern(value string) string {
	if offset := strings.IndexByte(value, '/'); offset > 0 {
		// Translate "@from//x/y:z" to "@@to//x/y:z".
		return "@@" + r.value + value[offset:]
	}
	// Translate "@from" to "@@to//:from".
	return "@@" + r.value + "//:" + strings.TrimLeft(value, "@")
}
