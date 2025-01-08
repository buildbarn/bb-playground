package label

import (
	"errors"
	"regexp"
)

const validApparentTargetPatternPattern = `(` +
	validApparentOrCanonicalRepoPattern + validMaybeAbsoluteTargetPatternPattern + `|` +
	`@@` + validAbsoluteTargetPatternPattern +
	`)`

var validApparentTargetPatternRegexp = regexp.MustCompile("^" + validApparentTargetPatternPattern + "$")

var invalidApparentTargetPatternPattern = errors.New("apparent target pattern must match " + validApparentTargetPatternPattern)

// ApparentTargetPattern is a target pattern string that is prefixed
// with either a canonical or apparent repo name. This type can be used
// to refer to zero or more targets within the context of a given
// repository.
type ApparentTargetPattern struct {
	value string
}

func newValidApparentTargetPattern(value string) ApparentTargetPattern {
	return ApparentTargetPattern{value: removeTargetPatternTargetNameIfRedundant(value)}
}

func NewApparentTargetPattern(value string) (ApparentTargetPattern, error) {
	if !validApparentTargetPatternRegexp.MatchString(value) {
		return ApparentTargetPattern{}, invalidApparentTargetPatternPattern
	}
	return newValidApparentTargetPattern(value), nil
}

func MustNewApparentTargetPattern(value string) ApparentTargetPattern {
	tp, err := NewApparentTargetPattern(value)
	if err != nil {
		panic(err)
	}
	return tp
}

func (tp ApparentTargetPattern) String() string {
	return tp.value
}

// AsCanonical upgrades an existing ApparentTargetPattern to a
// CanonicalTargetPattern if it prefixed with a canonical repo name.
func (tp ApparentTargetPattern) AsCanonical() (CanonicalTargetPattern, bool) {
	if hasCanonicalRepo(tp.value) {
		return CanonicalTargetPattern{value: tp.value}, true
	}
	return CanonicalTargetPattern{}, false
}

// GetApparentRepo returns the apparent repo name of the target pattern,
// if the target pattern is not prefixed with a canonical repo name.
func (tp ApparentTargetPattern) GetApparentRepo() (ApparentRepo, bool) {
	return getApparentRepo(tp.value)
}

// WithCanonicalRepo replaces the repo name of the target pattern with a
// provided canonical repo name.
func (tp ApparentTargetPattern) WithCanonicalRepo(canonicalRepo CanonicalRepo) CanonicalTargetPattern {
	return newValidCanonicalTargetPattern(canonicalRepo.applyToLabelOrTargetPattern(tp.value))
}
