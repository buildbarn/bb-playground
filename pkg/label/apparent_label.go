package label

import (
	"errors"
	"regexp"
	"strings"
)

const validApparentLabelPattern = `@(` + validApparentRepoPattern + `|@` + validCanonicalRepoPattern + `)` + validMaybeAbsoluteLabelPattern

var validApparentLabelRegexp = regexp.MustCompile("^" + validApparentLabelPattern + "$")

var invalidApparentLabelPattern = errors.New("apparent label must match " + validApparentLabelPattern)

// ApparentLabel is a label string that is prefixed with either a
// canonical or apparent repo name. This type can be used to refer to
// targets within the context of a given repository.
type ApparentLabel struct {
	value string
}

func newValidApparentLabel(value string) ApparentLabel {
	return ApparentLabel{value: removeTargetNameIfRedundant(value)}
}

func NewApparentLabel(value string) (ApparentLabel, error) {
	if !validApparentLabelRegexp.MatchString(value) {
		return ApparentLabel{}, invalidApparentLabelPattern
	}
	return newValidApparentLabel(value), nil
}

func MustNewApparentLabel(value string) ApparentLabel {
	l, err := NewApparentLabel(value)
	if err != nil {
		panic(err)
	}
	return l
}

func (l ApparentLabel) String() string {
	return l.value
}

// AsCanonicalLabel upgrades an existing ApparentLabel to a
// CanonicalLabel if it prefixed with a canonical repo name.
func (l ApparentLabel) AsCanonicalLabel() (CanonicalLabel, bool) {
	if l.value[1] == '@' {
		return CanonicalLabel{value: l.value}, true
	}
	return CanonicalLabel{}, false
}

// GetApparentRepo returns the apparent repo name of the label, if the
// label is not prefixed with a canonical repo name.
func (l ApparentLabel) GetApparentRepo() (ApparentRepo, bool) {
	repo := l.value[1:]
	if repo[0] == '@' {
		return ApparentRepo{}, false
	}
	if offset := strings.IndexByte(repo, '/'); offset > 0 {
		repo = repo[:offset]
	}
	return ApparentRepo{value: repo}, true
}

// WithCanonicalRepo replaces the repo name of the label with a
// provided canonical repo name.
func (l ApparentLabel) WithCanonicalRepo(canonicalRepo CanonicalRepo) CanonicalLabel {
	if offset := strings.IndexByte(l.value, '/'); offset > 0 {
		// Translate "@from//x/y:z" to "@@to//x/y:z".
		return newValidCanonicalLabel("@@" + canonicalRepo.value + l.value[offset:])
	}
	// Translate "@from" to "@@to//:from".
	return newValidCanonicalLabel("@@" + canonicalRepo.value + "//:" + strings.TrimLeft(l.value, "@"))
}
