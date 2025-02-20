package label

import (
	"errors"
	"regexp"
	"strings"
)

const (
	validApparentOrCanonicalRepoPattern = `@(` + validApparentRepoPattern + `|@` + validCanonicalRepoPattern + `)`
	validApparentLabelPattern           = `(` +
		validApparentOrCanonicalRepoPattern + validMaybeAbsoluteLabelPattern + `|` +
		`@@` + validAbsoluteLabelPattern +
		`)`
)

var validApparentLabelRegexp = regexp.MustCompile("^" + validApparentLabelPattern + "$")

var invalidApparentLabelPattern = errors.New("apparent label must match " + validApparentLabelPattern)

// ApparentLabel is a label string that is prefixed with either a
// canonical or apparent repo name. This type can be used to refer to
// a single target within the context of a given repository.
type ApparentLabel struct {
	value string
}

func newValidApparentLabel(value string) ApparentLabel {
	return ApparentLabel{value: removeLabelTargetNameIfRedundant(value)}
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

func hasCanonicalRepo(value string) bool {
	return len(value) > 2 && value[1] == '@' && value[2] != '/'
}

// AsCanonical upgrades an existing ApparentLabel to a CanonicalLabel if
// it prefixed with a canonical repo name.
func (l ApparentLabel) AsCanonical() (CanonicalLabel, bool) {
	if hasCanonicalRepo(l.value) {
		return CanonicalLabel{value: l.value}, true
	}
	return CanonicalLabel{}, false
}

func getApparentRepo(value string) (ApparentRepo, bool) {
	repo := value[1:]
	if repo[0] == '@' {
		return ApparentRepo{}, false
	}
	if offset := strings.IndexByte(repo, '/'); offset > 0 {
		repo = repo[:offset]
	}
	return ApparentRepo{value: repo}, true
}

// GetApparentRepo returns the apparent repo name of the label, if the
// label is not prefixed with a canonical repo name.
func (l ApparentLabel) GetApparentRepo() (ApparentRepo, bool) {
	return getApparentRepo(l.value)
}

// WithCanonicalRepo replaces the repo name of the label with a
// provided canonical repo name.
func (l ApparentLabel) WithCanonicalRepo(canonicalRepo CanonicalRepo) CanonicalLabel {
	return newValidCanonicalLabel(canonicalRepo.applyToLabelOrTargetPattern(l.value))
}

// AsResolvedWithError converts an apparent label to a canonical label
// containing an error message. This method can be called after
// attempting to resolve an apparent repo to a canonical repo fails.
func (l ApparentLabel) AsResolvedWithError(message string) ResolvedLabel {
	if strings.IndexByte(message, ']') >= 0 {
		panic("error message cannot contain closing brackets")
	}

	if offset := strings.IndexByte(l.value, '/'); offset > 0 {
		// Translate "@repo//x/y:z" to "@@[message]//x/y:z".
		return ResolvedLabel{value: "@@[" + message + "]" + l.value[offset:]}
	}
	// Translate "@repo" to "@@[message]//:repo".
	return ResolvedLabel{value: "@@[" + message + "]//:" + strings.TrimLeft(l.value, "@")}
}
