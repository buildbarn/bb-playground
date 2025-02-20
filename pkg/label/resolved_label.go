package label

import (
	"errors"
	"regexp"
	"strings"
)

const validResolvedLabelPattern = `(` + validCanonicalLabelPattern + `|` +
	`@@\[[^\]]+\]` + validAbsoluteLabelPattern + `)`

var validResolvedLabelRegexp = regexp.MustCompile("^" + validResolvedLabelPattern + "$")

var invalidResolvedLabelPattern = errors.New("resolved label must match " + validResolvedLabelPattern)

// ResolvedLabel corresponds to a label for which resolution to a
// canonical label has been attempted. The label is either a canonical
// label, or it is a label for which resolution to a canonical label
// failed. In that case the repo name consists of an error message
// enclosed in square brackets (i.e. @@[error message]//package:target).
type ResolvedLabel struct {
	value string
}

// NewResolvedLabel creates a new label for which resolution to a
// canonical label has been attempted.
func NewResolvedLabel(value string) (ResolvedLabel, error) {
	if !validResolvedLabelRegexp.MatchString(value) {
		return ResolvedLabel{}, invalidResolvedLabelPattern
	}
	return ResolvedLabel{value: removeLabelTargetNameIfRedundant(value)}, nil
}

func MustNewResolvedLabel(value string) ResolvedLabel {
	l, err := NewResolvedLabel(value)
	if err != nil {
		panic(err)
	}
	return l
}

func (l ResolvedLabel) String() string {
	return l.value
}

func (l ResolvedLabel) asCanonical() (CanonicalLabel, bool) {
	if l.value[2] != '[' {
		return CanonicalLabel{value: l.value}, true
	}
	return CanonicalLabel{}, false
}

// AsCanonical returns the canonical label corresponding to the current
// label value if the label does not contain an error message.
func (l ResolvedLabel) AsCanonical() (CanonicalLabel, error) {
	if canonicalLabel, ok := l.asCanonical(); ok {
		return canonicalLabel, nil
	}

	// Extract the error message out of the label.
	errorMessage := l.value[3:]
	return CanonicalLabel{}, errors.New(errorMessage[:strings.IndexByte(errorMessage, ']')])
}

// GetPackagePath returns the package path of the resolved label.
func (l ResolvedLabel) GetPackagePath() string {
	if canonicalLabel, ok := l.asCanonical(); ok {
		return canonicalLabel.GetCanonicalPackage().GetPackagePath()
	}

	// Label has an error message. Skip past the error message first
	// before attempting to obtain the package path.
	errorMessage := l.value[3:]
	packagePath := errorMessage[strings.IndexByte(errorMessage, ']')+3:]
	if colonOffset := strings.IndexByte(packagePath, ':'); colonOffset >= 0 {
		packagePath = packagePath[:colonOffset]
	}
	return packagePath
}

// GetTargetName returns the target name of the resolved label.
func (l ResolvedLabel) GetTargetName() TargetName {
	if canonicalLabel, ok := l.asCanonical(); ok {
		return canonicalLabel.GetTargetName()
	}

	// Label has an error message. Skip past the error message first
	// before attempting to obtain the package path.
	errorMessage := l.value[3:]
	packagePath := errorMessage[strings.IndexByte(errorMessage, ']')+2:]
	if colonOffset := strings.IndexByte(packagePath, ':'); colonOffset >= 0 {
		return TargetName{value: packagePath[colonOffset+1:]}
	}
	return TargetName{value: packagePath[strings.LastIndexByte(packagePath, '/')+1:]}
}
