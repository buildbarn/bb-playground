package label

import (
	"errors"
	"regexp"
	"strings"
)

// CanonicalPackage is a label string that refers to a package, meaning
// it does not include a target name. Furthermore, it is prefixed with a
// canonical repo name.
type CanonicalPackage struct {
	value string
}

const (
	validCanonicalPackagePattern        = `@@` + validCanonicalRepoPattern + `(//` + validNonEmptyPackageNamePattern + `)?`
	validApparentOrAbsoluteLabelPattern = `(` + validApparentLabelPattern + `|` + validAbsoluteLabelPattern + `)`
	validRelativeTargetNameLabelPattern = `((` + validNonEmptyPackageNamePattern + `)?:)?` + validTargetNamePattern
)

var (
	validCanonicalPackageRegexp        = regexp.MustCompile("^" + validCanonicalPackagePattern + "$")
	validApparentOrAbsoluteLabelRegexp = regexp.MustCompile("^" + validApparentOrAbsoluteLabelPattern + "$")
	validRelativeTargetNameLabelRegexp = regexp.MustCompile("^" + validRelativeTargetNameLabelPattern + "$")
)

var (
	invalidCanonicalPackagePattern = errors.New("canonical package name must match " + validCanonicalPackagePattern)
	invalidLabelPattern            = errors.New("label must match (" + validApparentOrAbsoluteLabelPattern + "|" + validRelativeTargetNameLabelPattern + ")")
)

func NewCanonicalPackage(value string) (CanonicalPackage, error) {
	if !validCanonicalPackageRegexp.MatchString(value) {
		return CanonicalPackage{}, invalidCanonicalPackagePattern
	}
	return CanonicalPackage{value: value}, nil
}

func MustNewCanonicalPackage(value string) CanonicalPackage {
	p, err := NewCanonicalPackage(value)
	if err != nil {
		panic(err)
	}
	return p
}

func (p CanonicalPackage) GetCanonicalRepo() CanonicalRepo {
	repo := p.value[2:]
	if offset := strings.IndexByte(repo, '/'); offset >= 0 {
		repo = repo[:offset]
	}
	return CanonicalRepo{value: repo}
}

func (p CanonicalPackage) GetPackagePath() string {
	if offset := strings.IndexByte(p.value, '/'); offset >= 0 {
		return p.value[offset+2:]
	}
	return ""
}

// AppendLabel appends a provided label value to a canonical package
// name. The resulting label is apparent, because the provided label
// value may be prefixed with an apparent repo. It is the caller's
// responsibility to resolve the apparent repo to a canonical one.
func (p CanonicalPackage) AppendLabel(value string) (ApparentLabel, error) {
	// Parse apparent/absolute labels and relative labels
	// separately. Values such as "@hello" are both valid apparent
	// labels and bare target names. We should prefer parsing those
	// as apparent labels. On the other hand, "@hello/foo" can only
	// be a bare target name, because apparent repo names need to be
	// followed by two slashes.
	if validApparentOrAbsoluteLabelRegexp.MatchString(value) {
		switch value[0] {
		case '@':
			// Provided label is already apparent.
			return newValidApparentLabel(value), nil
		case '/':
			// Provided label is absolute.
			repo := p.value
			if offset := strings.IndexByte(repo, '/'); offset >= 0 {
				repo = repo[:offset]
			}
			return newValidApparentLabel(repo + value), nil
		default:
			panic("pattern matched labels that were not apparent or absolute")
		}
	}

	if validRelativeTargetNameLabelRegexp.MatchString(value) {
		midfix := ""
		if strings.IndexByte(value, ':') < 0 {
			// Provided label is a bare target name. Bazel
			// allows those to refer to targets inside
			// the longest matching subpackage (e.g.,
			// "a/b/c" maps to either "a/b/c:c", "a/b:c",
			// "a:b/c", or ":a/b/c").
			//
			// We don't support this scheme, as it
			// significantly complicates the resolution
			// process. We always interpret such labels as
			// refering to targets in the current package
			// (i.e., always "a/b/c" maps to ":a/b/c").
			midfix = ":"
			if strings.IndexByte(p.value, '/') < 0 {
				midfix = "//:"
			}
		} else {
			// Provided label is relative.
			if strings.IndexByte(p.value, '/') < 0 {
				midfix = "//"
			} else if value[0] != ':' {
				midfix = "/"
			}
		}
		return newValidApparentLabel(p.value + midfix + value), nil
	}

	return ApparentLabel{}, invalidLabelPattern
}

func (p CanonicalPackage) String() string {
	return p.value
}
