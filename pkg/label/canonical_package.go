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
	validCanonicalPackagePattern                = `@@` + validCanonicalRepoPattern + `(//` + validNonEmptyPackageNamePattern + `)?`
	validApparentOrAbsoluteLabelPattern         = `(` + validApparentLabelPattern + `|` + validAbsoluteLabelPattern + `)`
	validRelativeTargetNameLabelPattern         = `((` + validNonEmptyPackageNamePattern + `)?:)?` + validTargetNamePattern
	validApparentOrAbsoluteTargetPatternPattern = `(` + validApparentTargetPatternPattern + `|` + validAbsoluteTargetPatternPattern + `)`
	validRelativeTargetNameTargetPatternPattern = `(` + validRelativeTargetNameLabelPattern + `|` + validRelativeRecursiveTargetPatternPattern + `)`
)

var (
	validCanonicalPackageRegexp                = regexp.MustCompile("^" + validCanonicalPackagePattern + "$")
	validApparentOrAbsoluteLabelRegexp         = regexp.MustCompile("^" + validApparentOrAbsoluteLabelPattern + "$")
	validRelativeTargetNameLabelRegexp         = regexp.MustCompile("^" + validRelativeTargetNameLabelPattern + "$")
	validApparentOrAbsoluteTargetPatternRegexp = regexp.MustCompile("^" + validApparentOrAbsoluteTargetPatternPattern + "$")
	validRelativeTargetNameTargetPatternRegexp = regexp.MustCompile("^" + validRelativeTargetNameTargetPatternPattern + "$")
)

var (
	invalidCanonicalPackagePattern = errors.New("canonical package name must match " + validCanonicalPackagePattern)
	invalidLabelPattern            = errors.New("label must match (" + validApparentOrAbsoluteLabelPattern + "|" + validRelativeTargetNameLabelPattern + ")")
	invalidTargetPatternPattern    = errors.New("target pattern must match (" + validApparentOrAbsoluteTargetPatternPattern + "|" + validRelativeTargetNameTargetPatternPattern + ")")
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

func (p CanonicalPackage) concatenateApparentOrAbsolute(value string) string {
	switch value[0] {
	case '@':
		// Provided label is already apparent.
		return value
	case '/':
		// Provided label is absolute.
		repo := p.value
		if offset := strings.IndexByte(repo, '/'); offset >= 0 {
			repo = repo[:offset]
		}
		return repo + value
	default:
		panic("pattern matched labels that were not apparent or absolute")
	}
}

func (p CanonicalPackage) concatenateRelative(value string, forceHasPackage bool) string {
	midfix := ""
	if forceHasPackage || strings.IndexByte(value, ':') >= 0 {
		// Provided label is relative.
		if strings.IndexByte(p.value, '/') < 0 {
			midfix = "//"
		} else if value[0] != ':' {
			midfix = "/"
		}
	} else {
		// Provided label is a bare target name. Bazel allows
		// those to refer to targets inside the longest matching
		// subpackage (e.g., "a/b/c" maps to either "a/b/c:c",
		// "a/b:c", "a:b/c", or ":a/b/c").
		//
		// We don't support this scheme, as it significantly
		// complicates the resolution process. We always
		// interpret such labels as refering to targets in the
		// current package (i.e., always "a/b/c" maps to
		// ":a/b/c").
		midfix = ":"
		if strings.IndexByte(p.value, '/') < 0 {
			midfix = "//:"
		}
	}
	return p.value + midfix + value
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
		return newValidApparentLabel(p.concatenateApparentOrAbsolute(value)), nil
	}
	if validRelativeTargetNameLabelRegexp.MatchString(value) {
		return newValidApparentLabel(p.concatenateRelative(value, false)), nil
	}
	return ApparentLabel{}, invalidLabelPattern
}

// AppendTargetName appends a target name to a canonical package name,
// thereby turning it into a canonical label.
func (p CanonicalPackage) AppendTargetName(targetName TargetName) CanonicalLabel {
	midfix := ":"
	if strings.IndexByte(p.value, '/') < 0 {
		midfix = "//:"
	}
	return newValidCanonicalLabel(p.value + midfix + targetName.value)
}

// AppendTargetPattern appends a provided target pattern value to a
// canonical package name. The resulting target pattern is apparent,
// because the provided label value may be prefixed with an apparent
// repo. It is the caller's responsibility to resolve the apparent repo
// to a canonical one.
func (p CanonicalPackage) AppendTargetPattern(value string) (ApparentTargetPattern, error) {
	if validApparentOrAbsoluteTargetPatternRegexp.MatchString(value) {
		return newValidApparentTargetPattern(p.concatenateApparentOrAbsolute(value)), nil
	}
	if validRelativeTargetNameTargetPatternRegexp.MatchString(value) {
		// Even though "a/b/c" should be interpreted as a target
		// name (":a/b/c"), "a/b/..." should be interpreted as a
		// wildcard package match.
		forceHasPackage := value == "..." || strings.HasSuffix(value, "/...")
		return newValidApparentTargetPattern(p.concatenateRelative(value, forceHasPackage)), nil
	}
	return ApparentTargetPattern{}, invalidTargetPatternPattern
}

var validNonEmptyPackageNameRegexp = regexp.MustCompile("^" + validNonEmptyPackageNamePattern + "$")

var invalidNonEmptyPackageNamePattern = errors.New("non-empty package name must match " + validNonEmptyPackageNamePattern)

// ToRecursiveTargetPatternBelow appends a relative package path to a
// canonical package name, and subsequently converts it to a recursive
// target pattern.
//
// This method can be used during expansion of recursive target patterns
// to split up work, so that subpackages can be traversed in parallel.
func (p CanonicalPackage) ToRecursiveTargetPatternBelow(pathBelow string, includeFileTargets bool) (CanonicalTargetPattern, error) {
	if !validNonEmptyPackageNameRegexp.MatchString(pathBelow) {
		return CanonicalTargetPattern{}, invalidNonEmptyPackageNamePattern
	}

	midfix := "/"
	if strings.IndexByte(p.value, '/') < 0 {
		midfix = "//"
	}
	suffix := "/..."
	if includeFileTargets {
		suffix = "/...:*"
	}
	return CanonicalTargetPattern{value: p.value + midfix + pathBelow + suffix}, nil
}

func (p CanonicalPackage) String() string {
	return p.value
}
