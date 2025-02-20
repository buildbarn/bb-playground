package label

import (
	"errors"
	"regexp"
	"strings"
)

// CanonicalLabel is a label string that is prefixed with a canonical
// repo name. This type can be used to refer to a single target in an
// unambiguous way.
type CanonicalLabel struct {
	value string
}

const (
	// Components of package and target names may contain all
	// printable characters, except backslash, forward slash, and
	// colon.
	validPackageTargetNameCharacterPattern       = `[^[:cntrl:]/:\\]`
	validPackageTargetNameNonDotCharacterPattern = `[^[:cntrl:]./:\\]`

	// Package name components may not consist solely of dots.
	validPackageNameComponentPattern = validPackageTargetNameCharacterPattern + `*` +
		validPackageTargetNameNonDotCharacterPattern +
		validPackageTargetNameCharacterPattern + `*`
	validNonEmptyPackageNamePattern = validPackageNameComponentPattern + `(/` + validPackageNameComponentPattern + `)*`

	// Target name components may consist solely of dots, as long as
	// there are more than two. Valid target name components MUST be
	// a superset of valid package name components. Otherwise, it
	// wouldn't be safe to translate //a/b to //a/b:b.
	validTargetNameComponentPattern = `(` + validPackageNameComponentPattern + `|\.\.+)`
	validTargetNamePattern          = validTargetNameComponentPattern + `(/` + validTargetNameComponentPattern + `)*`

	validAbsoluteLabelPattern = `//(` +
		`:` + validTargetNamePattern + `|` +
		validNonEmptyPackageNamePattern + `(:` + validTargetNamePattern + `)?` +
		`)`
	validMaybeAbsoluteLabelPattern = `(` + validAbsoluteLabelPattern + `)?`

	validCanonicalLabelPattern = `@@` + validCanonicalRepoPattern + validMaybeAbsoluteLabelPattern
)

var validCanonicalLabelRegexp = regexp.MustCompile("^" + validCanonicalLabelPattern + "$")

var invalidCanonicalLabelPattern = errors.New("canonical label must match " + validCanonicalLabelPattern)

// removeLabelTargetNameIfRedundant checks whether the target name
// contained in a label is identical to the last component of the repo
// name (@foo//:foo) or package path (@foo//bar:bar). If so, the target
// name is removed.
func removeLabelTargetNameIfRedundant(label string) string {
	if targetNameOffset := strings.IndexByte(label, ':'); targetNameOffset >= 0 {
		targetName := label[targetNameOffset+1:]
		canonicalPackage := label[:targetNameOffset]
		var lastComponent string
		if canonicalRepo, ok := strings.CutSuffix(canonicalPackage, "//"); ok {
			if strings.HasPrefix(canonicalRepo, "@@[") {
				// Resolved label of shape
				// "@@[error message]//:foo". These
				// labels should always keep their
				// target name. We shouldn't convert
				// "@@[x]//:[x]" to "@@[x]".
				return label
			}
			canonicalPackage = canonicalRepo
			lastComponent = strings.TrimLeft(canonicalPackage, "@")
		} else {
			lastComponent = canonicalPackage[strings.LastIndexByte(canonicalPackage, '/')+1:]
		}
		if targetName == lastComponent {
			label = canonicalPackage
		}
	}
	return label
}

// NewCanonicalLabel creates a new CanonicalLabel based on the provided
// label value.
func NewCanonicalLabel(value string) (CanonicalLabel, error) {
	if !validCanonicalLabelRegexp.MatchString(value) {
		return CanonicalLabel{}, invalidCanonicalLabelPattern
	}
	return newValidCanonicalLabel(value), nil
}

func newValidCanonicalLabel(value string) CanonicalLabel {
	return CanonicalLabel{value: removeLabelTargetNameIfRedundant(value)}
}

func MustNewCanonicalLabel(value string) CanonicalLabel {
	l, err := NewCanonicalLabel(value)
	if err != nil {
		panic(err)
	}
	return l
}

// GetCanonicalPackage strips the target name from a label, thereby
// returning the canonical package name.
func (l CanonicalLabel) GetCanonicalPackage() CanonicalPackage {
	if offset := strings.IndexByte(l.value, ':'); offset >= 0 {
		return CanonicalPackage{value: strings.TrimSuffix(l.value[:offset], "//")}
	}
	return CanonicalPackage{value: l.value}
}

func (l CanonicalLabel) GetCanonicalRepo() CanonicalRepo {
	repo := l.value[2:]
	if offset := strings.IndexByte(repo, '/'); offset >= 0 {
		repo = repo[:offset]
	}
	return CanonicalRepo{value: repo}
}

// GetTargetName returns the name of the target within a package.
func (l CanonicalLabel) GetTargetName() TargetName {
	if offset := strings.IndexByte(l.value, ':'); offset >= 0 {
		// Label has an explicit target name.
		return TargetName{value: l.value[offset+1:]}
	}
	if offset := strings.LastIndexByte(l.value, '/'); offset >= 0 {
		// Derive the target name from the last component.
		return TargetName{value: l.value[offset+1:]}
	}
	// Derive the target name from the repo name.
	return TargetName{value: strings.TrimLeft(l.value, "@")}
}

// GetRepoRelativePath converts a label to a pathname string that is
// relative to the root of the repo. This can be used in contexts where
// labels refer to paths of input files.
func (l CanonicalLabel) GetRepoRelativePath() string {
	label := l.value[2:]
	if targetNameOffset := strings.IndexByte(label, ':'); targetNameOffset >= 0 {
		repoAndPackage := label[:targetNameOffset]
		targetName := label[targetNameOffset+1:]
		pkg := repoAndPackage[strings.IndexByte(repoAndPackage, '/')+2:]
		if len(pkg) == 0 {
			// @@a//:b -> b.
			return targetName
		}
		// @@a//b/c:d -> b/c/d.
		return pkg + "/" + targetName
	}
	if slashOffset := strings.IndexByte(label, '/'); slashOffset >= 0 {
		// @@a//b/c -> b/c/c.
		return label[slashOffset+2:] + "/" + label[strings.LastIndexByte(label, '/')+1:]
	}
	// @@a -> a.
	return label
}

func (l CanonicalLabel) String() string {
	return l.value
}

func (l CanonicalLabel) AppendStarlarkIdentifier(identifier StarlarkIdentifier) CanonicalStarlarkIdentifier {
	return CanonicalStarlarkIdentifier{
		value: l.value + "%" + identifier.value,
	}
}

func (l CanonicalLabel) AsResolved() ResolvedLabel {
	// All canonical labels are also valid resolved labels.
	return ResolvedLabel{value: l.value}
}
