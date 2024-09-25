package label

import (
	"strings"
)

type Label struct {
	value string
}

func NewLabel(value string) (Label, error) {
	// TODO: Validate and normalize!
	return Label{value: value}, nil
}

func MustNewLabel(value string) Label {
	l, err := NewLabel(value)
	if err != nil {
		panic(err)
	}
	return l
}

func (l Label) GetPackageLabel() Label {
	colon := strings.IndexByte(l.value, ':')
	if colon < 0 {
		return l
	}
	newValue := l.value[:colon]
	if len(newValue) > 2 {
		newValue = strings.TrimSuffix(l.value[:colon], "//")
	}
	return Label{value: newValue}
}

func (l Label) GetPackagePath() (string, bool) {
	slashes := strings.Index(l.value, "//")
	if slashes < 0 {
		// Bare "@repo" labels contain no "//", but do
		// correspond to the repo's root.
		return "", strings.HasPrefix(l.value, "@")
	}

	// Trim target name.
	packagePath := l.value[slashes+2:]
	colon := strings.IndexByte(packagePath, ':')
	if colon >= 0 {
		packagePath = packagePath[:colon]
	}
	return packagePath, true
}

func (l Label) GetCanonicalRepo() (CanonicalRepo, bool) {
	if r, ok := strings.CutPrefix(l.value, "@@"); ok {
		return CanonicalRepo{value: r[:strings.IndexByte(r, '/')]}, true
	}
	return CanonicalRepo{}, false
}

func (l Label) String() string {
	return l.value
}
