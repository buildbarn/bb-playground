package label

import (
	"errors"
	"regexp"
)

type ApparentRepo struct {
	value string
}

const validApparentRepoPattern = "[a-zA-Z][-.\\w]*"

var validApparentRepoRegexp = regexp.MustCompile("^" + validApparentRepoPattern + "$")

var invalidApparentRepoPattern = errors.New("apparent repo name must match " + validApparentRepoPattern)

func NewApparentRepo(value string) (ApparentRepo, error) {
	if !validApparentRepoRegexp.MatchString(value) {
		return ApparentRepo{}, invalidApparentRepoPattern
	}
	return ApparentRepo{value: value}, nil
}

func MustNewApparentRepo(value string) ApparentRepo {
	r, err := NewApparentRepo(value)
	if err != nil {
		panic(err)
	}
	return r
}

func (r ApparentRepo) String() string {
	return r.value
}
