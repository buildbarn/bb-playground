package glob

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"unicode/utf8"

	"github.com/buildbarn/bonanza/pkg/encoding/varint"
)

type state struct {
	isIncluded    bool
	isExcluded    bool
	star          *state
	starStarSlash *state
	runes         map[rune]*state
}

func (s *state) gotRune(r rune) *state {
	if s2, ok := s.runes[r]; ok {
		return s2
	}
	if s.runes == nil {
		s.runes = map[rune]*state{}
	}
	s2 := &state{}
	s.runes[r] = s2
	return s2
}

func maybeCreateState(s **state) *state {
	if *s == nil {
		*s = &state{}
	}
	return *s
}

func (s *state) terminate(included bool) {
	if included {
		s.isIncluded = true
	} else {
		s.isExcluded = true
	}
}

func (s *state) addPattern(pattern string, included bool) error {
	gotRunes := false
	stars := 0
	previousDirectoryEnd := &state{}
	starStarAllowed := true
	for _, r := range pattern {
		switch r {
		case '*':
			switch stars {
			case 1:
				if gotRunes {
					return errors.New("\"**\" can not be placed inside a component")
				}
			case 2:
				return errors.New("\"***\" has no special meaning")
			}
			stars++
		case '/':
			switch stars {
			case 0:
				if !gotRunes {
					return errors.New("pathname components cannot be empty")
				}

				previousDirectoryEnd = s
				s = s.gotRune('/')
				starStarAllowed = true
			case 1:
				s = maybeCreateState(&s.star)

				previousDirectoryEnd = s
				s = s.gotRune('/')
				starStarAllowed = true
			case 2:
				if !starStarAllowed {
					return errors.New("redundant \"**\"")
				}

				s = maybeCreateState(&s.starStarSlash)
				starStarAllowed = false
			default:
				panic("unexpected number of stars")
			}
			gotRunes = false
			stars = 0
		default:
			switch stars {
			case 1:
				s = maybeCreateState(&s.star)
			case 2:
				return errors.New("\"**\" can not be placed inside a component")
			}
			s = s.gotRune(r)
			gotRunes = true
			stars = 0
		}
	}

	switch stars {
	case 0:
		if !gotRunes {
			return errors.New("pathname components cannot be empty")
		}
	case 1:
		s = maybeCreateState(&s.star)
	case 2:
		if !starStarAllowed {
			return errors.New("redundant \"**\"")
		}

		previousDirectoryEnd.terminate(included)
		s = maybeCreateState(&s.starStarSlash)
		s = maybeCreateState(&s.star)
	default:
		panic("unexpected number of stars")
	}
	s.terminate(included)
	return nil
}

// Compile a set of patterns into a nondeterministic finite automaton
// (NFA) that is capable of matching paths.
func Compile(includes, excludes []string) ([]byte, error) {
	var start state
	for _, pattern := range includes {
		if err := start.addPattern(pattern, true); err != nil {
			return nil, fmt.Errorf("invalid \"includes\" pattern %#v: %w", pattern, err)
		}
	}
	for _, pattern := range excludes {
		if err := start.addPattern(pattern, false); err != nil {
			return nil, fmt.Errorf("invalid \"excludes\" pattern %#v: %w", pattern, err)
		}
	}

	var nfa []byte
	statesToEmit := []*state{&start}
	for len(statesToEmit) > 0 {
		s := statesToEmit[0]
		statesToEmit = statesToEmit[1:]

		// Emit a tag for the state, indicating whether paths
		// that terminate in the state are part of the results,
		// whether the state is followed by a "*" or "**", and
		// the number of outgoing edges for runes.
		tag := 0
		if s.isExcluded {
			tag |= 1 << 1
		} else if s.isIncluded {
			tag |= 1 << 0
		}
		if s.star != nil {
			tag |= 1 << 2
			statesToEmit = append(statesToEmit, s.star)
		}
		if s.starStarSlash != nil {
			tag |= 1 << 3
			statesToEmit = append(statesToEmit, s.starStarSlash)
		}
		tag |= len(s.runes) << 4
		nfa = varint.AppendForward(nfa, tag)

		// Emit the runes of the outgoing edges for this state.
		for _, r := range slices.Sorted(maps.Keys(s.runes)) {
			nfa = utf8.AppendRune(nfa, r)
			statesToEmit = append(statesToEmit, s.runes[r])
		}
	}
	return nfa, nil
}
