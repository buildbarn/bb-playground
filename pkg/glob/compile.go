package glob

import (
	"errors"
	"fmt"
	"math/bits"
	"unicode"
	"unicode/utf8"

	"github.com/buildbarn/bonanza/pkg/encoding/varint"
)

const (
	maxRuneBits = 21

	stateBits1 = (64 - 21) / 2
	stateMask1 = 1<<stateBits1 - 1

	stateBits2 = 2 * stateBits1
	stateMask2 = 1<<stateBits2 - 1
)

func init() {
	if bits.Len(unicode.MaxRune) > maxRuneBits {
		panic("maxRuneBits is too small")
	}
}

// basicState keeps track of basic properties of a state of the NFA for
// parsing glob patterns. The following data is included:
//
//   - Bottom bits: If non-zero, the current state has an outgoing edge
//     for wildcard "*". The index of the target state is provided.
//
//   - Middle bits: If non-zero, the current state has an outgoing edge
//     for wildcard "**/". The index of the target state is provided.
//
//   - Top bits: Whether the current state is an end state.
type basicState uint64

func (s basicState) getEndState() int {
	return int(s >> stateBits2)
}

func (s *basicState) setEndState(mode int) {
	*s = *s&stateMask2 | basicState(mode)<<stateBits2
}

func (s basicState) getStarIndex() uint32 {
	return uint32(s & stateMask1)
}

func (s *basicState) setStarIndex(i uint32) {
	*s = *s&^stateMask1 | basicState(i)
}

func (s basicState) getStarStarSlashIndex() uint32 {
	return uint32(s >> stateBits1 & stateMask1)
}

func (s *basicState) setStarStarSlashIndex(i uint32) {
	*s = *s&^(stateMask1<<stateBits1) | (basicState(i) << stateBits1)
}

// compiledState keeps track of all properties of a state of the NFA for
// parsing glob patterns that is currently being compiled.
type compiledState struct {
	// Outgoing edge transitions for wildcards "*" and "**/", and whether
	// the current state is an end state.
	basicState

	// Outgoing edge transitions for regular characters. These are
	// stored in the form of a linked list, where each state
	// references its sibling. This fields holds the following data:
	//
	// - Bottom bits: If non-zero, the current state has one or more
	//   outgoing references for non-wildcard characters. The index
	//   of the state associated with the first outgoing edge is
	//   provided.
	//
	// - Middle bits: If non-zero, the parent state has multiple
	//   outgoing references for non-wildcard characters. The index
	//   of the next sibling of the current state is provided.
	//
	// - Top bits: The rune associated with the outgoing edge
	//   pointing from the parent state to the current state.
	runes uint64
}

func (s *compiledState) getCurrentRune() rune {
	return rune(s.runes >> stateBits2)
}

func (s *compiledState) initializeRune(r rune, nextRuneIndex uint32) {
	s.runes = (uint64(r) << stateBits2) | uint64(nextRuneIndex)<<stateBits1
}

func (s *compiledState) getFirstRuneIndex() uint32 {
	return uint32(s.runes & stateMask1)
}

func (s *compiledState) setFirstRuneIndex(i uint32) {
	s.runes = s.runes&^stateMask1 | uint64(i)
}

func (s *compiledState) getNextRuneIndex() uint32 {
	return uint32(s.runes >> stateBits1 & stateMask1)
}

func (s *compiledState) setNextRuneIndex(i uint32) {
	s.runes = s.runes&^(stateMask1<<stateBits1) | (uint64(i) << stateBits1)
}

// addState attaches a new state to the NFA that is being constructed.
// The newly created state has no outgoing edges.
func addState(states *[]compiledState) (uint32, error) {
	i := uint32(len(*states))
	if i > stateMask1 {
		return 0, fmt.Errorf("patterns require more than %d states to represent", stateMask1)
	}
	*states = append(*states, compiledState{})
	return i, nil
}

// getOrAddRune returns the index of the state associated with the
// outgoing edge for a non-wildcard character. A new outgoing edge and
// state are created if none exists yet.
func getOrAddRune(states *[]compiledState, currentState uint32, r rune) (uint32, error) {
	// Find the spot at which to insert a new outgoing edge. We want
	// to keep outgoing edges sorted by rune, so that the output
	// remains stable even if patterns are reordered.
	previousState, compareState := uint32(0), (*states)[currentState].getFirstRuneIndex()
	for ; compareState != 0; previousState, compareState = compareState, (*states)[compareState].getNextRuneIndex() {
		if cr := (*states)[compareState].getCurrentRune(); cr == r {
			// Found an existing outgoing edge.
			return compareState, nil
		} else if cr > r {
			break
		}
	}

	// No outgoing edge found. Create a new state and insert an
	// outgoing edge in the right spot.
	i, err := addState(states)
	if err != nil {
		return 0, err
	}
	if previousState == 0 {
		(*states)[currentState].setFirstRuneIndex(i)
	} else {
		(*states)[previousState].setNextRuneIndex(i)
	}
	(*states)[i].initializeRune(r, compareState)
	return i, nil
}

// getOrAddRune returns the index of the state associated with the
// outgoing edge for wildcard "*". A new outgoing edge and state are
// created if none exists yet.
func getOrAddStar(states *[]compiledState, currentState uint32) (uint32, error) {
	if i := (*states)[currentState].getStarIndex(); i != 0 {
		return i, nil
	}
	i, err := addState(states)
	if err != nil {
		return 0, err
	}
	(*states)[currentState].setStarIndex(i)
	return i, nil
}

// getOrAddRune returns the index of the state associated with the
// outgoing edge for wildcard "**/". A new outgoing edge and state are
// created if none exists yet.
func getOrAddStarStarSlash(states *[]compiledState, currentState uint32) (uint32, error) {
	if i := (*states)[currentState].getStarStarSlashIndex(); i != 0 {
		return i, nil
	}
	i, err := addState(states)
	if err != nil {
		return 0, err
	}
	(*states)[currentState].setStarStarSlashIndex(i)
	return i, nil
}

func addPattern(states *[]compiledState, pattern string, terminationMode int) error {
	// Walk over the pattern and add states to the NFA for each
	// construct we encounter.
	gotRunes := false
	gotStarStarSlash := false
	gotStars := 0
	previousDirectoryEndState := uint32(0)
	currentState := uint32(0)
	var err error
	for _, r := range pattern {
		if r == '*' {
			switch gotStars {
			case 1:
				if gotRunes {
					return errors.New("\"**\" can not be placed inside a component")
				}
			case 2:
				return errors.New("\"***\" has no special meaning")
			}
			gotStars++
		} else {
			if r == '/' {
				if gotStars == 2 {
					if gotStarStarSlash {
						return errors.New("redundant \"**\"")
					}

					currentState, err = getOrAddStarStarSlash(states, currentState)
					if err != nil {
						return err
					}
					gotStarStarSlash = true
				} else {
					switch gotStars {
					case 0:
						if !gotRunes {
							return errors.New("pathname components cannot be empty")
						}
					case 1:
						currentState, err = getOrAddStar(states, currentState)
						if err != nil {
							return err
						}
					default:
						panic("unexpected number of stars")
					}

					previousDirectoryEndState = currentState
					currentState, err = getOrAddRune(states, currentState, '/')
					if err != nil {
						return err
					}
					gotStarStarSlash = false
				}
				gotRunes = false
			} else {
				switch gotStars {
				case 1:
					currentState, err = getOrAddStar(states, currentState)
					if err != nil {
						return err
					}
				case 2:
					return errors.New("\"**\" can not be placed inside a component")
				}
				currentState, err = getOrAddRune(states, currentState, r)
				if err != nil {
					return err
				}
				gotRunes = true
			}
			gotStars = 0
		}
	}

	// Process any trailing stars.
	switch gotStars {
	case 0:
		if !gotRunes {
			return errors.New("pathname components cannot be empty")
		}
	case 1:
		currentState, err = getOrAddStar(states, currentState)
		if err != nil {
			return err
		}
	case 2:
		if gotStarStarSlash {
			return errors.New("redundant \"**\"")
		}

		// Convert any trailing "**" to "**/*". For consistency
		// with Bazel, a bare "**" does not match the root
		// directory, while "foo/**" does match "foo".
		if previousDirectoryEndState > 0 {
			(*states)[previousDirectoryEndState].setEndState(terminationMode)
		}
		currentState, err = getOrAddStarStarSlash(states, currentState)
		if err != nil {
			return err
		}
		currentState, err = getOrAddStar(states, currentState)
		if err != nil {
			return err
		}
	default:
		panic("unexpected number of stars")
	}
	(*states)[currentState].setEndState(terminationMode)
	return nil
}

// Compile a set of glob patterns into a nondeterministic finite
// automaton (NFA) that is capable of matching paths.
func Compile(includes, excludes []string) ([]byte, error) {
	states := []compiledState{{}}
	for _, pattern := range includes {
		if err := addPattern(&states, pattern, 1); err != nil {
			return nil, fmt.Errorf("invalid \"includes\" pattern %#v: %w", pattern, err)
		}
	}
	for _, pattern := range excludes {
		if err := addPattern(&states, pattern, 2); err != nil {
			return nil, fmt.Errorf("invalid \"excludes\" pattern %#v: %w", pattern, err)
		}
	}

	var nfa []byte
	statesToEmit := []uint32{0}
	for len(statesToEmit) > 0 {
		s := &states[statesToEmit[0]]
		statesToEmit = statesToEmit[1:]

		// Emit a tag for the state, indicating whether paths
		// that terminate in the state are part of the results,
		// whether the state is followed by a "*" or "**", and
		// the number of outgoing edges for runes.
		tag := s.getEndState()
		if i := s.getStarIndex(); i != 0 {
			tag |= 1 << 2
			statesToEmit = append(statesToEmit, i)
		}
		if i := s.getStarStarSlashIndex(); i != 0 {
			tag |= 1 << 3
			statesToEmit = append(statesToEmit, i)
		}
		for i := s.getFirstRuneIndex(); i != 0; i = states[i].getNextRuneIndex() {
			tag += 1 << 4
			statesToEmit = append(statesToEmit, i)
		}
		nfa = varint.AppendForward(nfa, tag)

		// Emit the runes of the outgoing edges for this state.
		for i := s.getFirstRuneIndex(); i != 0; i = states[i].getNextRuneIndex() {
			nfa = utf8.AppendRune(nfa, states[i].getCurrentRune())
		}

	}
	return nfa, nil
}
