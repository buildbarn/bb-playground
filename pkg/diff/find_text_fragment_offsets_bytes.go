package diff

import (
	"fmt"
	"io"

	"github.com/bluekeyes/go-gitdiff/gitdiff"
)

// match of a fragment of a diff file at a given location inside an input file.
type match struct {
	startingOffsetBytes int64
	startingLine        int64
}

// fragmentState contains the state of a single fragment contained in a
// diff file to apply to a given file.
type fragmentState struct {
	// The data to look for in the input file. The data is prefixed
	// with a newline to simplify the string searching.
	toMatch          []byte
	toMatchLines     int64
	toMatchSizeBytes int

	// Knuth-Morris-Pratt partial match table, used to determine how
	// far matchingBytesCount needs to be rewound upon mismatch.
	partialMatchTable []int
	finalPartialMatch int

	// The number of bytes in toMatch that match the current input.
	matchingBytesCount int

	// The first occurrence of the data to match in the input file,
	// and the occurrence that is closest to the line numbers
	// provided in the diff file.
	firstMatch match
	bestMatch  match
}

// mayFinish returns true if there is no longer any need to search for a
// given fragment. This is the case if the current distance to the
// optimal starting line exceeds that of the last one that was found.
func (s *fragmentState) mayFinish(optimalStartingLine, currentLine int64) bool {
	if s.bestMatch.startingLine == 0 {
		// We didn't find a valid match yet. We must continue.
		return false
	}
	if currentLine < optimalStartingLine {
		// We have a match, but we also haven't even started
		// reading the optimal line of input.
		return false
	}
	if s.bestMatch.startingLine >= optimalStartingLine {
		// The current match is after the optimal line.
		// We aren't going to find anything that is better.
		return true
	}
	// The current match is n lines before the optimal line. Allow
	// the search to continue up to n lines after the optimal line.
	linesAfterOptimal := currentLine - s.toMatchLines - optimalStartingLine
	lastMatchLinesBeforeOptimal := optimalStartingLine - s.bestMatch.startingLine
	return linesAfterOptimal >= lastMatchLinesBeforeOptimal
}

func newFragmentState(fragment *gitdiff.TextFragment) fragmentState {
	// Concatenate all lines context/deleted lines in the fragment
	// to obtain the part to match.
	toMatchSizeBytes := 0
	toMatchLines := int64(0)
	for _, line := range fragment.Lines {
		if line.Op == gitdiff.OpContext || line.Op == gitdiff.OpDelete {
			toMatchSizeBytes += len(line.Line)
			toMatchLines++
		}
	}
	toMatch := append(make([]byte, 0, toMatchSizeBytes+1), '\n')
	for _, line := range fragment.Lines {
		if line.Op == gitdiff.OpContext || line.Op == gitdiff.OpDelete {
			toMatch = append(toMatch, line.Line...)
		}
	}

	// Compute the Knuth-Morris-Pratt partial match table.
	partialMatchTable := append(make([]int, 0, toMatchSizeBytes+1), -1)
	cnd := 0
	for pos := 1; pos < toMatchSizeBytes+1; pos++ {
		if toMatch[pos] == toMatch[cnd] {
			partialMatchTable = append(partialMatchTable, partialMatchTable[cnd])
		} else {
			partialMatchTable = append(partialMatchTable, cnd)
			for cnd > 0 && partialMatchTable[pos] != partialMatchTable[cnd] {
				cnd = partialMatchTable[cnd]
			}
		}
		cnd++
	}

	return fragmentState{
		toMatch:           toMatch,
		toMatchLines:      toMatchLines,
		toMatchSizeBytes:  toMatchSizeBytes,
		partialMatchTable: partialMatchTable,
		finalPartialMatch: cnd,
	}
}

// FindTextFragmentOffsetsBytes scans a file and searches for
// occurrences of diff fragments. If all of the fragments can be found
// in the file, a list of byte offsets is returned at which the
// fragments are present. These can then be provided to
// ReplaceTextFragments() to perform the actual substitution.
func FindTextFragmentOffsetsBytes(fragments []*gitdiff.TextFragment, r io.ByteReader) ([]int64, error) {
	if len(fragments) == 0 {
		return nil, nil
	}

	state := append(make([]fragmentState, 0, len(fragments)), newFragmentState(fragments[0]))
	finishedFragments, checkingFragments := 0, 1
	currentChar := byte('\n')
	currentOffsetBytes := int64(0)
	currentLine := int64(1)
	for {
		// For each of the fragments that are currently being
		// looked for, perform string matching against the input.
		for i := finishedFragments; i < checkingFragments; i++ {
			s := &state[i]
			for currentChar != s.toMatch[s.matchingBytesCount] {
				s.matchingBytesCount = s.partialMatchTable[s.matchingBytesCount]
				if s.matchingBytesCount < 0 {
					break
				}
			}
			s.matchingBytesCount++
			if s.matchingBytesCount == len(s.toMatch) {
				// Found a match. Only rewind partially,
				// so that we consider all offsets at
				// which the fragment matches.
				s.matchingBytesCount = s.finalPartialMatch

				newMatch := match{
					startingOffsetBytes: currentOffsetBytes - int64(s.toMatchSizeBytes),
					startingLine:        currentLine - s.toMatchLines,
				}
				s.bestMatch = newMatch
				if s.firstMatch.startingLine == 0 {
					s.firstMatch = newMatch

					// If this is the first match
					// for a given fragment, we may
					// kick off the search for the
					// next fragment.
					if checkingFragments < len(fragments) {
						state = append(state, newFragmentState(fragments[checkingFragments]))
						checkingFragments++
					}
				}
			}
		}

		// Check if there are any fragments that are no longer
		// worth checking, because any additional occurrence of
		// the fragment would be further from the optimal
		// location than what's already been found.
		for {
			s := &state[finishedFragments]
			if !s.mayFinish(fragments[finishedFragments].OldPosition, currentLine) {
				break
			}

			s.toMatch = nil
			s.partialMatchTable = nil
			finishedFragments++

			// No need to continue scanning the file if
			// we've found the offsets of all fragments.
			if finishedFragments == len(fragments) {
				goto Finished
			}
		}

		var err error
		currentChar, err = r.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		currentOffsetBytes++
		if currentChar == '\n' {
			currentLine++
		}
	}

	if state[len(state)-1].firstMatch.startingLine == 0 {
		return nil, fmt.Errorf("only found matching locations for %d out of %d fragments", checkingFragments-1, len(fragments))
	}

Finished:
	offsetsBytes := make([]int64, len(fragments))
	offsetsBytes[len(fragments)-1] = state[len(fragments)-1].bestMatch.startingOffsetBytes
	for i := len(fragments) - 2; i >= 0; i-- {
		// Preferably select the match that is closest to the
		// originally specified line number. If this is not
		// possible due to fragments overlapping, fall back to
		// the first match. These are guaranteed to not overlap.
		s := &state[i]
		if o := s.bestMatch.startingOffsetBytes; o+int64(s.toMatchSizeBytes) <= offsetsBytes[i+1] {
			offsetsBytes[i] = o
		} else {
			offsetsBytes[i] = s.firstMatch.startingOffsetBytes
		}

		// Offsets that are returned are relative to the end of
		// the previous fragment.
		offsetsBytes[i+1] -= offsetsBytes[i]
		offsetsBytes[i+1] -= int64(s.toMatchSizeBytes)
	}
	return offsetsBytes, nil
}
