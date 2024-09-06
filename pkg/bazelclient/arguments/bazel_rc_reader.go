package arguments

import (
	"io"
	"strings"
	"unicode"
)

// BazelRCReader is a reader for ".bazelrc" files. These files may list
// flags that need to be provided to Bazel, either by default or by
// providing --config= flags.
type BazelRCReader struct {
	r io.RuneReader
}

// NewBazelRCReader wraps an io.RuneReader, providing it a BazelRCReader
// that can be used to read lines from ".bazelrc" files.
func NewBazelRCReader(r io.RuneReader) *BazelRCReader {
	return &BazelRCReader{
		r: r,
	}
}

// Read the contents of the ".bazelrc" file, up to and including the
// next line that contains one or more fields of text.
func (r *BazelRCReader) Read() ([]string, error) {
	var fields []string
	for {
		var field strings.Builder
		hasField := false
		reachedEndOfLine := false
		reachedEndOfFile := false

	ParseField:
		for {
			c, _, err := r.r.ReadRune()
			if err != nil {
				if err == io.EOF {
					reachedEndOfLine = true
					reachedEndOfFile = true
					break ParseField
				}
				return nil, err
			}

			switch {
			case c == '#':
				// Start of comment. Consume data until
				// the end of the current line.
				for {
					c, _, err = r.r.ReadRune()
					if err != nil {
						if err == io.EOF {
							reachedEndOfLine = true
							reachedEndOfFile = true
							break ParseField
						}
						return nil, err
					}
					if c == '\n' {
						reachedEndOfLine = true
						break ParseField
					}
				}
			case c == '\'', c == '"':
				// Start of a quoted string. Read until
				// the first non-escaped closing quote
				// character.
				hasField = true
				for {
					cQuoted, _, err := r.r.ReadRune()
					if err != nil {
						if err == io.EOF {
							return nil, io.ErrUnexpectedEOF
						}
						return nil, err
					}
					switch cQuoted {
					case c:
						continue ParseField
					case '\\':
						cEscaped, _, err := r.r.ReadRune()
						if err != nil {
							if err == io.EOF {
								return nil, io.ErrUnexpectedEOF
							}
							return nil, err
						}
						field.WriteRune(cEscaped)
					default:
						field.WriteRune(cQuoted)
					}
				}
			case c == '\\':
				// Escaped character. Apply the next
				// character literally, or skip the next
				// newline character.
				cEscaped, _, err := r.r.ReadRune()
				if err != nil {
					if err == io.EOF {
						return nil, io.ErrUnexpectedEOF
					}
					return nil, err
				}
				if cEscaped != '\n' {
					hasField = true
					field.WriteRune(cEscaped)
				}
			case c == '\n':
				// Terminating newline.
				reachedEndOfLine = true
				break ParseField
			case unicode.IsSpace(c):
				// Field separator.
				break ParseField
			default:
				hasField = true
				field.WriteRune(c)
			}
		}

		if hasField {
			fields = append(fields, field.String())
		}
		if reachedEndOfLine && len(fields) > 0 {
			return fields, nil
		}
		if reachedEndOfFile {
			return nil, io.EOF
		}
	}
}
