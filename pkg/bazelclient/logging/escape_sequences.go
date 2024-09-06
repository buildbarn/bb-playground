package logging

type EscapeSequences struct {
	Reset []byte

	Bold []byte

	Red   []byte
	Green []byte
}

var (
	NoEscapeSequences    = EscapeSequences{}
	VT100EscapeSequences = EscapeSequences{
		Reset: []byte("\x1b[m"),

		Bold: []byte("\x1b[1m"),

		Red:   []byte("\x1b[31m"),
		Green: []byte("\x1b[32m"),
	}
)
