package logging

import (
	"os"

	"github.com/buildbarn/bonanza/pkg/bazelclient/arguments"

	"golang.org/x/term"
)

type Logger interface {
	Fatal(v ...any)
	Fatalf(format string, v ...any)
	Info(v ...any)
	Infof(format string, v ...any)
}

func NewLoggerFromFlags(commonFlags *arguments.CommonFlags) Logger {
	w := os.Stderr
	var escapeSequences *EscapeSequences
	switch commonFlags.Color {
	case arguments.Color_Yes:
		escapeSequences = &VT100EscapeSequences
	case arguments.Color_No:
		escapeSequences = &NoEscapeSequences
	case arguments.Color_Auto:
		if term.IsTerminal(int(w.Fd())) {
			escapeSequences = &VT100EscapeSequences
		} else {
			escapeSequences = &NoEscapeSequences
		}
	}
	return NewConsoleLogger(w, escapeSequences)
}
