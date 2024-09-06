package logging

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

type consoleLogger struct {
	w               io.Writer
	escapeSequences *EscapeSequences
}

func NewConsoleLogger(w io.Writer, escapeSequences *EscapeSequences) Logger {
	return &consoleLogger{
		w:               w,
		escapeSequences: escapeSequences,
	}
}

func (l *consoleLogger) Error(v ...any) {
	var b bytes.Buffer

	b.Write(l.escapeSequences.Bold)
	b.Write(l.escapeSequences.Red)
	b.WriteString("ERROR: ")
	b.Write(l.escapeSequences.Reset)
	fmt.Fprint(&b, v...)
	b.Write([]byte{'\n'})

	l.w.Write(b.Bytes())
}

func (l *consoleLogger) Errorf(format string, v ...any) {
	var b bytes.Buffer

	b.Write(l.escapeSequences.Bold)
	b.Write(l.escapeSequences.Red)
	b.WriteString("ERROR: ")
	b.Write(l.escapeSequences.Reset)
	fmt.Fprintf(&b, format, v...)
	b.Write([]byte{'\n'})

	l.w.Write(b.Bytes())
}

func (l *consoleLogger) Fatal(v ...any) {
	l.Error(v...)
	os.Exit(1)
}

func (l *consoleLogger) Fatalf(format string, v ...any) {
	l.Errorf(format, v...)
	os.Exit(1)
}

func (l *consoleLogger) Info(v ...any) {
	var b bytes.Buffer

	b.Write(l.escapeSequences.Green)
	b.WriteString("INFO: ")
	b.Write(l.escapeSequences.Reset)
	fmt.Fprint(&b, v...)
	b.Write([]byte{'\n'})

	l.w.Write(b.Bytes())
}

func (l *consoleLogger) Infof(format string, v ...any) {
	var b bytes.Buffer

	b.Write(l.escapeSequences.Green)
	b.WriteString("INFO: ")
	b.Write(l.escapeSequences.Reset)
	fmt.Fprintf(&b, format, v...)
	b.Write([]byte{'\n'})

	l.w.Write(b.Bytes())
}
