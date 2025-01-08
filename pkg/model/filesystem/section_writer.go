package filesystem

import "io"

func NewSectionWriter(w io.WriterAt) *SectionWriter {
	return &SectionWriter{w: w}
}

// SectionWriter provides an implementation of io.Writer on top of
// io.WriterAt. It is similar to io.SectionReader, but then for writes.
type SectionWriter struct {
	w           io.WriterAt
	offsetBytes int64
}

func (w *SectionWriter) GetOffsetBytes() int64 {
	return w.offsetBytes
}

func (w *SectionWriter) Write(p []byte) (int, error) {
	n, err := w.w.WriteAt(p, w.offsetBytes)
	w.offsetBytes += int64(n)
	return n, err
}

func (w *SectionWriter) WriteString(s string) (int, error) {
	n, err := w.w.WriteAt([]byte(s), w.offsetBytes)
	w.offsetBytes += int64(n)
	return n, err
}
