package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Writer encodes RESP2 frames onto a buffered stream.
//
// A Writer is not safe for concurrent use. Each connection owns one Writer, so
// a slow client can never block another connection on a shared lock.
type Writer struct {
	ew *errWriter
	bw *bufio.Writer

	// MaxBulkBytes bounds the size of a single encoded bulk string or array
	// element set. Zero means the hard ceiling is used. This protects the
	// server from encoding a response the client cannot reasonably consume.
	MaxBulkBytes int

	num [24]byte
}

// errWriter latches the first write error. bufio.Writer absorbs and hides the
// underlying error, and a connection must surface it, so the encoder records
// it here as well.
type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) Write(p []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}
	n, err := e.w.Write(p)
	if err != nil {
		e.err = err
	}
	return n, err
}

// NewWriter wraps w in a buffered RESP2 encoder.
func NewWriter(w io.Writer) *Writer {
	ew := &errWriter{w: w}
	return &Writer{ew: ew, bw: bufio.NewWriterSize(ew, 16<<10)}
}

// Write encodes one value.
//
// Simple strings and errors are validated: they must not contain CR, LF or NUL,
// because a newline inside them would let a response inject protocol frames.
// Use Bulk for arbitrary bytes; GET, MGET and friends must never reply with a
// simple string.
func (w *Writer) Write(v Value) error {
	switch v.Type {
	case TypeSimpleString:
		if err := checkLineSafe(v.Str); err != nil {
			return err
		}
		_ = w.bw.WriteByte('+')
		_, _ = w.bw.Write(v.Str)
		_, _ = w.bw.WriteString("\r\n")

	case TypeError:
		if err := checkLineSafe(v.Str); err != nil {
			return err
		}
		_ = w.bw.WriteByte('-')
		_, _ = w.bw.Write(v.Str)
		_, _ = w.bw.WriteString("\r\n")

	case TypeInteger:
		_ = w.bw.WriteByte(':')
		_, _ = w.bw.Write(strconv.AppendInt(w.num[:0], v.Int, 10))
		_, _ = w.bw.WriteString("\r\n")

	case TypeBulkString:
		if v.IsNull {
			_, _ = w.bw.WriteString("$-1\r\n")
			break
		}
		if err := w.checkBulk(len(v.Str)); err != nil {
			return err
		}
		w.writeLen('$', len(v.Str))
		_, _ = w.bw.Write(v.Str)
		_, _ = w.bw.WriteString("\r\n")

	case TypeArray:
		if v.IsNull {
			_, _ = w.bw.WriteString("*-1\r\n")
			break
		}
		if len(v.Array) > HardMaxArrayLen {
			return fmt.Errorf("protocol: array of %d elements exceeds the encodable limit of %d", len(v.Array), HardMaxArrayLen)
		}
		w.writeLen('*', len(v.Array))
		for i := range v.Array {
			if err := w.Write(v.Array[i]); err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("protocol: cannot encode a value of type %s", v.Type)
	}
	return w.ew.err
}

// checkBulk rejects a bulk string that is larger than the configured ceiling.
func (w *Writer) checkBulk(n int) error {
	limit := w.MaxBulkBytes
	if limit <= 0 || limit > HardMaxBulkBytes {
		limit = HardMaxBulkBytes
	}
	if n > limit {
		return fmt.Errorf("protocol: bulk string of %d bytes exceeds the %d byte response limit", n, limit)
	}
	return nil
}

func (w *Writer) writeLen(prefix byte, n int) {
	_ = w.bw.WriteByte(prefix)
	_, _ = w.bw.Write(strconv.AppendInt(w.num[:0], int64(n), 10))
	_, _ = w.bw.WriteString("\r\n")
}

// Flush writes any buffered frames to the underlying writer.
func (w *Writer) Flush() error { return w.bw.Flush() }

// Buffered reports how many encoded bytes have not been flushed yet.
func (w *Writer) Buffered() int { return w.bw.Buffered() }

// checkLineSafe rejects content that would break a CRLF-delimited line.
func checkLineSafe(b []byte) error {
	for _, c := range b {
		switch c {
		case '\r', '\n':
			return errors.New("protocol: simple strings and errors must not contain CR or LF")
		case 0:
			return errors.New("protocol: simple strings and errors must not contain NUL")
		}
	}
	return nil
}

// Encode writes one value to w using a fresh Writer and flushes it.
func Encode(w io.Writer, v Value) error {
	enc := NewWriter(w)
	if err := enc.Write(v); err != nil {
		return err
	}
	return enc.Flush()
}

// EncodeCommand serialises a request as an array of bulk strings, which is the
// canonical encoding clients use.
func EncodeCommand(args ...[]byte) []byte {
	size := 1 + 12 + 2
	for _, a := range args {
		size += 1 + 12 + 2 + len(a) + 2
	}
	buf := make([]byte, 0, size)
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, a := range args {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(a)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, a...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

// EncodeCommandStrings is EncodeCommand for string arguments.
func EncodeCommandStrings(args ...string) []byte {
	raw := make([][]byte, len(args))
	for i, a := range args {
		raw[i] = []byte(a)
	}
	return EncodeCommand(raw...)
}
