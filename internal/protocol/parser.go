package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
)

// bulkReadChunk bounds how many bytes the parser commits to a single bulk
// string allocation before it has actually received them. A client that
// declares a huge bulk string but sends nothing therefore costs at most one
// chunk of memory instead of the declared size.
const bulkReadChunk = 32 << 10

// argCapHint bounds the number of argument slots pre-allocated for a request.
// The declared array length is attacker-controlled, so the parser only trusts
// it in chunks and grows as arguments really arrive.
const argCapHint = 1024

// Parser is a streaming RESP2 decoder.
//
// A Parser is not safe for concurrent use. Create one per connection; the
// parser owns the read buffer, which also lets the server detect leftover
// pipelined bytes via Buffered.
type Parser struct {
	br     *bufio.Reader
	limits Limits
	line   []byte
}

// NewParser wraps r in a buffered streaming parser. The reader buffer is owned
// by the parser.
func NewParser(r io.Reader, limits Limits) *Parser {
	return &Parser{
		br:     bufio.NewReaderSize(r, 16<<10),
		limits: limits.WithDefaults(),
		line:   make([]byte, 0, 256),
	}
}

// Limits returns the effective limits after defaults and clamping.
func (p *Parser) Limits() Limits { return p.limits }

// Buffered reports how many bytes are already available in the read buffer
// without touching the network. The server uses this to write one flush per
// batch of pipelined replies instead of one flush per request.
func (p *Parser) Buffered() int { return p.br.Buffered() }

// ReadCommand reads exactly one client request: an array of bulk strings.
//
// Errors:
//   - io.EOF on a clean close at a frame boundary (the peer disconnected);
//   - io.ErrUnexpectedEOF when a frame was cut off mid-flight;
//   - *ProtocolError for a malformed frame, an oversized length, or a frame
//     that violates the configured limits.
//
// A ProtocolError leaves the stream desynchronised: the caller must reply with
// the error and close the connection rather than trying to resynchronise.
func (p *Parser) ReadCommand() (Command, error) {
	prefix, err := p.br.ReadByte()
	if err != nil {
		// A clean disconnect at a frame boundary is not a protocol violation.
		return nil, err
	}
	if Type(prefix) != TypeArray {
		return nil, protocolErr("expected an array of bulk strings, found %s", describe(prefix))
	}

	n, err := p.readLength()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, protocolErr("invalid multi-bulk length %d", n)
	}
	if n > int64(p.limits.MaxArrayLen) {
		return nil, protocolErr("multi-bulk length %d exceeds the %d argument limit", n, p.limits.MaxArrayLen)
	}

	capHint := n
	if capHint > argCapHint {
		capHint = argCapHint
	}
	args := make(Command, 0, capHint)
	var declared int64
	for i := int64(0); i < n; i++ {
		prefix, err := p.br.ReadByte()
		if err != nil {
			return nil, wrapUnexpectedEOF(err)
		}
		if Type(prefix) != TypeBulkString {
			return nil, protocolErr("expected a bulk string argument at position %d, found %s", i+1, describe(prefix))
		}
		length, err := p.readLength()
		if err != nil {
			return nil, err
		}
		if length < 0 {
			return nil, protocolErr("bulk string length %d is negative", length)
		}
		if length > int64(p.limits.MaxBulkBytes) {
			return nil, protocolErr("bulk string length %d exceeds the %d byte limit", length, p.limits.MaxBulkBytes)
		}
		declared += length
		if declared > int64(p.limits.MaxRequestBytes) {
			return nil, protocolErr("request of %d bytes exceeds the %d byte limit", declared, p.limits.MaxRequestBytes)
		}
		arg, err := p.readBulkBody(length)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
}

// ReadValue reads one RESP2 frame of any supported type. It accepts nested
// arrays up to Limits.MaxDepth and is used by the CLI and by tests.
func (p *Parser) ReadValue() (Value, error) { return p.readValue(0) }

func (p *Parser) readValue(depth int) (Value, error) {
	if depth > p.limits.MaxDepth {
		return Value{}, protocolErr("nesting depth exceeds the %d level limit", p.limits.MaxDepth)
	}
	prefix, err := p.br.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch Type(prefix) {
	case TypeSimpleString, TypeError:
		line, err := p.readLine()
		if err != nil {
			return Value{}, err
		}
		// The line may alias the read buffer, so copy before returning.
		return Value{Type: Type(prefix), Str: bytes.Clone(line)}, nil

	case TypeInteger:
		line, err := p.readLine()
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return Value{}, protocolErr("invalid integer %q", truncateForError(line))
		}
		return Value{Type: TypeInteger, Int: n}, nil

	case TypeBulkString:
		length, err := p.readLength()
		if err != nil {
			return Value{}, err
		}
		if length == -1 {
			return NullBulk(), nil
		}
		if length < 0 {
			return Value{}, protocolErr("bulk string length %d is negative", length)
		}
		if length > int64(p.limits.MaxBulkBytes) {
			return Value{}, protocolErr("bulk string length %d exceeds the %d byte limit", length, p.limits.MaxBulkBytes)
		}
		body, err := p.readBulkBody(length)
		if err != nil {
			return Value{}, err
		}
		return Bulk(body), nil

	case TypeArray:
		length, err := p.readLength()
		if err != nil {
			return Value{}, err
		}
		if length == -1 {
			return NullArray(), nil
		}
		if length < 0 {
			return Value{}, protocolErr("array length %d is negative", length)
		}
		if length > int64(p.limits.MaxArrayLen) {
			return Value{}, protocolErr("array length %d exceeds the %d element limit", length, p.limits.MaxArrayLen)
		}
		capHint := length
		if capHint > argCapHint {
			capHint = argCapHint
		}
		items := make([]Value, 0, capHint)
		for i := int64(0); i < length; i++ {
			item, err := p.readValue(depth + 1)
			if err != nil {
				return Value{}, err
			}
			items = append(items, item)
		}
		return Arr(items...), nil

	default:
		return Value{}, protocolErr("unknown RESP type byte %s", describe(prefix))
	}
}

// readBulkBody reads exactly n bytes and the CRLF that terminates them.
func (p *Parser) readBulkBody(n int64) ([]byte, error) {
	var body []byte
	for remaining := n; remaining > 0; {
		step := remaining
		if step > bulkReadChunk {
			step = bulkReadChunk
		}
		start := len(body)
		body = append(body, make([]byte, step)...)
		if _, err := io.ReadFull(p.br, body[start:]); err != nil {
			return nil, wrapUnexpectedEOF(err)
		}
		remaining -= step
	}
	if err := p.expectCRLF(); err != nil {
		return nil, err
	}
	return body, nil
}

// readLength reads and parses a CRLF-terminated decimal length. The result is
// validated against the int64 range by strconv, so a hostile length such as
// 9223372036854775808 is rejected here instead of overflowing later.
func (p *Parser) readLength() (int64, error) {
	line, err := p.readLine()
	if err != nil {
		return 0, err
	}
	if len(line) == 0 {
		return 0, protocolErr("empty length line")
	}
	if len(line) == 1 && line[0] == '?' {
		// RESP3 streaming aggregates. Explicitly rejected instead of being
		// silently misread as a length.
		return 0, protocolErr("streaming lengths are not supported (RESP2 only)")
	}
	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		var numErr *strconv.NumError
		if errors.As(err, &numErr) && errors.Is(numErr.Err, strconv.ErrRange) {
			return 0, protocolErr("length %q overflows a 64-bit integer", truncateForError(line))
		}
		return 0, protocolErr("invalid length %q", truncateForError(line))
	}
	return n, nil
}

// readLine reads one CRLF-terminated line and returns it without the
// terminator.
//
// The returned slice may alias the parser's read buffer and is only valid
// until the next read, so callers must copy anything they keep. Long lines are
// reassembled in an internal buffer, but never beyond Limits.MaxLineBytes.
func (p *Parser) readLine() ([]byte, error) {
	p.line = p.line[:0]
	accumulating := false
	for {
		chunk, err := p.br.ReadSlice('\n')
		if err == nil {
			out := chunk
			if accumulating {
				p.line = append(p.line, chunk...)
				out = p.line
			}
			if len(out) > p.limits.MaxLineBytes+2 {
				return nil, protocolErr("line exceeds the %d byte limit", p.limits.MaxLineBytes)
			}
			if len(out) < 2 || out[len(out)-2] != '\r' {
				return nil, protocolErr("line is not terminated by CRLF")
			}
			return out[:len(out)-2], nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, wrapUnexpectedEOF(err)
		}
		accumulating = true
		p.line = append(p.line, chunk...)
		if len(p.line) > p.limits.MaxLineBytes {
			return nil, protocolErr("line exceeds the %d byte limit", p.limits.MaxLineBytes)
		}
	}
}

// expectCRLF consumes the two bytes that must terminate a bulk string body.
func (p *Parser) expectCRLF() error {
	var crlf [2]byte
	if _, err := io.ReadFull(p.br, crlf[:]); err != nil {
		return wrapUnexpectedEOF(err)
	}
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return protocolErr("bulk string body is not terminated by CRLF")
	}
	return nil
}

// wrapUnexpectedEOF converts a mid-frame EOF into io.ErrUnexpectedEOF so that
// callers can distinguish a clean disconnect from a truncated frame.
func wrapUnexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// truncateForError bounds how much client data is echoed into an error string.
func truncateForError(b []byte) string {
	const max = 32
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "..."
}

// ParseCommand decodes exactly one request from b. It is a convenience helper
// for tests and tooling; it does not reject trailing bytes.
func ParseCommand(b []byte, limits Limits) (Command, error) {
	return NewParser(bytes.NewReader(b), limits).ReadCommand()
}

// ParseValue decodes exactly one RESP2 frame from b.
func ParseValue(b []byte, limits Limits) (Value, error) {
	return NewParser(bytes.NewReader(b), limits).ReadValue()
}
