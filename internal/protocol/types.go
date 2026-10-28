// Package protocol implements MiniRedis's RESP2 wire protocol.
//
// The parser and encoder are written from the RESP2 specification and do not
// depend on any third-party protocol library. The implementation is designed
// for streaming network input:
//
//   - it never assumes that one TCP read equals one request;
//   - it reassembles frames that arrive in fragments;
//   - it keeps parsing after a complete frame so pipelined requests work;
//   - it treats all client-supplied lengths as untrusted and validates them
//     against explicit limits before allocating memory.
//
// Supported RESP2 types: simple strings, errors, integers, bulk strings (with
// the null bulk string), and arrays (including nested arrays when reading).
package protocol

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Type is a RESP2 type byte.
type Type byte

// The RESP2 type prefixes MiniRedis understands. The names are prefixed with
// Type to leave the short constructor names (Simple, Bulk, Arr, Int) free.
const (
	TypeSimpleString Type = '+'
	TypeError        Type = '-'
	TypeInteger      Type = ':'
	TypeBulkString   Type = '$'
	TypeArray        Type = '*'
)

// String returns a stable name used in error messages and tests.
func (t Type) String() string {
	switch t {
	case TypeSimpleString:
		return "simple-string"
	case TypeError:
		return "error"
	case TypeInteger:
		return "integer"
	case TypeBulkString:
		return "bulk-string"
	case TypeArray:
		return "array"
	default:
		return fmt.Sprintf("unknown(0x%02x)", byte(t))
	}
}

// Value is a decoded RESP2 frame.
//
// The zero Value has Type 0 and is not encodable. Use the constructors below
// (Simple, Errorf, Int, Bulk, NullBulk, Arr) instead of building literals.
//
// Ownership: Bulk values own their byte slice. The parser allocates a fresh
// slice for every bulk string, and the encoder never retains a reference, so a
// Value can be handed to another goroutine once it has been constructed.
type Value struct {
	Type   Type
	Str    []byte
	Int    int64
	Array  []Value
	IsNull bool
}

// Simple builds a RESP2 simple string.
func Simple(s string) Value { return Value{Type: TypeSimpleString, Str: []byte(s)} }

// Errorf builds a RESP2 error. The message must not contain CR or LF.
func Errorf(format string, args ...any) Value {
	return Value{Type: TypeError, Str: []byte(fmt.Sprintf(format, args...))}
}

// Int builds a RESP2 integer.
func Int(i int64) Value { return Value{Type: TypeInteger, Int: i} }

// Bulk builds a RESP2 bulk string. Bulk takes ownership of b; callers must not
// mutate b after the call. A nil b encodes as the empty bulk string, not as a
// null bulk string.
func Bulk(b []byte) Value { return Value{Type: TypeBulkString, Str: b} }

// BulkString is Bulk for a Go string.
func BulkString(s string) Value { return Value{Type: TypeBulkString, Str: []byte(s)} }

// NullBulk builds the RESP2 null bulk string ($-1).
func NullBulk() Value { return Value{Type: TypeBulkString, IsNull: true} }

// NullArray builds the RESP2 null array (*-1).
func NullArray() Value { return Value{Type: TypeArray, IsNull: true} }

// Arr builds a RESP2 array from the given elements.
func Arr(values ...Value) Value {
	if values == nil {
		values = []Value{}
	}
	return Value{Type: TypeArray, Array: values}
}

// IsError reports whether the value is a RESP2 error frame.
func (v Value) IsError() bool { return v.Type == TypeError }

// Bytes returns the payload of bulk strings and simple strings. For any other
// type it returns nil.
func (v Value) Bytes() []byte {
	switch v.Type {
	case TypeBulkString, TypeSimpleString, TypeError:
		return v.Str
	default:
		return nil
	}
}

// Render returns a single-line, human-readable rendering of the value using
// redis-cli-like conventions. It is used by the CLI and by test failure
// messages; it never panics on binary content, which it escapes.
func (v Value) Render() string {
	switch v.Type {
	case TypeSimpleString:
		return quoteIfNeeded(v.Str, false)
	case TypeError:
		return "(error) " + quoteIfNeeded(v.Str, false)
	case TypeInteger:
		return "(integer) " + strconv.FormatInt(v.Int, 10)
	case TypeBulkString:
		if v.IsNull {
			return "(nil)"
		}
		return quoteIfNeeded(v.Str, true)
	case TypeArray:
		if v.IsNull {
			return "(nil)"
		}
		if len(v.Array) == 0 {
			return "(empty array)"
		}
		if len(v.Array) == 1 {
			return "1) " + v.Array[0].Render()
		}
		var b strings.Builder
		for i, item := range v.Array {
			if i > 0 {
				b.WriteByte('\n')
			}
			fmt.Fprintf(&b, "%d) %s", i+1, item.Render())
		}
		return b.String()
	default:
		return fmt.Sprintf("(unencodable value of type %s)", v.Type)
	}
}

// quoteIfNeeded renders bytes for humans. When alwaysQuote is false the input
// is returned unescaped if it contains no control characters, so that
// "+OK" prints as "OK". Otherwise the value is wrapped in double quotes.
func quoteIfNeeded(b []byte, alwaysQuote bool) string {
	needsQuote := alwaysQuote
	if !alwaysQuote {
		for _, c := range b {
			if c < 0x20 || c == 0x7f {
				needsQuote = true
				break
			}
		}
	}
	if !needsQuote {
		return string(b)
	}
	var sb strings.Builder
	sb.WriteByte('"')
	for _, c := range b {
		switch c {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		default:
			if c < 0x20 || c == 0x7f {
				fmt.Fprintf(&sb, `\x%02x`, c)
			} else {
				sb.WriteByte(c)
			}
		}
	}
	sb.WriteByte('"')
	return sb.String()
}

// Command is a decoded client request: a non-empty list of arguments whose
// first element is the command name. Both names and arguments are binary safe.
type Command [][]byte

// Name returns the command name upper-cased. RESP2 command names are ASCII.
func (c Command) Name() string {
	if len(c) == 0 {
		return ""
	}
	return strings.ToUpper(string(c[0]))
}

// Arg returns the i-th argument after the command name, or nil when absent.
func (c Command) Arg(i int) []byte {
	if i+1 >= len(c) {
		return nil
	}
	return c[i+1]
}

// Args returns all arguments except the command name. The returned slice
// aliases the command and must not be mutated.
func (c Command) Args() [][]byte {
	if len(c) < 2 {
		return nil
	}
	return c[1:]
}

// Limits bounds everything the parser will accept from an untrusted peer.
//
// Every field is a hard ceiling. Zero values are replaced with the defaults
// from DefaultLimits by normalize, and values above the absolute ceilings in
// this package are clamped down again, so a misconfigured server cannot be
// talked into an unbounded allocation.
type Limits struct {
	// MaxBulkBytes is the largest single bulk string body accepted.
	MaxBulkBytes int
	// MaxArrayLen is the largest number of elements in a request array.
	MaxArrayLen int
	// MaxRequestBytes bounds the summed bulk-string sizes of one request.
	MaxRequestBytes int
	// MaxLineBytes bounds a single protocol line (lengths, simple strings).
	MaxLineBytes int
	// MaxDepth bounds nesting when reading general values. Requests are
	// always one level deep; this only matters for ReadValue.
	MaxDepth int
}

// Absolute ceilings. These are defensive upper bounds that no configuration
// may exceed, so that untrusted lengths are always far below the int64 range.
const (
	HardMaxBulkBytes    = 512 << 20 // 512 MiB
	HardMaxArrayLen     = 1 << 22   // ~4.2M arguments
	HardMaxRequestBytes = 1 << 30   // 1 GiB
	HardMaxLineBytes    = 1 << 20   // 1 MiB
	HardMaxDepth        = 64
)

// DefaultLimits returns the limits used when a caller supplies none.
func DefaultLimits() Limits {
	return Limits{
		MaxBulkBytes:    64 << 20,
		MaxArrayLen:     1 << 20,
		MaxRequestBytes: 64 << 20,
		MaxLineBytes:    64 << 10,
		MaxDepth:        8,
	}
}

// WithDefaults fills zero fields with defaults and clamps every field to the
// absolute ceilings in this package.
func (l Limits) WithDefaults() Limits {
	d := DefaultLimits()
	if l.MaxBulkBytes <= 0 {
		l.MaxBulkBytes = d.MaxBulkBytes
	}
	if l.MaxArrayLen <= 0 {
		l.MaxArrayLen = d.MaxArrayLen
	}
	if l.MaxRequestBytes <= 0 {
		l.MaxRequestBytes = d.MaxRequestBytes
	}
	if l.MaxLineBytes <= 0 {
		l.MaxLineBytes = d.MaxLineBytes
	}
	if l.MaxDepth <= 0 {
		l.MaxDepth = d.MaxDepth
	}
	if l.MaxBulkBytes > HardMaxBulkBytes {
		l.MaxBulkBytes = HardMaxBulkBytes
	}
	if l.MaxArrayLen > HardMaxArrayLen {
		l.MaxArrayLen = HardMaxArrayLen
	}
	if l.MaxRequestBytes > HardMaxRequestBytes {
		l.MaxRequestBytes = HardMaxRequestBytes
	}
	if l.MaxLineBytes > HardMaxLineBytes {
		l.MaxLineBytes = HardMaxLineBytes
	}
	if l.MaxDepth > HardMaxDepth {
		l.MaxDepth = HardMaxDepth
	}
	return l
}

// ProtocolError reports a frame that violates RESP2 or one of the configured
// limits. The server replies with the message and closes the connection, which
// is the only safe recovery for a desynchronised stream.
type ProtocolError struct {
	// Reason explains the violation without echoing unbounded client data.
	Reason string
	cause  error
}

// Error implements error. The rendered form is suitable for sending to a
// client as a RESP2 error frame.
func (e *ProtocolError) Error() string {
	if e.cause != nil {
		return "Protocol error: " + e.Reason + ": " + e.cause.Error()
	}
	return "Protocol error: " + e.Reason
}

// Unwrap exposes the underlying transport error, if any.
func (e *ProtocolError) Unwrap() error { return e.cause }

func protocolErr(format string, args ...any) *ProtocolError {
	return &ProtocolError{Reason: fmt.Sprintf(format, args...)}
}

// IsProtocolError reports whether err is (or wraps) a ProtocolError.
func IsProtocolError(err error) bool {
	var pe *ProtocolError
	return errors.As(err, &pe)
}

// describe renders a type byte for an error message without echoing binary
// payload bytes.
func describe(b byte) string {
	if b >= 0x20 && b < 0x7f {
		return fmt.Sprintf("%q", string(rune(b)))
	}
	return fmt.Sprintf("byte 0x%02x", b)
}
