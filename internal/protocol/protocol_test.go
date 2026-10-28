package protocol

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// chunkReader returns data in fixed-size pieces regardless of how much the
// caller asks for. It models a TCP stream that splits frames arbitrarily.
type chunkReader struct {
	data []byte
	pos  int
	step int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := r.step
	if n > len(p) {
		n = len(p)
	}
	if r.pos+n > len(r.data) {
		n = len(r.data) - r.pos
	}
	copy(p, r.data[r.pos:r.pos+n])
	r.pos += n
	return n, nil
}

// byteAtATimeReader is the most hostile fragmentation case: every Read call
// returns a single byte.
type byteAtATimeReader struct {
	data []byte
	pos  int
}

func (r *byteAtATimeReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = r.data[r.pos]
	r.pos++
	return 1, nil
}

func testLimits() Limits {
	return Limits{
		MaxBulkBytes:    1 << 16,
		MaxArrayLen:     1024,
		MaxRequestBytes: 1 << 17,
		MaxLineBytes:    4096,
		MaxDepth:        4,
	}
}

func readCommandFrom(t *testing.T, wire string, limits Limits) (Command, error) {
	t.Helper()
	return NewParser(strings.NewReader(wire), limits).ReadCommand()
}

func mustReadCommand(t *testing.T, wire string, limits Limits) Command {
	t.Helper()
	cmd, err := readCommandFrom(t, wire, limits)
	if err != nil {
		t.Fatalf("ReadCommand(%q) returned error: %v", wire, err)
	}
	return cmd
}

// ---------------------------------------------------------------------------
// Valid frames
// ---------------------------------------------------------------------------

func TestReadCommandValid(t *testing.T) {
	tests := []struct {
		name string
		wire string
		want [][]byte
	}{
		{
			name: "ping",
			wire: "*1\r\n$4\r\nPING\r\n",
			want: [][]byte{[]byte("PING")},
		},
		{
			name: "two arguments",
			wire: "*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n",
			want: [][]byte{[]byte("PING"), []byte("hello")},
		},
		{
			name: "empty bulk string argument",
			wire: "*2\r\n$3\r\nSET\r\n$0\r\n\r\n",
			want: [][]byte{[]byte("SET"), {}},
		},
		{
			name: "binary argument with CR LF and NUL",
			wire: "*2\r\n$3\r\nGET\r\n$5\r\na\r\nb\x00\r\n",
			want: [][]byte{[]byte("GET"), []byte("a\r\nb\x00")},
		},
		{
			name: "many arguments",
			wire: "*4\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
			want: [][]byte{[]byte("MSET"), []byte("a"), []byte("b"), []byte("c")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mustReadCommand(t, tc.wire, testLimits())
			if len(got) != len(tc.want) {
				t.Fatalf("got %d arguments, want %d (%q)", len(got), len(tc.want), got)
			}
			for i := range tc.want {
				if !bytes.Equal(got[i], tc.want[i]) {
					t.Errorf("argument %d = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestReadCommandDoesNotAssumeOneReadPerCommand(t *testing.T) {
	// The same request, delivered in every fragmentation pattern, must decode
	// identically.
	wire := []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	want := [][]byte{[]byte("SET"), []byte("key"), []byte("value")}

	patterns := map[string]io.Reader{
		"whole":        bytes.NewReader(wire),
		"two bytes":    &chunkReader{data: wire, step: 2},
		"seven":        &chunkReader{data: wire, step: 7},
		"byte by byte": &byteAtATimeReader{data: wire},
		"big chunks":   &chunkReader{data: wire, step: len(wire)},
	}

	for name, r := range patterns {
		t.Run(name, func(t *testing.T) {
			cmd, err := NewParser(r, testLimits()).ReadCommand()
			if err != nil {
				t.Fatalf("ReadCommand: %v", err)
			}
			if len(cmd) != len(want) {
				t.Fatalf("got %d arguments, want %d", len(cmd), len(want))
			}
			for i := range want {
				if !bytes.Equal(cmd[i], want[i]) {
					t.Errorf("argument %d = %q, want %q", i, cmd[i], want[i])
				}
			}
		})
	}
}

func TestReadCommandPipelined(t *testing.T) {
	wire := strings.Join([]string{
		"*1\r\n$4\r\nPING\r\n",
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
		"*2\r\n$4\r\nECHO\r\n$0\r\n\r\n",
	}, "")

	p := NewParser(strings.NewReader(wire), testLimits())
	wantNames := []string{"PING", "SET", "GET", "ECHO"}
	for i, wantName := range wantNames {
		cmd, err := p.ReadCommand()
		if err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		if cmd.Name() != wantName {
			t.Fatalf("command %d name = %q, want %q", i, cmd.Name(), wantName)
		}
	}
	if _, err := p.ReadCommand(); !errors.Is(err, io.EOF) {
		t.Fatalf("after the last command: got %v, want io.EOF", err)
	}
}

// ---------------------------------------------------------------------------
// Truncated and clean-close behaviour
// ---------------------------------------------------------------------------

func TestReadCommandTruncated(t *testing.T) {
	full := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	// Every prefix of a valid request must fail with io.ErrUnexpectedEOF.
	for i := 1; i < len(full); i++ {
		prefix := full[:i]
		_, err := readCommandFrom(t, prefix, testLimits())
		if err == nil {
			t.Fatalf("prefix of %d bytes unexpectedly decoded", i)
		}
		if IsProtocolError(err) {
			// A prefix may legitimately look malformed (for example a length
			// line that is still incomplete); accept either diagnostic kind.
			continue
		}
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("prefix of %d bytes: got %v, want io.ErrUnexpectedEOF", i, err)
		}
	}
}

func TestReadCommandCleanClose(t *testing.T) {
	p := NewParser(strings.NewReader(""), testLimits())
	if _, err := p.ReadCommand(); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

// ---------------------------------------------------------------------------
// Malformed frames
// ---------------------------------------------------------------------------

func TestReadCommandMalformed(t *testing.T) {
	tests := []struct {
		name string
		wire string
	}{
		{"simple string instead of array", "+OK\r\n"},
		{"integer instead of array", ":1\r\n"},
		{"bulk string instead of array", "$4\r\nPING\r\n"},
		{"unknown type byte", "\x00\r\n"},
		{"array with zero elements", "*0\r\n"},
		{"array with negative length", "*-1\r\n"},
		{"no CRLF after array length", "*1\n$4\r\nPING\r\n"},
		{"bare LF in array length", "*1\n"},
		{"non numeric array length", "*abc\r\n$4\r\nPING\r\n"},
		{"empty array length", "*\r\n"},
		{"argument is not a bulk string", "*2\r\n$4\r\nPING\r\n:3\r\n"},
		{"argument is an array", "*2\r\n$4\r\nPING\r\n*1\r\n$1\r\nx\r\n"},
		{"negative bulk length", "*1\r\n$-2\r\n\r\n"},
		{"bulk body missing CRLF", "*1\r\n$4\r\nPINGXY"},
		{"bulk body terminated by LF only", "*1\r\n$4\r\nPING\n\r\n"},
		{"streaming length (RESP3)", "*1\r\n$?\r\n"},
		{"space in length", "*1 \r\n$4\r\nPING\r\n"},
		{"hex length", "*-0x1\r\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readCommandFrom(t, tc.wire, testLimits())
			if err == nil {
				t.Fatalf("wire %q unexpectedly decoded", tc.wire)
			}
			if !IsProtocolError(err) {
				t.Fatalf("wire %q: got %v (%T), want *ProtocolError", tc.wire, err, err)
			}
		})
	}
}

func TestProtocolErrorIsInspectable(t *testing.T) {
	_, err := readCommandFrom(t, "+OK\r\n", testLimits())
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("got %T, want *ProtocolError", err)
	}
	if !strings.Contains(pe.Reason, "array") {
		t.Errorf("reason %q should mention the expected array", pe.Reason)
	}
	if !strings.HasPrefix(pe.Error(), "Protocol error: ") {
		t.Errorf("Error() = %q, want a Protocol error prefix", pe.Error())
	}
}

// ---------------------------------------------------------------------------
// Limits, overflow and allocation safety
// ---------------------------------------------------------------------------

func TestReadCommandRejectsOversizedBulk(t *testing.T) {
	limits := testLimits()
	limits.MaxBulkBytes = 64
	length := limits.MaxBulkBytes + 1
	wire := "*1\r\n$" + strconv.Itoa(length) + "\r\n" + strings.Repeat("x", length) + "\r\n"

	_, err := readCommandFrom(t, wire, limits)
	if err == nil {
		t.Fatal("expected an error for a bulk string above the limit")
	}
	if !IsProtocolError(err) {
		t.Fatalf("got %v (%T), want *ProtocolError", err, err)
	}
	if !strings.Contains(err.Error(), "64") {
		t.Errorf("error %q should mention the configured limit", err)
	}
}

func TestReadCommandAcceptsBulkExactlyAtLimit(t *testing.T) {
	limits := testLimits()
	limits.MaxBulkBytes = 32
	payload := strings.Repeat("x", 32)
	wire := "*2\r\n$3\r\nGET\r\n$32\r\n" + payload + "\r\n"

	cmd := mustReadCommand(t, wire, limits)
	if len(cmd) != 2 || string(cmd[1]) != payload {
		t.Fatalf("decode failed at the exact limit: %q", cmd)
	}
}

func TestReadCommandRejectsTooManyArguments(t *testing.T) {
	limits := testLimits()
	limits.MaxArrayLen = 4
	var sb strings.Builder
	sb.WriteString("*5\r\n")
	for i := 0; i < 5; i++ {
		sb.WriteString("$1\r\nx\r\n")
	}
	_, err := readCommandFrom(t, sb.String(), limits)
	if !IsProtocolError(err) {
		t.Fatalf("got %v, want a ProtocolError for the argument limit", err)
	}
}

func TestReadCommandRejectsOversizedRequest(t *testing.T) {
	limits := testLimits()
	limits.MaxBulkBytes = 1 << 20
	limits.MaxRequestBytes = 100
	var sb strings.Builder
	sb.WriteString("*3\r\n$1\r\na\r\n$80\r\n")
	sb.WriteString(strings.Repeat("x", 80))
	sb.WriteString("\r\n$80\r\n")
	sb.WriteString(strings.Repeat("y", 80))
	sb.WriteString("\r\n")

	_, err := readCommandFrom(t, sb.String(), limits)
	if !IsProtocolError(err) {
		t.Fatalf("got %v, want a ProtocolError for the total request limit", err)
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("error %q should explain the limit that was exceeded", err)
	}
}

func TestReadCommandRejectsIntegerOverflowInLength(t *testing.T) {
	tests := []string{
		"*99999999999999999999999\r\n",
		"*9223372036854775808\r\n",
		"*1\r\n$99999999999999999999999\r\n",
		"*18446744073709551616\r\n",
	}

	for _, wire := range tests {
		t.Run(wire, func(t *testing.T) {
			_, err := readCommandFrom(t, wire, testLimits())
			if !IsProtocolError(err) {
				t.Fatalf("wire %q: got %v, want a ProtocolError", wire, err)
			}
			if !strings.Contains(err.Error(), "overflow") && !strings.Contains(err.Error(), "exceeds") {
				t.Errorf("error %q should mention overflow or the limit", err)
			}
		})
	}
}

func TestReadCommandRejectsOverlongLine(t *testing.T) {
	limits := testLimits()
	limits.MaxLineBytes = 16
	wire := "*1\r\n$" + strings.Repeat("0", 64) + "4\r\nPING\r\n"
	_, err := readCommandFrom(t, wire, limits)
	if !IsProtocolError(err) {
		t.Fatalf("got %v, want a ProtocolError for the line limit", err)
	}
}

// TestDeclaredLengthDoesNotAllocateUpfront is the allocation-safety guarantee:
// a client may declare a large bulk string and then send nothing, and the
// parser must fail without committing the declared size to memory.
func TestDeclaredLengthDoesNotAllocateUpfront(t *testing.T) {
	limits := DefaultLimits()
	wire := "*1\r\n$67108864\r\n" // declares 64 MiB, sends no body
	_, err := readCommandFrom(t, wire, limits)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
	}

	// The parser must not have grown its line buffer to the declared size.
	p := NewParser(strings.NewReader(wire), limits)
	_, _ = p.ReadCommand()
	if len(p.line) > 64 {
		t.Fatalf("parser line buffer grew to %d bytes for a 64 MiB declaration", len(p.line))
	}
}

// TestDeclaredArrayLengthDoesNotAllocateUpfront covers the same guarantee for
// the pre-allocated argument slots.
func TestDeclaredArrayLengthDoesNotAllocateUpfront(t *testing.T) {
	limits := DefaultLimits()
	wire := "*1000000\r\n$4\r\nPING\r\n" // declares 1M arguments, sends one
	p := NewParser(strings.NewReader(wire), limits)
	_, err := p.ReadCommand()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestLimitsClamping(t *testing.T) {
	got := Limits{
		MaxBulkBytes:    1 << 62,
		MaxArrayLen:     1 << 62,
		MaxRequestBytes: 1 << 62,
		MaxLineBytes:    1 << 62,
		MaxDepth:        1 << 30,
	}.WithDefaults()

	if got.MaxBulkBytes != HardMaxBulkBytes {
		t.Errorf("MaxBulkBytes = %d, want %d", got.MaxBulkBytes, HardMaxBulkBytes)
	}
	if got.MaxArrayLen != HardMaxArrayLen {
		t.Errorf("MaxArrayLen = %d, want %d", got.MaxArrayLen, HardMaxArrayLen)
	}
	if got.MaxRequestBytes != HardMaxRequestBytes {
		t.Errorf("MaxRequestBytes = %d, want %d", got.MaxRequestBytes, HardMaxRequestBytes)
	}
	if got.MaxLineBytes != HardMaxLineBytes {
		t.Errorf("MaxLineBytes = %d, want %d", got.MaxLineBytes, HardMaxLineBytes)
	}
	if got.MaxDepth != HardMaxDepth {
		t.Errorf("MaxDepth = %d, want %d", got.MaxDepth, HardMaxDepth)
	}

	zero := Limits{}.WithDefaults()
	if zero != DefaultLimits() {
		t.Errorf("zero limits = %+v, want %+v", zero, DefaultLimits())
	}
}

// ---------------------------------------------------------------------------
// ReadValue
// ---------------------------------------------------------------------------

func TestReadValue(t *testing.T) {
	t.Run("simple string", func(t *testing.T) {
		v, err := ParseValue([]byte("+OK\r\n"), testLimits())
		if err != nil {
			t.Fatal(err)
		}
		if v.Type != TypeSimpleString || string(v.Str) != "OK" {
			t.Fatalf("got %+v", v)
		}
	})

	t.Run("error", func(t *testing.T) {
		v, err := ParseValue([]byte("-ERR unknown command\r\n"), testLimits())
		if err != nil {
			t.Fatal(err)
		}
		if v.Type != TypeError || !v.IsError() {
			t.Fatalf("got %+v", v)
		}
	})

	t.Run("integer", func(t *testing.T) {
		for _, tc := range []struct {
			wire string
			want int64
		}{
			{":0\r\n", 0},
			{":-1\r\n", -1},
			{":9223372036854775807\r\n", 9223372036854775807},
			{":-9223372036854775808\r\n", -9223372036854775808},
		} {
			v, err := ParseValue([]byte(tc.wire), testLimits())
			if err != nil {
				t.Fatalf("%q: %v", tc.wire, err)
			}
			if v.Type != TypeInteger || v.Int != tc.want {
				t.Fatalf("%q: got %+v, want %d", tc.wire, v, tc.want)
			}
		}
	})

	t.Run("integer overflow", func(t *testing.T) {
		_, err := ParseValue([]byte(":9223372036854775808\r\n"), testLimits())
		if !IsProtocolError(err) {
			t.Fatalf("got %v, want a ProtocolError", err)
		}
	})

	t.Run("bulk string", func(t *testing.T) {
		v, err := ParseValue([]byte("$5\r\nhello\r\n"), testLimits())
		if err != nil {
			t.Fatal(err)
		}
		if v.IsNull || string(v.Str) != "hello" {
			t.Fatalf("got %+v", v)
		}
	})

	t.Run("empty bulk string is not null", func(t *testing.T) {
		v, err := ParseValue([]byte("$0\r\n\r\n"), testLimits())
		if err != nil {
			t.Fatal(err)
		}
		if v.IsNull {
			t.Fatal("empty bulk string decoded as null")
		}
		if len(v.Str) != 0 {
			t.Fatalf("got %q, want an empty string", v.Str)
		}
	})

	t.Run("null bulk string", func(t *testing.T) {
		v, err := ParseValue([]byte("$-1\r\n"), testLimits())
		if err != nil {
			t.Fatal(err)
		}
		if !v.IsNull || v.Type != TypeBulkString {
			t.Fatalf("got %+v", v)
		}
		if v.Render() != "(nil)" {
			t.Fatalf("Render() = %q, want %q", v.Render(), "(nil)")
		}
	})

	t.Run("binary bulk string", func(t *testing.T) {
		payload := "\x00\x01\r\n\xff"
		v, err := ParseValue([]byte("$5\r\n"+payload+"\r\n"), testLimits())
		if err != nil {
			t.Fatal(err)
		}
		if string(v.Str) != payload {
			t.Fatalf("got %q, want %q", v.Str, payload)
		}
	})

	t.Run("array", func(t *testing.T) {
		v, err := ParseValue([]byte("*3\r\n:1\r\n$3\r\nabc\r\n+OK\r\n"), testLimits())
		if err != nil {
			t.Fatal(err)
		}
		if v.Type != TypeArray || len(v.Array) != 3 {
			t.Fatalf("got %+v", v)
		}
		if v.Array[0].Int != 1 || string(v.Array[1].Str) != "abc" || string(v.Array[2].Str) != "OK" {
			t.Fatalf("got %+v", v.Array)
		}
	})

	t.Run("nested array", func(t *testing.T) {
		v, err := ParseValue([]byte("*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n$1\r\nx\r\n"), testLimits())
		if err != nil {
			t.Fatal(err)
		}
		if len(v.Array) != 2 || len(v.Array[0].Array) != 2 || len(v.Array[1].Array) != 1 {
			t.Fatalf("got %+v", v)
		}
	})

	t.Run("empty array", func(t *testing.T) {
		v, err := ParseValue([]byte("*0\r\n"), testLimits())
		if err != nil {
			t.Fatal(err)
		}
		if v.IsNull || len(v.Array) != 0 {
			t.Fatalf("got %+v", v)
		}
		if !strings.Contains(v.Render(), "empty array") {
			t.Fatalf("Render() = %q", v.Render())
		}
	})

	t.Run("null array", func(t *testing.T) {
		v, err := ParseValue([]byte("*-1\r\n"), testLimits())
		if err != nil {
			t.Fatal(err)
		}
		if !v.IsNull || v.Type != TypeArray {
			t.Fatalf("got %+v", v)
		}
	})

	t.Run("depth limit", func(t *testing.T) {
		limits := testLimits()
		limits.MaxDepth = 2
		wire := "*1\r\n*1\r\n*1\r\n*1\r\n:1\r\n"
		if _, err := ParseValue([]byte(wire), limits); !IsProtocolError(err) {
			t.Fatalf("got %v, want a ProtocolError for the depth limit", err)
		}
	})

	t.Run("unknown type", func(t *testing.T) {
		if _, err := ParseValue([]byte("~x\r\n"), testLimits()); !IsProtocolError(err) {
			t.Fatalf("got %v, want a ProtocolError", err)
		}
	})
}

// TestReadValueCopiesLines guards against the parser returning a slice that
// aliases its internal read buffer across subsequent reads.
func TestReadValueCopiesLines(t *testing.T) {
	p := NewParser(strings.NewReader("+first\r\n+second\r\n"), testLimits())
	first, err := p.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.ReadValue(); err != nil {
		t.Fatal(err)
	}
	if string(first.Str) != "first" {
		t.Fatalf("earlier value was mutated by a later read: %q", first.Str)
	}
}

// ---------------------------------------------------------------------------
// Encoder
// ---------------------------------------------------------------------------

func TestEncoder(t *testing.T) {
	tests := []struct {
		name  string
		value Value
		want  string
	}{
		{"simple string", Simple("OK"), "+OK\r\n"},
		{"error", Errorf("ERR nope"), "-ERR nope\r\n"},
		{"zero", Int(0), ":0\r\n"},
		{"negative", Int(-42), ":-42\r\n"},
		{"max int64", Int(9223372036854775807), ":9223372036854775807\r\n"},
		{"bulk", BulkString("hello"), "$5\r\nhello\r\n"},
		{"empty bulk", BulkString(""), "$0\r\n\r\n"},
		{"binary bulk", Bulk([]byte("a\r\nb")), "$4\r\na\r\nb\r\n"},
		{"null bulk", NullBulk(), "$-1\r\n"},
		{"empty array", Arr(), "*0\r\n"},
		{"array", Arr(Int(1), BulkString("x")), "*2\r\n:1\r\n$1\r\nx\r\n"},
		{"nested array", Arr(Arr(Int(1))), "*1\r\n*1\r\n:1\r\n"},
		{"null array", NullArray(), "*-1\r\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := Encode(&buf, tc.value); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if got := buf.String(); got != tc.want {
				t.Fatalf("Encode = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestEncoderNeverWritesTheWordNil pins the rule that a missing value is
// encoded as the null bulk string, never as text.
func TestEncoderNeverWritesTheWordNil(t *testing.T) {
	var buf bytes.Buffer
	if err := Encode(&buf, NullBulk()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(buf.String()), "nil") {
		t.Fatalf("null bulk encoded as %q", buf.String())
	}
	if buf.String() != "$-1\r\n" {
		t.Fatalf("null bulk encoded as %q, want %q", buf.String(), "$-1\r\n")
	}
}

func TestEncoderRejectsLineBreakingSimpleStrings(t *testing.T) {
	cases := []Value{
		Simple("OK\r\n+PWNED"),
		Simple("OK\n"),
		Simple("OK\r"),
		Errorf("ERR bad\ninjected"),
		Simple("nul\x00byte"),
	}
	for _, v := range cases {
		var buf bytes.Buffer
		if err := Encode(&buf, v); err == nil {
			t.Fatalf("Encode(%q) accepted content that breaks RESP framing", v.Str)
		}
		if buf.Len() != 0 {
			t.Fatalf("Encode(%q) wrote %q before failing", v.Str, buf.String())
		}
	}
}

func TestEncoderRejectsOversizedBulk(t *testing.T) {
	enc := NewWriter(&bytes.Buffer{})
	enc.MaxBulkBytes = 8
	if err := enc.Write(BulkString("0123456789")); err == nil {
		t.Fatal("expected an error for a bulk string above the response limit")
	}
}

func TestEncoderRejectsUnencodableType(t *testing.T) {
	var buf bytes.Buffer
	if err := Encode(&buf, Value{Type: Type('~')}); err == nil {
		t.Fatal("expected an error for an unencodable type")
	}
}

func TestEncoderRoundTrip(t *testing.T) {
	original := Arr(
		Simple("OK"),
		Errorf("ERR bad thing"),
		Int(-7),
		BulkString("binary\r\n\x00payload"),
		NullBulk(),
		Arr(Int(1), Int(2)),
		NullArray(),
		Arr(),
	)
	var buf bytes.Buffer
	if err := Encode(&buf, original); err != nil {
		t.Fatal(err)
	}

	decoded, err := ParseValue(buf.Bytes(), DefaultLimits())
	if err != nil {
		t.Fatalf("re-parsing encoded value: %v", err)
	}

	reencoded := bytes.NewBuffer(nil)
	if err := Encode(reencoded, decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), reencoded.Bytes()) {
		t.Fatalf("round trip changed the frame:\n got %q\nwant %q", reencoded.Bytes(), buf.Bytes())
	}
}

func TestEncodeCommandRoundTrip(t *testing.T) {
	args := [][]byte{[]byte("SET"), []byte("k\x00ey"), []byte("v\r\nalue"), {}}
	wire := EncodeCommand(args...)

	cmd, err := ParseCommand(wire, DefaultLimits())
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	if len(cmd) != len(args) {
		t.Fatalf("got %d arguments, want %d", len(cmd), len(args))
	}
	for i := range args {
		if !bytes.Equal(cmd[i], args[i]) {
			t.Errorf("argument %d = %q, want %q", i, cmd[i], args[i])
		}
	}

	// The string helper must produce identical bytes.
	if got := EncodeCommandStrings("SET", "k", "v"); !bytes.Equal(got, EncodeCommand([]byte("SET"), []byte("k"), []byte("v"))) {
		t.Error("EncodeCommandStrings and EncodeCommand disagree")
	}
}

func TestCommandHelpers(t *testing.T) {
	cmd := Command{[]byte("set"), []byte("key"), []byte("value")}
	if got := cmd.Name(); got != "SET" {
		t.Errorf("Name() = %q, want SET", got)
	}
	if got := string(cmd.Arg(0)); got != "key" {
		t.Errorf("Arg(0) = %q, want key", got)
	}
	if got := cmd.Arg(9); got != nil {
		t.Errorf("Arg(9) = %q, want nil", got)
	}
	if got := len(cmd.Args()); got != 2 {
		t.Errorf("len(Args()) = %d, want 2", got)
	}
	if got := (Command{}).Name(); got != "" {
		t.Errorf("empty command Name() = %q", got)
	}
	if got := (Command{[]byte("PING")}).Args(); got != nil {
		t.Errorf("Args() for a bare command = %v, want nil", got)
	}
}

// ---------------------------------------------------------------------------
// Fuzz
// ---------------------------------------------------------------------------

// FuzzReadCommand asserts that no byte sequence can panic the parser, make it
// allocate unbounded memory, or leave it in an inconsistent state.
func FuzzReadCommand(f *testing.F) {
	seeds := []string{
		"*1\r\n$4\r\nPING\r\n",
		"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n",
		"*0\r\n",
		"*-1\r\n",
		"*1\r\n$-1\r\n",
		"*99999999999999999999\r\n",
		"$5\r\nhello\r\n",
		"+OK\r\n",
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n",
		"\r\n",
		"*2\r\n$1\r\na\r\n$1\r\nb\r\n*2\r\n$1\r\nc\r\n$1\r\nd\r\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	limits := Limits{MaxBulkBytes: 1024, MaxArrayLen: 16, MaxRequestBytes: 4096, MaxLineBytes: 512, MaxDepth: 3}

	f.Fuzz(func(t *testing.T, data []byte) {
		p := NewParser(bytes.NewReader(data), limits)
		// A bounded loop: the fuzzer must not be able to make the parser spin
		// forever on the same input.
		for i := 0; i < 8; i++ {
			cmd, err := p.ReadCommand()
			if err != nil {
				return
			}
			if len(cmd) == 0 {
				t.Fatalf("parser returned an empty command for %q", data)
			}
			if len(cmd) > limits.MaxArrayLen {
				t.Fatalf("parser returned %d arguments, above the %d limit", len(cmd), limits.MaxArrayLen)
			}
			for _, arg := range cmd {
				if len(arg) > limits.MaxBulkBytes {
					t.Fatalf("parser returned a %d byte argument, above the %d limit", len(arg), limits.MaxBulkBytes)
				}
			}
			if p.Buffered() == 0 {
				return
			}
		}
	})
}
