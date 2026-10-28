package command_test

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/amandev11/miniredis/internal/clock"
	"github.com/amandev11/miniredis/internal/command"
	"github.com/amandev11/miniredis/internal/observability"
	"github.com/amandev11/miniredis/internal/protocol"
	"github.com/amandev11/miniredis/internal/store"
)

const startMS = int64(1_700_000_000_000)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// fakeRuntime implements command.RuntimeInfo without a server, so the command
// layer can be tested on its own.
type fakeRuntime struct {
	info      command.Info
	password  string
	shutdowns int
}

func (f *fakeRuntime) ServerInfo() command.Info { return f.info }

func (f *fakeRuntime) RequestShutdown() { f.shutdowns++ }

func (f *fakeRuntime) CheckPassword(supplied []byte) bool {
	if f.password == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(f.password), supplied) == 1
}

type harness struct {
	t       *testing.T
	store   *store.Store
	reg     *command.Registry
	clk     *clock.Manual
	metrics *observability.Metrics
	runtime *fakeRuntime
	authed  bool
	require bool
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	clk := clock.NewManual(startMS)
	return &harness{
		t:       t,
		store:   store.New(clk, store.Options{}),
		reg:     command.NewDefaultRegistry(),
		clk:     clk,
		metrics: observability.New(),
		runtime: &fakeRuntime{},
	}
}

// do dispatches a command, failing the test on an unexpected error.
func (h *harness) do(args ...string) protocol.Value {
	h.t.Helper()
	v, err := h.try(args...)
	if err != nil {
		h.t.Fatalf("%v returned an error: %v", args, err)
	}
	return v
}

// try dispatches a command and returns any command error.
func (h *harness) try(args ...string) (protocol.Value, error) {
	h.t.Helper()
	raw := make([][]byte, len(args))
	for i, a := range args {
		raw[i] = []byte(a)
	}
	ctx := &command.Context{
		Args:         raw,
		Store:        h.store,
		Registry:     h.reg,
		Metrics:      h.metrics,
		Runtime:      h.runtime,
		Authed:       h.authed || !h.require,
		AuthRequired: h.require,
	}
	return h.reg.Dispatch(ctx)
}

// fail dispatches a command and returns its error, requiring one to occur.
func (h *harness) fail(args ...string) *command.Error {
	h.t.Helper()
	_, err := h.try(args...)
	if err == nil {
		h.t.Fatalf("%v unexpectedly succeeded", args)
	}
	var cmdErr *command.Error
	if !errors.As(err, &cmdErr) {
		h.t.Fatalf("%v returned %T (%v), want *command.Error", args, err, err)
	}
	return cmdErr
}

func (h *harness) wantSimple(v protocol.Value, want string) {
	h.t.Helper()
	if v.Type != protocol.TypeSimpleString || string(v.Str) != want {
		h.t.Fatalf("got %s, want the simple string %q", v.Render(), want)
	}
}

func (h *harness) wantBulk(v protocol.Value, want string) {
	h.t.Helper()
	if v.Type != protocol.TypeBulkString || v.IsNull || string(v.Str) != want {
		h.t.Fatalf("got %s, want the bulk string %q", v.Render(), want)
	}
}

func (h *harness) wantNil(v protocol.Value) {
	h.t.Helper()
	if v.Type != protocol.TypeBulkString || !v.IsNull {
		h.t.Fatalf("got %s, want a null bulk string", v.Render())
	}
}

func (h *harness) wantInt(v protocol.Value, want int64) {
	h.t.Helper()
	if v.Type != protocol.TypeInteger || v.Int != want {
		h.t.Fatalf("got %s, want the integer %d", v.Render(), want)
	}
}

// ---------------------------------------------------------------------------
// Registry completeness
// ---------------------------------------------------------------------------

// wantCommands is the documented command set. A command that exists must be
// listed here, and every entry here must be registered: this is what keeps the
// implementation and the documentation from drifting apart.
var wantCommands = []string{
	"APPEND", "AUTH", "COMMAND", "DBSIZE", "DECR", "DECRBY", "DEL", "ECHO",
	"EXISTS", "EXPIRE", "EXPIREAT", "FLUSHDB", "GET", "HGET", "HGETALL",
	"HDEL", "HEXISTS", "HLEN", "HMGET", "HSET", "INCR", "INCRBY", "INFO",
	"KEYS", "MGET", "MSET", "PERSIST", "PEXPIRE", "PEXPIREAT", "PING",
	"PTTL", "QUIT", "SCAN", "SELECT", "SET", "SHUTDOWN", "STRLEN", "TTL",
	"TYPE",
}

func TestRegistryContainsEveryDocumentedCommand(t *testing.T) {
	reg := command.NewDefaultRegistry()

	got := reg.Names()
	want := append([]string(nil), wantCommands...)
	sort.Strings(want)

	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("registered commands differ from the documented set:\n got %v\nwant %v", got, want)
	}
	if reg.Len() != len(wantCommands) {
		t.Fatalf("Len() = %d, want %d", reg.Len(), len(wantCommands))
	}
}

func TestEveryCommandIsFullySpecified(t *testing.T) {
	reg := command.NewDefaultRegistry()

	for _, spec := range reg.Specs() {
		t.Run(spec.Name, func(t *testing.T) {
			if spec.Name != strings.ToUpper(spec.Name) {
				t.Errorf("name %q is not upper case", spec.Name)
			}
			if spec.Handler == nil {
				t.Error("no handler")
			}
			if spec.Summary == "" {
				t.Error("no summary for the CLI and documentation")
			}
			if spec.Arity == 0 {
				t.Error("arity 0 accepts anything, which is almost certainly a mistake")
			}
			write := spec.Flags.Has(command.FlagWrite)
			read := spec.Flags.Has(command.FlagReadOnly)
			if write && read {
				t.Errorf("flags %q cannot be both write and readonly", spec.Flags.String())
			}
			// Connection and administrative commands (PING, AUTH, QUIT,
			// SHUTDOWN, INFO, COMMAND, SELECT) touch no keyspace, so they carry
			// neither flag. Everything that does must declare exactly one.
			touchesKeyspace := spec.Flags.Has(command.FlagKeyspace) || spec.Flags.Has(command.FlagString) ||
				spec.Flags.Has(command.FlagHash) || spec.Flags.Has(command.FlagExpiry)
			if touchesKeyspace && write == read {
				t.Errorf("keyspace command %s must mark exactly one of write or readonly (flags %q)",
					spec.Name, spec.Flags.String())
			}
		})
	}
}

func TestUnknownCommandIsRejected(t *testing.T) {
	h := newHarness(t)
	err := h.fail("NOTACOMMAND", "x")
	if !err.Unknown {
		t.Error("the unknown-command error is not marked as unknown")
	}
	if !strings.Contains(err.Message, "unknown command") {
		t.Errorf("message %q should say the command is unknown", err.Message)
	}

	// A control character in the name must never reach a reply or a log raw.
	msg := h.fail("BAD\x1bNAME").Message
	if strings.ContainsRune(msg, 0x1b) {
		t.Errorf("message %q contains a raw escape byte", msg)
	}
}

func TestArityIsEnforced(t *testing.T) {
	h := newHarness(t)

	for _, args := range [][]string{
		{"GET"},
		{"GET", "a", "b"},
		{"SET", "k"},
		{"INCR"},
		{"HSET", "h", "f"},
		{"ABSOLUTELY_NOT_A_COMMAND"},
	} {
		err := h.fail(args...)
		if err.Message == "" {
			t.Fatalf("%v produced an empty error message", args)
		}
	}

	err := h.fail("GET")
	if !strings.Contains(err.Message, "wrong number of arguments") {
		t.Errorf("message %q should mention the argument count", err.Message)
	}
}

// ---------------------------------------------------------------------------
// Connection commands
// ---------------------------------------------------------------------------

func TestPingAndEcho(t *testing.T) {
	h := newHarness(t)

	h.wantSimple(h.do("PING"), "PONG")
	h.wantSimple(h.do("ping"), "PONG") // command names are case insensitive
	h.wantBulk(h.do("PING", "hello"), "hello")
	h.wantBulk(h.do("ECHO", ""), "")
	h.wantBulk(h.do("ECHO", "with\r\nnewline"), "with\r\nnewline")

	h.fail("PING", "a", "b")
	h.fail("ECHO")
}

func TestQuitClosesTheConnection(t *testing.T) {
	h := newHarness(t)

	raw := [][]byte{[]byte("QUIT")}
	ctx := &command.Context{Args: raw, Store: h.store, Registry: h.reg, Metrics: h.metrics}
	v, err := h.reg.Dispatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h.wantSimple(v, "OK")
	if !ctx.CloseAfterReply {
		t.Fatal("QUIT did not ask the connection to close")
	}
}

func TestSelectOnlyAcceptsDatabaseZero(t *testing.T) {
	h := newHarness(t)
	h.wantSimple(h.do("SELECT", "0"), "OK")
	h.fail("SELECT", "1")
	h.fail("SELECT", "notanumber")
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

func TestAuthenticationGate(t *testing.T) {
	h := newHarness(t)
	h.require = true
	h.runtime.password = "s3cret-password"

	// A protected command before AUTH must be refused.
	gateErr := h.fail("GET", "k")
	if !strings.HasPrefix(gateErr.Message, "NOAUTH") {
		t.Fatalf("message %q should start with NOAUTH", gateErr.Message)
	}

	// Commands explicitly allowed before authentication still work.
	h.wantSimple(h.do("PING"), "PONG")
	h.wantSimple(h.do("QUIT"), "OK")

	if _, err := h.try("AUTH", "wrong"); err == nil {
		t.Fatal("AUTH accepted a wrong password")
	} else {
		var cmdErr *command.Error
		if !errors.As(err, &cmdErr) || !strings.HasPrefix(cmdErr.Message, "WRONGPASS") {
			t.Fatalf("got %v, want WRONGPASS", err)
		}
		if !cmdErr.AuthFailure {
			t.Error("a failed AUTH is not marked as an authentication failure")
		}
	}

	// A correct AUTH marks the context, which is how the connection learns.
	raw := [][]byte{[]byte("AUTH"), []byte("s3cret-password")}
	ctx := &command.Context{
		Args: raw, Store: h.store, Registry: h.reg, Metrics: h.metrics,
		Runtime: h.runtime, AuthRequired: true,
	}
	v, err := h.reg.Dispatch(ctx)
	if err != nil {
		t.Fatalf("AUTH with the right password: %v", err)
	}
	h.wantSimple(v, "OK")
	if !ctx.Authenticated {
		t.Fatal("AUTH did not mark the context as authenticated")
	}

	// The two-argument form is accepted for the implicit default user only.
	h.authed = true
	h.wantSimple(h.do("AUTH", "default", "s3cret-password"), "OK")
	h.fail("AUTH", "someone", "s3cret-password")

	// With authentication on, a protected command works once authenticated.
	h.wantNil(h.do("GET", "missing"))
}

// TestAuthFailsWhenTheRuntimeHasNoPassword covers the defensive case where a
// connection believes authentication is required but the runtime cannot
// validate any credential. Such a server must be impossible to authenticate
// into, instead of accepting the first guess.
func TestAuthFailsWhenTheRuntimeHasNoPassword(t *testing.T) {
	h := newHarness(t)
	h.require = true // required, but the runtime has no password configured
	err := h.fail("AUTH", "anything")
	if !strings.HasPrefix(err.Message, "WRONGPASS") {
		t.Fatalf("message %q should refuse the credential", err.Message)
	}
}

func TestAuthIsRejectedWhenAuthIsNotRequired(t *testing.T) {
	h := newHarness(t)
	err := h.fail("AUTH", "x")
	if !strings.Contains(err.Message, "no password is set") {
		t.Fatalf("message %q should explain that no password is configured", err.Message)
	}
}

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

func TestSetAndGet(t *testing.T) {
	h := newHarness(t)

	h.wantSimple(h.do("SET", "k", "v"), "OK")
	h.wantBulk(h.do("GET", "k"), "v")
	h.wantNil(h.do("GET", "missing"))

	// Overwrite.
	h.wantSimple(h.do("SET", "k", "longer-value"), "OK")
	h.wantBulk(h.do("GET", "k"), "longer-value")

	// Empty values are values, not missing keys.
	h.wantSimple(h.do("SET", "empty", ""), "OK")
	v := h.do("GET", "empty")
	if v.IsNull {
		t.Fatal("an empty value was reported as a missing key")
	}
	h.wantBulk(v, "")
}

func TestSetWithBinaryValues(t *testing.T) {
	h := newHarness(t)

	key := string([]byte{0x00, 0xff, '\r', '\n'})
	value := string([]byte{0x00, 0x01, 0xff, '\r'})
	h.wantSimple(h.do("SET", key, value), "OK")
	h.wantBulk(h.do("GET", key), value)
}

func TestSetNXAndXX(t *testing.T) {
	h := newHarness(t)

	h.wantSimple(h.do("SET", "k", "first", "NX"), "OK")
	h.wantNil(h.do("SET", "k", "second", "NX"))
	h.wantBulk(h.do("GET", "k"), "first")

	h.wantSimple(h.do("SET", "k", "third", "XX"), "OK")
	h.wantBulk(h.do("GET", "k"), "third")
	h.wantNil(h.do("SET", "missing", "v", "XX"))
	h.wantNil(h.do("GET", "missing"))
}

func TestSetExpiryOptions(t *testing.T) {
	h := newHarness(t)

	h.wantSimple(h.do("SET", "k", "v", "EX", "10"), "OK")
	h.wantInt(h.do("TTL", "k"), 10)

	h.clk.Advance(4 * time.Second)
	h.wantInt(h.do("PTTL", "k"), 6000)

	h.wantSimple(h.do("SET", "k", "v2", "KEEPTTL"), "OK")
	h.wantInt(h.do("PTTL", "k"), 6000)

	// A plain SET clears the deadline.
	h.wantSimple(h.do("SET", "k", "v3"), "OK")
	h.wantInt(h.do("TTL", "k"), -1)

	h.wantSimple(h.do("SET", "p", "v", "PX", "2500"), "OK")
	h.wantInt(h.do("PTTL", "p"), 2500)

	// NX with EX is allowed and only applies when the key is absent.
	h.wantNil(h.do("SET", "p", "v", "NX", "EX", "5"))
	h.wantInt(h.do("PTTL", "p"), 2500)
}

func TestSetOptionValidation(t *testing.T) {
	h := newHarness(t)

	for _, args := range [][]string{
		{"SET", "k", "v", "EX"},                  // missing value
		{"SET", "k", "v", "EX", "0"},             // non-positive
		{"SET", "k", "v", "EX", "-1"},            // negative
		{"SET", "k", "v", "EX", "abc"},           // not an integer
		{"SET", "k", "v", "EX", "10", "PX", "1"}, // mutually exclusive
		{"SET", "k", "v", "EX", "10", "KEEPTTL"},
		{"SET", "k", "v", "NX", "XX"},
		{"SET", "k", "v", "NOTANOPTION"},
		{"SET", "k", "v", "EX", "99999999999999999999"},
	} {
		err := h.fail(args...)
		if err.Message == "" {
			t.Fatalf("%v produced an empty message", args)
		}
	}
	if got := h.store.Exists("k"); got != 0 {
		t.Fatal("a rejected SET still created the key")
	}
}

func TestMSetAndMGet(t *testing.T) {
	h := newHarness(t)

	h.wantSimple(h.do("MSET", "a", "1", "b", "2"), "OK")
	v := h.do("MGET", "a", "b", "missing")
	if v.Type != protocol.TypeArray || len(v.Array) != 3 {
		t.Fatalf("MGET returned %s", v.Render())
	}
	h.wantBulk(v.Array[0], "1")
	h.wantBulk(v.Array[1], "2")
	h.wantNil(v.Array[2])

	h.fail("MSET", "a", "1", "b") // odd number of arguments
}

func TestMSetClearsDeadlines(t *testing.T) {
	h := newHarness(t)
	h.wantSimple(h.do("SET", "a", "1", "EX", "100"), "OK")
	h.wantInt(h.do("TTL", "a"), 100)

	h.wantSimple(h.do("MSET", "a", "2"), "OK")
	h.wantInt(h.do("TTL", "a"), -1)
}

func TestDelAndExists(t *testing.T) {
	h := newHarness(t)

	h.wantSimple(h.do("MSET", "a", "1", "b", "2", "c", "3"), "OK")
	h.wantInt(h.do("EXISTS", "a", "b", "missing"), 2)
	h.wantInt(h.do("EXISTS", "a", "a"), 2) // duplicates count, as in Redis
	h.wantInt(h.do("DEL", "a", "c", "missing"), 2)
	h.wantInt(h.do("EXISTS", "a"), 0)
	h.wantInt(h.do("DEL", "missing"), 0)
}

func TestIncrementCommands(t *testing.T) {
	h := newHarness(t)

	h.wantInt(h.do("INCR", "n"), 1)
	h.wantInt(h.do("INCR", "n"), 2)
	h.wantInt(h.do("INCRBY", "n", "40"), 42)
	h.wantInt(h.do("DECR", "n"), 41)
	h.wantInt(h.do("DECRBY", "n", "41"), 0)
	h.wantBulk(h.do("GET", "n"), "0")

	// A fresh key starts from zero.
	h.wantInt(h.do("DECR", "fresh"), -1)
	h.wantInt(h.do("INCRBY", "fresh", "-1"), -2)
}

func TestIncrementErrors(t *testing.T) {
	h := newHarness(t)

	h.wantSimple(h.do("SET", "text", "hello"), "OK")
	err := h.fail("INCR", "text")
	if !strings.Contains(err.Message, "not an integer") {
		t.Fatalf("message %q should explain the value is not an integer", err.Message)
	}

	h.wantSimple(h.do("SET", "max", "9223372036854775807"), "OK")
	h.fail("INCR", "max")
	h.wantBulk(h.do("GET", "max"), "9223372036854775807")

	h.fail("INCRBY", "n", "notanumber")
	h.fail("INCRBY", "n")
	h.fail("DECRBY", "n", "-9223372036854775808")
}

func TestAppendAndStrLen(t *testing.T) {
	h := newHarness(t)

	h.wantInt(h.do("APPEND", "k", "abc"), 3)
	h.wantInt(h.do("APPEND", "k", "de"), 5)
	h.wantBulk(h.do("GET", "k"), "abcde")
	h.wantInt(h.do("STRLEN", "k"), 5)
	h.wantInt(h.do("STRLEN", "missing"), 0)
	h.wantInt(h.do("APPEND", "k", ""), 5)
}

// ---------------------------------------------------------------------------
// Hashes
// ---------------------------------------------------------------------------

func TestHashCommands(t *testing.T) {
	h := newHarness(t)

	h.wantInt(h.do("HSET", "h", "f1", "v1", "f2", "v2"), 2)
	h.wantInt(h.do("HSET", "h", "f1", "updated"), 0)
	h.wantInt(h.do("HSET", "h", "f3", "v3", "f3", "dup"), 1)

	h.wantBulk(h.do("HGET", "h", "f1"), "updated")
	h.wantNil(h.do("HGET", "h", "missing"))
	h.wantNil(h.do("HGET", "missing", "f"))
	h.wantInt(h.do("HLEN", "h"), 3)
	h.wantInt(h.do("HLEN", "missing"), 0)
	h.wantInt(h.do("HEXISTS", "h", "f1"), 1)
	h.wantInt(h.do("HEXISTS", "h", "nope"), 0)
	h.wantInt(h.do("HEXISTS", "missing", "f"), 0)

	pairs := h.do("HGETALL", "h")
	if pairs.Type != protocol.TypeArray || len(pairs.Array) != 6 {
		t.Fatalf("HGETALL returned %s", pairs.Render())
	}
	// Fields must come back sorted: f1, f2, f3.
	wantFields := []string{"f1", "f2", "f3"}
	for i, want := range wantFields {
		if got := string(pairs.Array[i*2].Str); got != want {
			t.Fatalf("field %d = %q, want %q (order must be deterministic)", i, got, want)
		}
	}

	if empty := h.do("HGETALL", "missing"); len(empty.Array) != 0 {
		t.Fatalf("HGETALL on a missing key returned %s", empty.Render())
	}
}

func TestHashMultiGet(t *testing.T) {
	h := newHarness(t)
	h.wantInt(h.do("HSET", "h", "a", "1", "c", "3"), 2)

	v := h.do("HMGET", "h", "a", "b", "c")
	if len(v.Array) != 3 {
		t.Fatalf("HMGET returned %s", v.Render())
	}
	h.wantBulk(v.Array[0], "1")
	h.wantNil(v.Array[1])
	h.wantBulk(v.Array[2], "3")

	// A missing key yields nulls rather than an error.
	missing := h.do("HMGET", "missing", "a")
	if len(missing.Array) != 1 {
		t.Fatalf("HMGET returned %s", missing.Render())
	}
	h.wantNil(missing.Array[0])
}

func TestHashDeleteRemovesEmptyKey(t *testing.T) {
	h := newHarness(t)
	h.wantInt(h.do("HSET", "h", "a", "1", "b", "2"), 2)

	h.wantInt(h.do("HDEL", "h", "a", "a", "nope"), 1)
	h.wantInt(h.do("HDEL", "h", "b"), 1)
	h.wantSimple(h.do("TYPE", "h"), "none")
	h.wantInt(h.do("HDEL", "h", "a"), 0)
	h.wantInt(h.do("HDEL", "missing", "a"), 0)

	h.fail("HSET", "h", "f")
}

func TestHashTypeErrors(t *testing.T) {
	h := newHarness(t)
	h.wantSimple(h.do("SET", "str", "value"), "OK")
	h.wantInt(h.do("HSET", "hash", "f", "v"), 1)

	for _, args := range [][]string{
		{"HSET", "str", "f", "v"},
		{"HGET", "str", "f"},
		{"HMGET", "str", "f"},
		{"HDEL", "str", "f"},
		{"HEXISTS", "str", "f"},
		{"HLEN", "str"},
		{"HGETALL", "str"},
	} {
		err := h.fail(args...)
		if !strings.HasPrefix(err.Message, "WRONGTYPE") {
			t.Fatalf("%v returned %q, want a WRONGTYPE error", args, err.Message)
		}
	}

	for _, args := range [][]string{
		{"GET", "hash"},
		{"INCR", "hash"},
		{"APPEND", "hash", "x"},
		{"STRLEN", "hash"},
	} {
		err := h.fail(args...)
		if !strings.HasPrefix(err.Message, "WRONGTYPE") {
			t.Fatalf("%v returned %q, want a WRONGTYPE error", args, err.Message)
		}
	}

	// MGET reports null for a key of the wrong type instead of failing.
	mget := h.do("MGET", "str", "hash")
	h.wantBulk(mget.Array[0], "value")
	h.wantNil(mget.Array[1])
}

func TestTypeAndDBSize(t *testing.T) {
	h := newHarness(t)

	h.wantSimple(h.do("TYPE", "missing"), "none")
	h.wantInt(h.do("DBSIZE"), 0)

	h.wantSimple(h.do("SET", "str", "v"), "OK")
	h.wantInt(h.do("HSET", "hash", "f", "v"), 1)
	h.wantSimple(h.do("TYPE", "str"), "string")
	h.wantSimple(h.do("TYPE", "hash"), "hash")
	h.wantInt(h.do("DBSIZE"), 2)

	h.wantSimple(h.do("FLUSHDB"), "OK")
	h.wantInt(h.do("DBSIZE"), 0)
}

// ---------------------------------------------------------------------------
// Expiration
// ---------------------------------------------------------------------------

func TestExpireAndTTL(t *testing.T) {
	h := newHarness(t)
	h.wantSimple(h.do("SET", "k", "v"), "OK")

	h.wantInt(h.do("TTL", "k"), -1)
	h.wantInt(h.do("PTTL", "k"), -1)
	h.wantInt(h.do("TTL", "missing"), -2)
	h.wantInt(h.do("PTTL", "missing"), -2)

	h.wantInt(h.do("EXPIRE", "k", "100"), 1)
	h.wantInt(h.do("TTL", "k"), 100)
	h.wantInt(h.do("PTTL", "k"), 100_000)

	h.clk.Advance(40 * time.Second)
	h.wantInt(h.do("TTL", "k"), 60)

	h.wantInt(h.do("PEXPIRE", "k", "500"), 1)
	h.wantInt(h.do("PTTL", "k"), 500)

	h.wantInt(h.do("EXPIRE", "missing", "10"), 0)
}

func TestExpireAtAndPExpireAt(t *testing.T) {
	h := newHarness(t)
	h.wantSimple(h.do("SET", "k", "v"), "OK")

	// An absolute deadline of one hour from now.
	absoluteSeconds := (startMS + 3_600_000) / 1000
	h.wantInt(h.do("EXPIREAT", "k", fmt.Sprint(absoluteSeconds)), 1)
	h.wantInt(h.do("PTTL", "k"), 3_600_000)

	h.wantInt(h.do("PEXPIREAT", "k", fmt.Sprint(startMS+1000)), 1)
	h.wantInt(h.do("PTTL", "k"), 1000)

	// A deadline in the past removes the key immediately, and Redis reports
	// that the timeout was set.
	h.wantInt(h.do("EXPIREAT", "k", "1"), 1)
	h.wantInt(h.do("EXISTS", "k"), 0)
}

func TestExpireWithNonPositiveValuesRemovesTheKey(t *testing.T) {
	h := newHarness(t)

	h.wantSimple(h.do("SET", "a", "v"), "OK")
	h.wantInt(h.do("EXPIRE", "a", "0"), 1)
	h.wantInt(h.do("EXISTS", "a"), 0)

	h.wantSimple(h.do("SET", "b", "v"), "OK")
	h.wantInt(h.do("PEXPIRE", "b", "-1"), 1)
	h.wantInt(h.do("EXISTS", "b"), 0)
}

func TestExpireOverflowIsRejected(t *testing.T) {
	h := newHarness(t)
	h.wantSimple(h.do("SET", "k", "v"), "OK")

	for _, args := range [][]string{
		{"EXPIRE", "k", "9223372036854775807"},
		{"EXPIRE", "k", "-9223372036854775808"},
		{"PEXPIRE", "k", "9223372036854775807"},
		{"EXPIREAT", "k", "9223372036854775807"},
		{"EXPIREAT", "k", "-9223372036854775808"},
	} {
		if err := h.fail(args...); !strings.Contains(err.Message, "overflow") {
			t.Fatalf("%v returned %q, want an overflow message", args, err.Message)
		}
	}

	// PEXPIREAT takes an absolute millisecond timestamp, so the largest int64
	// is a legal (if absurd) deadline rather than an overflow.
	h.wantInt(h.do("PEXPIREAT", "k", "9223372036854775807"), 1)
	h.wantInt(h.do("PERSIST", "k"), 1)
	// The key must be untouched by the rejected command.
	h.wantInt(h.do("TTL", "k"), -1)
}

func TestExpiredKeyBehavesAsMissing(t *testing.T) {
	h := newHarness(t)
	h.wantSimple(h.do("SET", "k", "v", "PX", "10"), "OK")

	h.clk.Advance(10 * time.Millisecond)

	h.wantNil(h.do("GET", "k"))
	h.wantInt(h.do("EXISTS", "k"), 0)
	h.wantInt(h.do("TTL", "k"), -2)
	h.wantInt(h.do("PTTL", "k"), -2)
	h.wantSimple(h.do("TYPE", "k"), "none")
	h.wantInt(h.do("DBSIZE"), 0)
	// No positive TTL may ever be reported for an expired key.
	if v := h.do("PTTL", "k"); v.Int > 0 {
		t.Fatalf("PTTL returned %d for an expired key", v.Int)
	}
}

func TestPersist(t *testing.T) {
	h := newHarness(t)
	h.wantSimple(h.do("SET", "k", "v", "EX", "10"), "OK")

	h.wantInt(h.do("PERSIST", "k"), 1)
	h.wantInt(h.do("TTL", "k"), -1)
	h.wantInt(h.do("PERSIST", "k"), 0) // already persistent
	h.wantInt(h.do("PERSIST", "missing"), 0)

	h.clk.Advance(60 * time.Second)
	h.wantBulk(h.do("GET", "k"), "v") // no deadline, still there
}

func TestTTLRounding(t *testing.T) {
	h := newHarness(t)
	h.wantSimple(h.do("SET", "k", "v", "PX", "1600"), "OK")

	// 1600 ms rounds to 2 seconds, matching Redis's nearest-second rounding.
	h.wantInt(h.do("TTL", "k"), 2)
	h.wantInt(h.do("PTTL", "k"), 1600)
}

// ---------------------------------------------------------------------------
// Keyspace traversal
// ---------------------------------------------------------------------------

func TestKeysPatterns(t *testing.T) {
	h := newHarness(t)
	h.wantSimple(h.do("MSET",
		"user:1", "a", "user:2", "b", "session:1", "c", "us", "d", "user-x", "e"), "OK")

	got := h.do("KEYS", "user:*")
	if len(got.Array) != 2 {
		t.Fatalf("KEYS user:* returned %d keys: %s", len(got.Array), got.Render())
	}
	// Sorted, so the result is deterministic. "user-x" must not match: the
	// literal ':' in the pattern has to be present in the key.
	want := []string{"user:1", "user:2"}
	for i, w := range want {
		if string(got.Array[i].Str) != w {
			t.Fatalf("key %d = %q, want %q", i, got.Array[i].Str, w)
		}
	}

	if all := h.do("KEYS", "*"); len(all.Array) != 5 {
		t.Fatalf("KEYS * returned %d keys, want 5", len(all.Array))
	}
	if none := h.do("KEYS", "nomatch*"); len(none.Array) != 0 {
		t.Fatalf("KEYS nomatch* returned %s", none.Render())
	}
}

func TestScanTraversal(t *testing.T) {
	h := newHarness(t)
	for i := 0; i < 25; i++ {
		h.wantSimple(h.do("SET", fmt.Sprintf("key:%02d", i), "v"), "OK")
	}

	seen := map[string]bool{}
	cursor := "0"
	iterations := 0
	for {
		iterations++
		if iterations > 100 {
			t.Fatal("SCAN did not terminate")
		}
		v := h.do("SCAN", cursor, "COUNT", "7")
		if len(v.Array) != 2 {
			t.Fatalf("SCAN returned %s", v.Render())
		}
		cursor = string(v.Array[0].Str)
		for _, k := range v.Array[1].Array {
			seen[string(k.Str)] = true
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != 25 {
		t.Fatalf("SCAN visited %d keys, want 25", len(seen))
	}

	// MATCH filters without losing the cursor contract.
	v := h.do("SCAN", "0", "MATCH", "key:0*", "COUNT", "100")
	if got := len(v.Array[1].Array); got != 10 {
		t.Fatalf("SCAN MATCH key:0* returned %d keys, want 10", got)
	}

	// A cursor beyond the keyspace ends the traversal.
	end := h.do("SCAN", "9999")
	if string(end.Array[0].Str) != "0" || len(end.Array[1].Array) != 0 {
		t.Fatalf("SCAN past the end returned %s", end.Render())
	}

	h.fail("SCAN", "notacursor")
	h.fail("SCAN", "0", "COUNT", "0")
	h.fail("SCAN", "0", "BOGUS")
}

func TestFlushDB(t *testing.T) {
	h := newHarness(t)
	h.wantSimple(h.do("SET", "a", "1"), "OK")
	h.wantSimple(h.do("SET", "b", "2", "EX", "100"), "OK")

	h.wantSimple(h.do("FLUSHDB"), "OK")
	h.wantInt(h.do("DBSIZE"), 0)
	h.wantInt(h.do("EXISTS", "a", "b"), 0)
}

func TestCommandIntrospection(t *testing.T) {
	h := newHarness(t)

	count := h.do("COMMAND", "COUNT")
	h.wantInt(count, int64(h.reg.Len()))

	all := h.do("COMMAND")
	if int64(len(all.Array)) != count.Int {
		t.Fatalf("COMMAND returned %d entries, COMMAND COUNT says %d", len(all.Array), count.Int)
	}

	info := h.do("COMMAND", "INFO", "get")
	if len(info.Array) != 1 || len(info.Array[0].Array) != 5 {
		t.Fatalf("COMMAND INFO get returned %s", info.Render())
	}

	// An unknown command yields a null entry rather than an error.
	unknown := h.do("COMMAND", "INFO", "nope")
	h.wantNil(unknown.Array[0])

	h.fail("COMMAND", "BOGUS")
}

// ---------------------------------------------------------------------------
// INFO
// ---------------------------------------------------------------------------

func TestInfoSections(t *testing.T) {
	h := newHarness(t)
	h.runtime.info = command.Info{
		Version:       "0.1.0",
		Mode:          "standalone",
		GoVersion:     "go1.test",
		OS:            "linux",
		Arch:          "amd64",
		ProcessID:     4242,
		Bind:          "127.0.0.1",
		Port:          6379,
		UptimeSeconds: 12,
		DataDir:       "/tmp/data",
		Clients:       command.ClientStats{ConnectedClients: 2, MaxClients: 512, TotalConnections: 9, RejectedConnections: 1},
		Store:         command.StoreStats{Keys: 3, Expires: 1, ApproxBytes: 42, HeapAllocBytes: 1 << 20, HeapSysBytes: 1 << 21},
		Exec:          command.ExecStats{CommandsProcessed: 100, CommandErrors: 4, ProtocolErrors: 1, KeysExpired: 7, AuthFailures: 2, AvgCommandMicros: 1.5, CommandsPerSecond: 8.25},
		Persistence: command.PersistenceInfo{
			Enabled: true, JournalEnabled: true, JournalPath: "/tmp/data/miniredis.mrj",
			JournalSizeBytes: 4096, JournalAppends: 100, JournalFsyncs: 100, FsyncMode: "always",
			SnapshotPath: "/tmp/data/miniredis.mrs", SnapshotExists: true, SnapshotLastSeq: 100,
			Recovery: &command.RecoverySummary{Status: "repaired", JournalRecords: 9, JournalApplied: 8, JournalSkipped: 1, DiscardedBytes: 12, Repaired: true, Warnings: []string{"tail repaired"}},
		},
		CommandStat: []observability.CommandStatSnapshot{{Name: "PING", Calls: 5, Errors: 0, Micros: 10}},
	}

	defaultSections := h.do("INFO")
	text := string(defaultSections.Str)
	for _, want := range []string{
		"# Server", "# Clients", "# Memory", "# Persistence", "# Stats", "# Keyspace",
		"miniredis_version:0.1.0",
		"tcp_bind:127.0.0.1",
		"connected_clients:2",
		"db0:keys=3,expires=1",
		"journal_size_bytes:4096",
		"recovery_status:repaired",
		"recovery_discarded_bytes:12",
		"total_commands_processed:100",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("default INFO is missing %q", want)
		}
	}
	if strings.Contains(text, "# Commandstats") {
		t.Error("the default INFO should not include commandstats")
	}

	all := string(h.do("INFO", "all").Str)
	if !strings.Contains(all, "# Commandstats") || !strings.Contains(all, "cmdstat_ping:calls=5") {
		t.Error("INFO all should include commandstats")
	}

	// No password may ever appear in INFO, even if one is configured.
	h.require = true
	h.authed = true
	h.runtime.password = "hunter2-should-never-appear"
	withAuth := string(h.do("INFO", "all").Str)
	if strings.Contains(withAuth, "hunter2-should-never-appear") {
		t.Fatal("INFO exposed the configured password")
	}

	// A named section returns only that section.
	server := string(h.do("INFO", "server").Str)
	if !strings.Contains(server, "# Server") || strings.Contains(server, "# Keyspace") {
		t.Errorf("INFO server returned the wrong sections:\n%s", server)
	}

	// An unknown section is an empty reply, not an error.
	if empty := h.do("INFO", "nosuchsection"); len(empty.Str) != 0 {
		t.Errorf("INFO nosuchsection returned %q, want empty", empty.Str)
	}
}

func TestInfoWithoutRuntime(t *testing.T) {
	h := newHarness(t)
	h.runtime = nil
	// The command layer must still answer sensibly when it has no server.
	raw := [][]byte{[]byte("INFO")}
	ctx := &command.Context{Args: raw, Store: h.store, Registry: h.reg, Metrics: h.metrics}
	v, err := h.reg.Dispatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(v.Str), "miniredis_version:unknown") {
		t.Errorf("fallback INFO = %q", v.Str)
	}
}

func TestShutdownRequestsServerStop(t *testing.T) {
	h := newHarness(t)

	raw := [][]byte{[]byte("SHUTDOWN")}
	ctx := &command.Context{Args: raw, Store: h.store, Registry: h.reg, Metrics: h.metrics, Runtime: h.runtime}
	if _, err := h.reg.Dispatch(ctx); err != nil {
		t.Fatal(err)
	}
	if !ctx.CloseAfterReply {
		t.Error("SHUTDOWN did not ask the connection to close")
	}
	if h.runtime.shutdowns != 1 {
		t.Errorf("SHUTDOWN requested %d shutdowns, want 1", h.runtime.shutdowns)
	}
	h.fail("SHUTDOWN", "BOGUS")
	h.fail("SHUTDOWN", "SAVE", "EXTRA")
}

// ---------------------------------------------------------------------------
// Glob matching
// ---------------------------------------------------------------------------

func TestGlobMatching(t *testing.T) {
	cases := []struct {
		pattern string
		value   string
		want    bool
	}{
		{"*", "", true},
		{"*", "anything", true},
		{"", "", true},
		{"", "x", false},
		{"a", "a", true},
		{"a", "b", false},
		{"?", "a", true},
		{"?", "", false},
		{"?", "ab", false},
		{"a?c", "abc", true},
		{"a?c", "ac", false},
		{"user:*", "user:42", true},
		{"user:*", "session:42", false},
		{"*:*", "user:42", true},
		{"*:*", "user42", false},
		{"a*b*c", "aXbYc", true},
		{"a*b*c", "aXcYb", false},
		{"[abc]", "b", true},
		{"[abc]", "d", false},
		{"[a-z]x", "mx", true},
		{"[^abc]", "d", true},
		{"[^abc]", "a", false},
		{"[0-9][0-9]", "42", true},
		{"[0-9][0-9]", "4a", false},
		{"\\*", "*", true},
		{"\\*", "x", false},
		{"[*]", "*", true},
		{"[", "[", true},       // malformed class is a literal
		{"[abc", "[abc", true}, // no closing bracket
		{"*a*a*a*a*a*b", "aaaaaaaaaaaaaaaaaaaaaaaaac", false},
	}

	for _, tc := range cases {
		t.Run(tc.pattern+"/"+tc.value, func(t *testing.T) {
			h := newHarness(t)
			h.wantSimple(h.do("SET", tc.value, "v"), "OK")

			got := h.do("KEYS", tc.pattern)
			if len(got.Array) != boolToLen(tc.want) {
				t.Fatalf("KEYS %q on %q matched %d keys, want %v", tc.pattern, tc.value, len(got.Array), tc.want)
			}
		})
	}
}

func boolToLen(v bool) int {
	if v {
		return 1
	}
	return 0
}

// ---------------------------------------------------------------------------
// Persistence failure mapping
// ---------------------------------------------------------------------------

// failingRecorder reports every write as failed, which is how the command
// layer's error mapping is exercised end to end.
type failingRecorder struct {
	err       error
	ambiguous bool
}

func (f *failingRecorder) Record(store.Mutation) (uint64, error) {
	if f.ambiguous {
		return 0, &store.AmbiguousAppendError{Err: f.err}
	}
	return 0, f.err
}

func (f *failingRecorder) Seq() uint64 { return 0 }

func TestPersistenceFailureIsReportedNotSwallowed(t *testing.T) {
	clk := clock.NewManual(startMS)
	rec := &failingRecorder{err: errors.New("no space left on device")}
	st := store.New(clk, store.Options{Recorder: rec})

	reg := command.NewDefaultRegistry()
	ctx := &command.Context{
		Args:  [][]byte{[]byte("SET"), []byte("k"), []byte("v")},
		Store: st, Registry: reg, Metrics: observability.New(),
	}
	_, err := reg.Dispatch(ctx)
	if err == nil {
		t.Fatal("SET succeeded although the journal could not be written")
	}
	var cmdErr *command.Error
	if !errors.As(err, &cmdErr) {
		t.Fatalf("got %T, want *command.Error", err)
	}
	if !strings.HasPrefix(cmdErr.Message, "MISCONF") {
		t.Fatalf("message %q should be a MISCONF error", cmdErr.Message)
	}
	if _, ok, _ := st.Get("k"); ok {
		t.Fatal("the value was stored even though the write failed")
	}

	// An ambiguous failure is reported as such, and the write is applied.
	rec.ambiguous = true
	if _, err := reg.Dispatch(ctx); err == nil {
		t.Fatal("an ambiguous append failure was not reported")
	}
	if _, ok, _ := st.Get("k"); !ok {
		t.Fatal("an ambiguous append should still apply the mutation")
	}
}
