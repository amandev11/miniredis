// Package command implements MiniRedis's command dispatch and the commands
// themselves.
//
// Every command is registered with its name, arity, flags and handler, so
// there is no central switch statement. INFO, the HTTP diagnostics endpoint and
// the CLI's help all read the same registry, which means a command that exists
// is also documented and dispatcheable.
//
// Commands only ever touch the real store and the real protocol encoder: no
// handler fabricates a response that does not reflect database state.
package command

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/amandev11/miniredis/internal/observability"
	"github.com/amandev11/miniredis/internal/protocol"
	"github.com/amandev11/miniredis/internal/store"
)

// Flags describe what a command does. They drive introspection (COMMAND,
// INFO) and the authentication gate.
type Flags uint32

// Command flags.
const (
	// FlagWrite marks a command that can mutate the keyspace.
	FlagWrite Flags = 1 << iota
	// FlagReadOnly marks a command that only reads.
	FlagReadOnly
	// FlagConnection marks connection-level commands such as PING.
	FlagConnection
	// FlagKeyspace marks commands that work on keys in general.
	FlagKeyspace
	// FlagString marks string commands.
	FlagString
	// FlagHash marks hash commands.
	FlagHash
	// FlagExpiry marks expiration commands.
	FlagExpiry
	// FlagServer marks administrative and diagnostic commands.
	FlagServer
	// FlagAdmin marks commands that can stop the server or wipe data.
	FlagAdmin
	// FlagNoAuth marks commands allowed before authentication succeeds.
	FlagNoAuth
)

// Has reports whether every bit in want is set.
func (f Flags) Has(want Flags) bool { return f&want == want }

// String renders the flags for COMMAND output.
func (f Flags) String() string {
	names := []struct {
		flag Flags
		name string
	}{
		{FlagWrite, "write"},
		{FlagReadOnly, "readonly"},
		{FlagConnection, "connection"},
		{FlagKeyspace, "keyspace"},
		{FlagString, "string"},
		{FlagHash, "hash"},
		{FlagExpiry, "expiry"},
		{FlagServer, "server"},
		{FlagAdmin, "admin"},
	}
	var out []string
	for _, n := range names {
		if f.Has(n.flag) {
			out = append(out, n.name)
		}
	}
	return strings.Join(out, " ")
}

// Handler executes one command and returns its reply.
//
// Returning a *Error produces a RESP error frame with that exact message.
// Returning any other error is an internal failure: the server logs it and
// replies with a generic error so internals never leak to a client.
type Handler func(c *Context) (protocol.Value, error)

// Spec describes one command.
type Spec struct {
	// Name is the upper-case command name.
	Name string
	// Arity follows the Redis convention: a positive value is the exact number
	// of arguments including the name, a negative value is the minimum.
	Arity int
	// Flags describe behavior.
	Flags Flags
	// Hint is the argument hint shown by the CLI, for example "key value".
	Hint string
	// Summary is a one-line description.
	Summary string
	// Handler implements the command.
	Handler Handler
}

// accepts reports whether n arguments satisfy the arity rule.
func (s *Spec) accepts(n int) bool {
	switch {
	case s.Arity > 0:
		return n == s.Arity
	case s.Arity < 0:
		return n >= -s.Arity
	default:
		return true
	}
}

// arityText renders the arity rule for help output.
func (s *Spec) arityText() string {
	switch {
	case s.Arity > 0:
		return fmt.Sprintf("exactly %d argument(s) including the name", s.Arity)
	case s.Arity < 0:
		return fmt.Sprintf("at least %d argument(s) including the name", -s.Arity)
	default:
		return "any number of arguments"
	}
}

// Registry maps command names to specifications.
type Registry struct {
	specs map[string]*Spec
	names []string
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{specs: make(map[string]*Spec)}
}

// Register adds a command. Registering the same name twice panics, because it
// would make dispatch depend on initialization order.
func (r *Registry) Register(s *Spec) {
	name := strings.ToUpper(s.Name)
	if _, exists := r.specs[name]; exists {
		panic("command: duplicate registration for " + name)
	}
	cp := *s
	cp.Name = name
	r.specs[name] = &cp
	r.names = append(r.names, name)
	sort.Strings(r.names)
}

// Lookup finds a command by name. The name is upper-cased first.
func (r *Registry) Lookup(name string) (*Spec, bool) {
	s, ok := r.specs[strings.ToUpper(name)]
	return s, ok
}

// Names returns every registered command name in sorted order.
func (r *Registry) Names() []string {
	return append([]string(nil), r.names...)
}

// Specs returns every registered command in sorted order.
func (r *Registry) Specs() []*Spec {
	out := make([]*Spec, 0, len(r.names))
	for _, n := range r.names {
		out = append(out, r.specs[n])
	}
	return out
}

// Len returns the number of registered commands.
func (r *Registry) Len() int { return len(r.specs) }

// Context carries everything a handler needs.
type Context struct {
	// Args is the full argument list including the command name.
	Args [][]byte
	// Cmd is the resolved command specification.
	Cmd *Spec
	// Store is the live in-memory database.
	Store *store.Store
	// Metrics receives per-command counters.
	Metrics *observability.Metrics
	// Runtime exposes server state to INFO and SHUTDOWN.
	Runtime RuntimeInfo
	// Registry is the command table, used by COMMAND and INFO.
	Registry *Registry
	// Authed reports whether this connection may run protected commands.
	Authed bool
	// AuthRequired reports whether the server requires authentication.
	AuthRequired bool
	// ClientID is the connection's identifier.
	ClientID uint64
	// RemoteAddr is the client's address.
	RemoteAddr string

	// Authenticated is set by AUTH on success so the connection can update its
	// state without the command reaching into the connection.
	Authenticated bool
	// CloseAfterReply asks the connection to close once the reply is written.
	CloseAfterReply bool
}

// Name returns the upper-cased command name.
func (c *Context) Name() string {
	if len(c.Args) == 0 {
		return ""
	}
	return strings.ToUpper(string(c.Args[0]))
}

// Arg returns argument i after the command name, or nil when absent.
func (c *Context) Arg(i int) []byte {
	if i+1 >= len(c.Args) {
		return nil
	}
	return c.Args[i+1]
}

// NumArgs returns the number of arguments excluding the command name.
func (c *Context) NumArgs() int {
	if len(c.Args) == 0 {
		return 0
	}
	return len(c.Args) - 1
}

// now returns the current time in Unix milliseconds from the store's clock, so
// that tests can drive expiration deterministically.
func (c *Context) now() int64 { return c.Store.Clock().NowMS() }

// StoreStats is the keyspace section of INFO.
type StoreStats struct {
	Keys           int
	Expires        int
	ApproxBytes    int64
	HeapAllocBytes uint64
	HeapSysBytes   uint64
}

// ExecStats is the stats section of INFO.
type ExecStats struct {
	CommandsProcessed uint64
	CommandErrors     uint64
	ProtocolErrors    uint64
	KeysExpired       uint64
	AuthFailures      uint64
	AvgCommandMicros  float64
	CommandsPerSecond float64
}

// ClientStats is the clients section of INFO.
type ClientStats struct {
	ConnectedClients    int
	MaxClients          int
	TotalConnections    uint64
	RejectedConnections uint64
}

// RecoverySummary is the recovery portion of the persistence section of INFO.
type RecoverySummary struct {
	Status         string
	DurationMS     int64
	CompletedAtMS  int64
	SnapshotLoaded bool
	SnapshotSeq    uint64
	SnapshotKeys   int
	JournalRecords int
	JournalApplied int
	JournalSkipped int
	JournalOrphans int
	DiscardedBytes int64
	Repaired       bool
	Warnings       []string
}

// PersistenceInfo is the persistence section of INFO.
type PersistenceInfo struct {
	Enabled             bool
	JournalEnabled      bool
	JournalPath         string
	JournalSizeBytes    int64
	JournalSeq          uint64
	JournalAppends      uint64
	JournalFsyncs       uint64
	JournalLastFsyncMS  int64
	JournalWriteErrors  uint64
	JournalPoisoned     bool
	JournalPoisonReason string
	FsyncMode           string
	SnapshotPath        string
	SnapshotExists      bool
	SnapshotCount       uint64
	SnapshotLastMS      int64
	SnapshotLastSeq     uint64
	SnapshotLastBytes   int64
	LastError           string
	Recovery            *RecoverySummary
}

// Info is the real runtime state that INFO and the diagnostics endpoint
// report. Every field is read from the running server.
type Info struct {
	Version       string
	Mode          string
	GoVersion     string
	OS            string
	Arch          string
	ProcessID     int
	Bind          string
	Port          int
	UptimeSeconds int64
	DataDir       string

	Clients     ClientStats
	Store       StoreStats
	Exec        ExecStats
	Persistence PersistenceInfo
	CommandStat []observability.CommandStatSnapshot
}

// RuntimeInfo is implemented by the server so that commands can report real
// runtime state without importing the server package.
type RuntimeInfo interface {
	// ServerInfo returns a consistent snapshot of server state.
	ServerInfo() Info
	// RequestShutdown asks the server to shut down after the current reply.
	RequestShutdown()
	// CheckPassword reports whether supplied matches the configured password,
	// using a constant-time comparison. It returns false when no password is
	// configured, so an unauthenticated server can never be authenticated into.
	CheckPassword(supplied []byte) bool
}

// Error is a command failure whose Message is already a RESP error string.
type Error struct {
	// Message is sent to the client verbatim.
	Message string
	// Unknown marks "unknown command", which the CLI uses to offer help.
	Unknown bool
	// AuthFailure marks a failed AUTH, which the connection counts so it can
	// drop a client that keeps guessing.
	AuthFailure bool
}

// Error implements error.
func (e *Error) Error() string { return e.Message }

// errorf builds a *Error.
func errorf(format string, args ...any) *Error {
	return &Error{Message: fmt.Sprintf(format, args...)}
}

// Well-known errors.
var (
	// ErrNoAuth is returned for a protected command before AUTH succeeded.
	ErrNoAuth = &Error{Message: "NOAUTH Authentication required."}
	// ErrWrongPass is returned for a failed AUTH.
	ErrWrongPass = &Error{Message: "WRONGPASS invalid username-password pair or user is disabled.", AuthFailure: true}
)

// Dispatch resolves and runs one command.
//
// It enforces arity, the authentication gate and error mapping, so individual
// handlers never have to check whether the caller is allowed to be there.
func (r *Registry) Dispatch(ctx *Context) (protocol.Value, error) {
	if len(ctx.Args) == 0 {
		return protocol.Value{}, errorf("ERR empty command")
	}
	name := ctx.Name()
	spec, ok := r.specs[name]
	if !ok {
		return protocol.Value{}, &Error{
			Message: fmt.Sprintf("ERR unknown command '%s'", sanitiseForMessage(ctx.Args[0])),
			Unknown: true,
		}
	}
	ctx.Cmd = spec

	if !spec.accepts(len(ctx.Args)) {
		return protocol.Value{}, errorf("ERR wrong number of arguments for '%s' command",
			strings.ToLower(spec.Name))
	}
	if ctx.AuthRequired && !ctx.Authed && !spec.Flags.Has(FlagNoAuth) {
		return protocol.Value{}, ErrNoAuth
	}

	value, err := spec.Handler(ctx)
	if err != nil {
		return protocol.Value{}, mapError(err)
	}
	return value, nil
}

// mapError turns a store or handler error into a client-facing error.
//
// Storage failures are reported as MISCONF, like Redis does, because a client
// must be able to tell "your write may not be durable" apart from "your
// request was wrong". Persistence messages contain file paths but never stored
// values.
func mapError(err error) error {
	var cmdErr *Error
	if errors.As(err, &cmdErr) {
		return cmdErr
	}

	switch {
	case errors.Is(err, store.ErrWrongType):
		return &Error{Message: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	case errors.Is(err, store.ErrNotAnInteger):
		return &Error{Message: "ERR value is not an integer or out of range"}
	case errors.Is(err, store.ErrIntOverflow):
		return &Error{Message: "ERR increment or decrement would overflow"}
	case errors.Is(err, store.ErrValueTooLarge):
		return &Error{Message: "ERR string exceeds the configured value size limit"}
	case errors.Is(err, store.ErrKeyTooLarge):
		return &Error{Message: "ERR key exceeds the configured key size limit"}
	case errors.Is(err, store.ErrHashTooLarge):
		return &Error{Message: "ERR hash exceeds the configured field limit"}
	case errors.Is(err, store.ErrFieldTooLarge):
		return &Error{Message: "ERR hash field or value exceeds the configured size limit"}
	case errors.Is(err, store.ErrInvalidExpiry):
		return &Error{Message: "ERR invalid expire time"}
	}

	var ambiguous *store.AmbiguousAppendError
	if errors.As(err, &ambiguous) {
		return &Error{Message: "MISCONF the write was applied in memory but may not be durable: " + ambiguous.Err.Error()}
	}
	return &Error{Message: "MISCONF persistence failure: " + err.Error()}
}

// sanitiseForMessage escapes a client-supplied command name so that it cannot
// inject control characters into a reply, a log line or a terminal.
func sanitiseForMessage(b []byte) string {
	const max = 32
	truncated := false
	if len(b) > max {
		b = b[:max]
		truncated = true
	}
	var sb strings.Builder
	for _, ch := range b {
		if ch >= 0x20 && ch < 0x7f {
			sb.WriteByte(ch)
		} else {
			fmt.Fprintf(&sb, "\\x%02x", ch)
		}
	}
	if truncated {
		sb.WriteString("...")
	}
	return sb.String()
}

// ---------------------------------------------------------------------------
// Argument helpers
// ---------------------------------------------------------------------------

// argInt parses a base-10 int64 using Redis's rules.
func argInt(name string, raw []byte) (int64, error) {
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, errorf("ERR value is not an integer or out of range")
	}
	_ = name
	return n, nil
}

// argDurationMS converts a TTL argument to milliseconds.
//
// unitScale is 1000 for seconds and 1 for milliseconds. Absolute conversions
// additionally add the current time.
func ttlFromRelative(now int64, value int64, scale int64) (int64, error) {
	if value <= 0 {
		return 0, errorf("ERR invalid expire time")
	}
	if value > (maxInt64-now)/scale {
		return 0, errorf("ERR invalid expire time: the deadline would overflow")
	}
	return now + value*scale, nil
}

// ttlFromAbsolute converts an absolute Unix timestamp argument.
func ttlFromAbsolute(value int64, scale int64) (int64, error) {
	if value <= 0 {
		return 0, errorf("ERR invalid expire time")
	}
	if value > maxInt64/scale {
		return 0, errorf("ERR invalid expire time: the deadline would overflow")
	}
	return value * scale, nil
}

const maxInt64 = int64(^uint64(0) >> 1)

// upperName upper-cases an ASCII argument without allocating a copy when the
// input is already upper case.
func upperName(b []byte) string { return strings.ToUpper(string(b)) }

// lowerName is used for messages that quote a command name.
func lowerName(b []byte) string { return strings.ToLower(string(b)) }

// milliseconds is a convenience for building deadline arithmetic in tests.
func milliseconds(d time.Duration) int64 { return d.Milliseconds() }
