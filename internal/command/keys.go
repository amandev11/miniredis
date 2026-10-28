package command

import (
	"strconv"
	"sync"

	"github.com/amandev11/miniredis/internal/protocol"
)

// defaultScanCount is the COUNT hint used when SCAN is called without one.
const defaultScanCount = 10

// maxScanCount bounds COUNT so a client cannot ask for an unbounded slab.
const maxScanCount = 1 << 20

// registerKeyspaceCommands installs the keyspace commands.
func registerKeyspaceCommands(r *Registry) {
	r.Register(&Spec{
		Name: "TYPE", Arity: 2, Flags: FlagReadOnly | FlagKeyspace,
		Hint: "key", Summary: "Report the value type of a key: string, hash or none.",
		Handler: cmdType,
	})
	r.Register(&Spec{
		Name: "DBSIZE", Arity: 1, Flags: FlagReadOnly | FlagKeyspace,
		Hint: "", Summary: "Count the live keys in the database.",
		Handler: cmdDBSize,
	})
	r.Register(&Spec{
		Name: "KEYS", Arity: 2, Flags: FlagReadOnly | FlagKeyspace,
		Hint: "pattern", Summary: "Return every key matching a glob pattern. Expensive: full keyspace scan.",
		Handler: cmdKeys,
	})
	r.Register(&Spec{
		Name: "SCAN", Arity: -2, Flags: FlagReadOnly | FlagKeyspace,
		Hint:    "cursor [MATCH pattern] [COUNT n]",
		Summary: "Weakly consistent cursor traversal of the keyspace.",
		Handler: cmdScan,
	})
	r.Register(&Spec{
		Name: "FLUSHDB", Arity: 1, Flags: FlagWrite | FlagKeyspace | FlagAdmin,
		Hint: "", Summary: "Remove every key. Development and test convenience.",
		Handler: cmdFlushDB,
	})
	r.Register(&Spec{
		Name: "COMMAND", Arity: -1, Flags: FlagReadOnly | FlagServer,
		Hint: "[COUNT]", Summary: "Introspect the command table.",
		Handler: cmdCommand,
	})
}

func cmdType(c *Context) (protocol.Value, error) {
	kind, ok := c.Store.Kind(string(c.Arg(0)))
	if !ok {
		return protocol.Simple("none"), nil
	}
	return protocol.Simple(kind.String()), nil
}

func cmdDBSize(c *Context) (protocol.Value, error) {
	return protocol.Int(int64(c.Store.Len())), nil
}

func cmdKeys(c *Context) (protocol.Value, error) {
	pattern := c.Arg(0)
	names := c.Store.KeyNames()
	out := make([]protocol.Value, 0, len(names))
	for _, name := range names {
		if globMatch(pattern, []byte(name)) {
			out = append(out, protocol.BulkString(name))
		}
	}
	return protocol.Arr(out...), nil
}

// cmdScan implements SCAN with an explicitly documented cursor model.
//
// The cursor is a zero-based index into the lexicographically sorted list of
// live keys at the time of the call. Traversal is *weakly consistent*: it is
// not a snapshot, and not Redis's reverse-binary cursor. Keys created or
// removed between two calls can be missed or returned twice. A cursor of 0
// means "start" and is also returned to mean "iteration finished".
func cmdScan(c *Context) (protocol.Value, error) {
	cursor, err := strconv.ParseUint(string(c.Arg(0)), 10, 64)
	if err != nil {
		return protocol.Value{}, errorf("ERR invalid cursor")
	}

	var (
		pattern = []byte("*")
		count   = defaultScanCount
	)
	for i := 1; i < c.NumArgs(); i++ {
		switch upperName(c.Arg(i)) {
		case "MATCH":
			if i+1 >= c.NumArgs() {
				return protocol.Value{}, errorf("ERR syntax error")
			}
			pattern = c.Arg(i + 1)
			i++
		case "COUNT":
			if i+1 >= c.NumArgs() {
				return protocol.Value{}, errorf("ERR syntax error")
			}
			n, err := argInt("count", c.Arg(i+1))
			if err != nil || n < 1 {
				return protocol.Value{}, errorf("ERR value is not an integer or out of range")
			}
			if n > maxScanCount {
				n = maxScanCount
			}
			count = int(n)
			i++
		default:
			return protocol.Value{}, errorf("ERR syntax error")
		}
	}

	names := c.Store.KeyNames()
	start := int(cursor)
	if start > len(names) {
		// The keyspace shrank below the cursor; the traversal is over.
		return scanReply(0, nil), nil
	}

	matches := make([]protocol.Value, 0, count)
	next := start
	for next < len(names) && len(matches) < count {
		if globMatch(pattern, []byte(names[next])) {
			matches = append(matches, protocol.BulkString(names[next]))
		}
		next++
	}
	if next >= len(names) {
		next = 0
	}
	return scanReply(next, matches), nil
}

func scanReply(cursor int, keys []protocol.Value) protocol.Value {
	return protocol.Arr(
		protocol.BulkString(strconv.Itoa(cursor)),
		protocol.Arr(keys...),
	)
}

func cmdFlushDB(c *Context) (protocol.Value, error) {
	if err := c.Store.Flush(); err != nil {
		return protocol.Value{}, err
	}
	return protocol.Simple("OK"), nil
}

func cmdCommand(c *Context) (protocol.Value, error) {
	registry := registryOf(c)

	// Bare COMMAND lists the whole table; anything else is a subcommand.
	if c.NumArgs() == 0 {
		specs := registry.Specs()
		out := make([]protocol.Value, 0, len(specs))
		for _, s := range specs {
			out = append(out, commandDescription(s))
		}
		return protocol.Arr(out...), nil
	}

	switch upperName(c.Arg(0)) {
	case "COUNT":
		return protocol.Int(int64(registry.Len())), nil
	case "LIST":
		names := registry.Names()
		out := make([]protocol.Value, 0, len(names))
		for _, n := range names {
			out = append(out, protocol.BulkString(n))
		}
		return protocol.Arr(out...), nil
	case "INFO":
		if c.NumArgs() < 2 {
			return protocol.Value{}, errorf("ERR wrong number of arguments for 'command|info' command")
		}
		out := make([]protocol.Value, 0, c.NumArgs()-1)
		for i := 1; i < c.NumArgs(); i++ {
			spec, ok := registry.Lookup(string(c.Arg(i)))
			if !ok {
				// Redis replies with a null entry for an unknown command.
				out = append(out, protocol.NullBulk())
				continue
			}
			out = append(out, protocol.Arr(
				protocol.BulkString(spec.Name),
				protocol.Int(int64(spec.Arity)),
				protocol.BulkString(spec.Flags.String()),
				protocol.BulkString(spec.Hint),
				protocol.BulkString(spec.Summary),
			))
		}
		return protocol.Arr(out...), nil
	default:
		return protocol.Value{}, errorf("ERR unknown subcommand '%s' for COMMAND", sanitiseForMessage(c.Arg(0)))
	}
}

// commandDescription renders one command table entry: name, arity and flags.
func commandDescription(s *Spec) protocol.Value {
	return protocol.Arr(
		protocol.BulkString(s.Name),
		protocol.Int(int64(s.Arity)),
		protocol.Arr(flagValues(s.Flags)...),
	)
}

func flagValues(f Flags) []protocol.Value {
	var out []protocol.Value
	for _, name := range []string{"write", "readonly", "connection", "keyspace", "string", "hash", "expiry", "server", "admin"} {
		var flag Flags
		switch name {
		case "write":
			flag = FlagWrite
		case "readonly":
			flag = FlagReadOnly
		case "connection":
			flag = FlagConnection
		case "keyspace":
			flag = FlagKeyspace
		case "string":
			flag = FlagString
		case "hash":
			flag = FlagHash
		case "expiry":
			flag = FlagExpiry
		case "server":
			flag = FlagServer
		case "admin":
			flag = FlagAdmin
		}
		if f.Has(flag) {
			out = append(out, protocol.BulkString(name))
		}
	}
	return out
}

// registryOf reaches the registry through the context. Context carries it so
// that unit tests can drive commands without a server.
func registryOf(c *Context) *Registry {
	if c.Registry != nil {
		return c.Registry
	}
	return fallbackRegistry()
}

// fallbackRegistry builds the default command table on first use.
//
// It is lazy on purpose: the command handlers read the registry for
// introspection, so building it eagerly would create a package initialisation
// cycle.
var (
	fallbackOnce sync.Once
	fallbackReg  *Registry
)

func fallbackRegistry() *Registry {
	fallbackOnce.Do(func() { fallbackReg = NewDefaultRegistry() })
	return fallbackReg
}

// ---------------------------------------------------------------------------
// Glob matching
// ---------------------------------------------------------------------------

// globMatch reports whether s matches a Redis-style glob pattern.
//
// Supported syntax: '*' matches any sequence, '?' matches one byte, '[abc]'
// and '[a-z]' match a set (negated with a leading '^'), and '\' escapes the
// next byte. A '[' without a closing ']' is treated as a literal byte, which is
// what Redis does.
//
// The implementation is iterative with a single backtracking point, so a
// pathological pattern such as "*a*a*a*a*b" cannot explode exponentially and
// no allocation is performed.
func globMatch(pattern, s []byte) bool {
	var (
		pi, si         int
		starPi, starSi = -1, 0
	)

	for si < len(s) {
		if pi < len(pattern) {
			switch pattern[pi] {
			case '*':
				starPi, starSi = pi, si
				pi++
				continue
			case '?':
				pi++
				si++
				continue
			case '[':
				if next, hit, ok := matchClass(pattern, pi, s[si]); ok {
					if hit {
						pi = next
						si++
						continue
					}
					// The class did not match; fall through to backtracking.
				} else if pattern[pi] == s[si] {
					// Malformed class: '[' is a literal.
					pi++
					si++
					continue
				}
			case '\\':
				if pi+1 < len(pattern) {
					if pattern[pi+1] == s[si] {
						pi += 2
						si++
						continue
					}
				} else if pattern[pi] == s[si] {
					pi++
					si++
					continue
				}
			default:
				if pattern[pi] == s[si] {
					pi++
					si++
					continue
				}
			}
		}

		if starPi >= 0 {
			// Backtrack: let the last '*' consume one more byte.
			starSi++
			si = starSi
			pi = starPi + 1
			continue
		}
		return false
	}

	// Consume any trailing '*'.
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}

// matchClass evaluates a '[...]' character class at pattern[at] against c.
//
// It returns the index just past the class, whether it matched, and whether the
// class was well formed. A malformed class (no closing bracket) reports
// ok == false so the caller can treat '[' as a literal byte.
func matchClass(pattern []byte, at int, c byte) (next int, matched bool, ok bool) {
	i := at + 1
	if i >= len(pattern) {
		return at, false, false
	}

	negate := false
	if pattern[i] == '^' {
		negate = true
		i++
	}

	hit := false
	first := true
	for i < len(pattern) {
		if pattern[i] == ']' && !first {
			if negate {
				return i + 1, !hit, true
			}
			return i + 1, hit, true
		}
		first = false

		lo := pattern[i]
		if lo == '\\' && i+1 < len(pattern) {
			i++
			lo = pattern[i]
		}

		// A range such as a-z.
		if i+2 < len(pattern) && pattern[i+1] == '-' && pattern[i+2] != ']' {
			hi := pattern[i+2]
			if hi == '\\' && i+3 < len(pattern) {
				hi = pattern[i+3]
				i++
			}
			if lo <= c && c <= hi {
				hit = true
			}
			i += 3
			continue
		}

		if lo == c {
			hit = true
		}
		i++
	}
	return at, false, false
}
