package command

import (
	"github.com/amandev11/miniredis/internal/protocol"
	"github.com/amandev11/miniredis/internal/store"
)

// registerStringCommands installs the string commands.
func registerStringCommands(r *Registry) {
	r.Register(&Spec{
		Name: "SET", Arity: -3, Flags: FlagWrite | FlagString,
		Hint:    "key value [EX seconds | PX milliseconds] [NX | XX] [KEEPTTL]",
		Summary: "Set key to hold a string value, optionally only when the key exists or does not exist.",
		Handler: cmdSet,
	})
	r.Register(&Spec{
		Name: "GET", Arity: 2, Flags: FlagReadOnly | FlagString,
		Hint: "key", Summary: "Get the string value of a key.",
		Handler: cmdGet,
	})
	r.Register(&Spec{
		Name: "MSET", Arity: -3, Flags: FlagWrite | FlagString,
		Hint: "key value [key value ...]", Summary: "Set several keys atomically. Clears their deadlines.",
		Handler: cmdMSet,
	})
	r.Register(&Spec{
		Name: "MGET", Arity: -2, Flags: FlagReadOnly | FlagString,
		Hint: "key [key ...]", Summary: "Get several string values; missing keys return a null bulk string.",
		Handler: cmdMGet,
	})
	r.Register(&Spec{
		Name: "DEL", Arity: -2, Flags: FlagWrite | FlagKeyspace,
		Hint: "key [key ...]", Summary: "Remove keys and return how many were removed.",
		Handler: cmdDel,
	})
	r.Register(&Spec{
		Name: "EXISTS", Arity: -2, Flags: FlagReadOnly | FlagKeyspace,
		Hint: "key [key ...]", Summary: "Count how many of the given keys exist.",
		Handler: cmdExists,
	})
	r.Register(&Spec{
		Name: "INCR", Arity: 2, Flags: FlagWrite | FlagString,
		Hint: "key", Summary: "Add one to the integer value of a key.",
		Handler: cmdIncr,
	})
	r.Register(&Spec{
		Name: "DECR", Arity: 2, Flags: FlagWrite | FlagString,
		Hint: "key", Summary: "Subtract one from the integer value of a key.",
		Handler: cmdDecr,
	})
	r.Register(&Spec{
		Name: "INCRBY", Arity: 3, Flags: FlagWrite | FlagString,
		Hint: "key increment", Summary: "Add an integer to the value of a key.",
		Handler: cmdIncrBy,
	})
	r.Register(&Spec{
		Name: "DECRBY", Arity: 3, Flags: FlagWrite | FlagString,
		Hint: "key decrement", Summary: "Subtract an integer from the value of a key.",
		Handler: cmdDecrBy,
	})
	r.Register(&Spec{
		Name: "APPEND", Arity: 3, Flags: FlagWrite | FlagString,
		Hint: "key value", Summary: "Append to a string value and return the resulting length.",
		Handler: cmdAppend,
	})
	r.Register(&Spec{
		Name: "STRLEN", Arity: 2, Flags: FlagReadOnly | FlagString,
		Hint: "key", Summary: "Return the length of the string value of a key.",
		Handler: cmdStrLen,
	})
}

func cmdSet(c *Context) (protocol.Value, error) {
	key := string(c.Arg(0))
	value := c.Arg(1)

	var (
		opts      store.SetOptions
		hasExpire bool
		deadline  int64
	)

	for i := 2; i < c.NumArgs(); i++ {
		switch upperName(c.Arg(i)) {
		case "EX", "PX":
			if hasExpire || opts.KeepTTL || i+1 >= c.NumArgs() {
				return protocol.Value{}, errorf("ERR syntax error")
			}
			scale := int64(1000)
			if upperName(c.Arg(i)) == "PX" {
				scale = 1
			}
			n, err := argInt("expire", c.Arg(i+1))
			if err != nil || n <= 0 {
				return protocol.Value{}, errorf("ERR invalid expire time in 'set' command")
			}
			// SET requires a strictly positive TTL, unlike EXPIRE.
			at, err := ttlFromRelative(c.now(), n, scale)
			if err != nil {
				return protocol.Value{}, errorf("ERR invalid expire time in 'set' command")
			}
			deadline, hasExpire = at, true
			i++

		case "KEEPTTL":
			if hasExpire || opts.KeepTTL {
				return protocol.Value{}, errorf("ERR syntax error")
			}
			opts.KeepTTL = true

		case "NX":
			if opts.Mode == store.SetXX {
				return protocol.Value{}, errorf("ERR syntax error")
			}
			opts.Mode = store.SetNX

		case "XX":
			if opts.Mode == store.SetNX {
				return protocol.Value{}, errorf("ERR syntax error")
			}
			opts.Mode = store.SetXX

		default:
			return protocol.Value{}, errorf("ERR syntax error")
		}
	}

	if hasExpire {
		opts.ExpireAt = deadline
	}

	applied, err := c.Store.Set(key, value, opts)
	if err != nil {
		return protocol.Value{}, err
	}
	if !applied {
		// NX or XX prevented the write: Redis replies with a null bulk string.
		return protocol.NullBulk(), nil
	}
	return protocol.Simple("OK"), nil
}

func cmdGet(c *Context) (protocol.Value, error) {
	value, ok, err := c.Store.Get(string(c.Arg(0)))
	if err != nil {
		return protocol.Value{}, err
	}
	if !ok {
		return protocol.NullBulk(), nil
	}
	return protocol.Bulk(value), nil
}

func cmdMSet(c *Context) (protocol.Value, error) {
	if c.NumArgs()%2 != 0 {
		return protocol.Value{}, errorf("ERR wrong number of arguments for 'mset' command")
	}
	pairs := make([]store.FieldValue, 0, c.NumArgs()/2)
	for i := 0; i+1 < c.NumArgs(); i += 2 {
		pairs = append(pairs, store.FieldValue{
			Name:  string(c.Arg(i)),
			Value: c.Arg(i + 1),
		})
	}
	if err := c.Store.MSet(pairs); err != nil {
		return protocol.Value{}, err
	}
	return protocol.Simple("OK"), nil
}

func cmdMGet(c *Context) (protocol.Value, error) {
	out := make([]protocol.Value, 0, c.NumArgs())
	for i := 0; i < c.NumArgs(); i++ {
		value, ok, err := c.Store.Get(string(c.Arg(i)))
		if err != nil {
			// Redis aborts the whole MGET on a type error only when one key is
			// involved; for several keys it reports null for the offending one.
			// MiniRedis mirrors Redis here by treating a type mismatch as a
			// null entry.
			if isWrongType(err) {
				out = append(out, protocol.NullBulk())
				continue
			}
			return protocol.Value{}, err
		}
		if !ok {
			out = append(out, protocol.NullBulk())
			continue
		}
		out = append(out, protocol.Bulk(value))
	}
	return protocol.Arr(out...), nil
}

func cmdDel(c *Context) (protocol.Value, error) {
	keys := make([]string, 0, c.NumArgs())
	for i := 0; i < c.NumArgs(); i++ {
		keys = append(keys, string(c.Arg(i)))
	}
	removed, err := c.Store.Delete(keys...)
	if err != nil {
		return protocol.Value{}, err
	}
	return protocol.Int(int64(removed)), nil
}

func cmdExists(c *Context) (protocol.Value, error) {
	keys := make([]string, 0, c.NumArgs())
	for i := 0; i < c.NumArgs(); i++ {
		keys = append(keys, string(c.Arg(i)))
	}
	return protocol.Int(int64(c.Store.Exists(keys...))), nil
}

func cmdIncr(c *Context) (protocol.Value, error) { return increment(c, 1) }

func cmdDecr(c *Context) (protocol.Value, error) { return increment(c, -1) }

func cmdIncrBy(c *Context) (protocol.Value, error) {
	delta, err := argInt("increment", c.Arg(1))
	if err != nil {
		return protocol.Value{}, err
	}
	return increment(c, delta)
}

func cmdDecrBy(c *Context) (protocol.Value, error) {
	delta, err := argInt("decrement", c.Arg(1))
	if err != nil {
		return protocol.Value{}, err
	}
	if delta == minInt64 {
		// Negating this value would overflow; the store would reject the
		// addend anyway, but reporting it here keeps the message precise.
		return protocol.Value{}, errorf("ERR increment or decrement would overflow")
	}
	return increment(c, -delta)
}

func increment(c *Context, delta int64) (protocol.Value, error) {
	value, err := c.Store.Incr(string(c.Arg(0)), delta)
	if err != nil {
		return protocol.Value{}, err
	}
	return protocol.Int(value), nil
}

func cmdAppend(c *Context) (protocol.Value, error) {
	n, err := c.Store.Append(string(c.Arg(0)), c.Arg(1))
	if err != nil {
		return protocol.Value{}, err
	}
	return protocol.Int(int64(n)), nil
}

func cmdStrLen(c *Context) (protocol.Value, error) {
	n, ok, err := c.Store.StrLen(string(c.Arg(0)))
	if err != nil {
		return protocol.Value{}, err
	}
	if !ok {
		return protocol.Int(0), nil
	}
	return protocol.Int(int64(n)), nil
}

const minInt64 = -maxInt64 - 1
