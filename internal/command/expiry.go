package command

import (
	"errors"

	"github.com/amandev11/miniredis/internal/protocol"
	"github.com/amandev11/miniredis/internal/store"
)

// registerExpiryCommands installs the expiration commands.
func registerExpiryCommands(r *Registry) {
	r.Register(&Spec{
		Name: "EXPIRE", Arity: 3, Flags: FlagWrite | FlagExpiry,
		Hint: "key seconds", Summary: "Set a key's deadline relative to now, in seconds.",
		Handler: expireCommand(1000, false),
	})
	r.Register(&Spec{
		Name: "PEXPIRE", Arity: 3, Flags: FlagWrite | FlagExpiry,
		Hint: "key milliseconds", Summary: "Set a key's deadline relative to now, in milliseconds.",
		Handler: expireCommand(1, false),
	})
	r.Register(&Spec{
		Name: "EXPIREAT", Arity: 3, Flags: FlagWrite | FlagExpiry,
		Hint: "key unix-seconds", Summary: "Set a key's deadline to an absolute Unix time in seconds.",
		Handler: expireCommand(1000, true),
	})
	r.Register(&Spec{
		Name: "PEXPIREAT", Arity: 3, Flags: FlagWrite | FlagExpiry,
		Hint: "key unix-milliseconds", Summary: "Set a key's deadline to an absolute Unix time in milliseconds.",
		Handler: expireCommand(1, true),
	})
	r.Register(&Spec{
		Name: "TTL", Arity: 2, Flags: FlagReadOnly | FlagExpiry,
		Hint: "key", Summary: "Remaining lifetime in seconds: -1 without a deadline, -2 if missing.",
		Handler: cmdTTL,
	})
	r.Register(&Spec{
		Name: "PTTL", Arity: 2, Flags: FlagReadOnly | FlagExpiry,
		Hint: "key", Summary: "Remaining lifetime in milliseconds: -1 without a deadline, -2 if missing.",
		Handler: cmdPTTL,
	})
	r.Register(&Spec{
		Name: "PERSIST", Arity: 2, Flags: FlagWrite | FlagExpiry,
		Hint: "key", Summary: "Remove a key's deadline, making it persistent.",
		Handler: cmdPersist,
	})
}

// expireCommand builds a handler for EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT.
//
// scale converts the argument to milliseconds (1000 for seconds, 1 for
// milliseconds). absolute selects whether the argument is a Unix timestamp
// instead of an offset from now.
//
// Semantics follow Redis: the argument may be zero or negative, in which case
// the key is removed immediately and the command still reports success. A
// missing key reports failure.
func expireCommand(scale int64, absolute bool) Handler {
	return func(c *Context) (protocol.Value, error) {
		key := string(c.Arg(0))
		raw, err := argInt("expire", c.Arg(1))
		if err != nil {
			return protocol.Value{}, err
		}

		var deadline int64
		if absolute {
			if raw > maxInt64/scale || raw < minInt64/scale {
				return protocol.Value{}, errorf("ERR invalid expire time: the deadline would overflow")
			}
			deadline = raw * scale
		} else {
			if raw > maxInt64/scale || raw < minInt64/scale {
				return protocol.Value{}, errorf("ERR invalid expire time: the deadline would overflow")
			}
			delta := raw * scale
			now := c.now()
			if (delta > 0 && now > maxInt64-delta) || (delta < 0 && now < minInt64-delta) {
				return protocol.Value{}, errorf("ERR invalid expire time: the deadline would overflow")
			}
			deadline = now + delta
		}

		// Zero is the internal encoding for "no deadline", so a requested
		// deadline at or before the epoch is clamped to a timestamp that is
		// unambiguously in the past.
		if deadline <= 0 {
			deadline = 1
		}

		exists, err := c.Store.SetExpiry(key, deadline)
		if err != nil {
			return protocol.Value{}, err
		}
		if !exists {
			return protocol.Int(0), nil
		}
		return protocol.Int(1), nil
	}
}

func cmdTTL(c *Context) (protocol.Value, error) { return ttlReply(c, 1000) }

func cmdPTTL(c *Context) (protocol.Value, error) { return ttlReply(c, 1) }

// ttlReply implements the documented TTL contract:
//
//	-2  the key does not exist (or has already expired)
//	-1  the key exists and has no deadline
//	 n  the remaining lifetime, in the requested unit
//
// A key is never reported with a positive remaining lifetime once its deadline
// has passed, because the store treats expired keys as absent.
func ttlReply(c *Context, unit int64) (protocol.Value, error) {
	remaining, state := c.Store.TTL(string(c.Arg(0)))
	switch state {
	case store.TTLMissing:
		return protocol.Int(-2), nil
	case store.TTLNoExpiry:
		return protocol.Int(-1), nil
	default:
		if unit == 1 {
			return protocol.Int(remaining), nil
		}
		// Seconds are rounded to nearest so that TTL and PTTL agree for short
		// lifetimes, matching Redis.
		return protocol.Int((remaining + unit/2) / unit), nil
	}
}

func cmdPersist(c *Context) (protocol.Value, error) {
	key := string(c.Arg(0))
	deadline, ok := c.Store.ExpireAt(key)
	if !ok || deadline == 0 {
		return protocol.Int(0), nil
	}
	if _, err := c.Store.SetExpiry(key, 0); err != nil {
		return protocol.Value{}, err
	}
	return protocol.Int(1), nil
}

// isWrongType reports whether err is the store's type-mismatch error.
func isWrongType(err error) bool { return errors.Is(err, store.ErrWrongType) }
