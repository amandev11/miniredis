package command

import (
	"github.com/amandev11/miniredis/internal/protocol"
	"github.com/amandev11/miniredis/internal/store"
)

// registerHashCommands installs the hash commands.
func registerHashCommands(r *Registry) {
	r.Register(&Spec{
		Name: "HSET", Arity: -4, Flags: FlagWrite | FlagHash,
		Hint: "key field value [field value ...]", Summary: "Set hash fields and return how many were created.",
		Handler: cmdHSet,
	})
	r.Register(&Spec{
		Name: "HGET", Arity: 3, Flags: FlagReadOnly | FlagHash,
		Hint: "key field", Summary: "Get the value of one hash field.",
		Handler: cmdHGet,
	})
	r.Register(&Spec{
		Name: "HMGET", Arity: -3, Flags: FlagReadOnly | FlagHash,
		Hint: "key field [field ...]", Summary: "Get several hash fields; missing fields return null.",
		Handler: cmdHMGet,
	})
	r.Register(&Spec{
		Name: "HDEL", Arity: -3, Flags: FlagWrite | FlagHash,
		Hint: "key field [field ...]", Summary: "Remove hash fields, deleting the key when it becomes empty.",
		Handler: cmdHDel,
	})
	r.Register(&Spec{
		Name: "HEXISTS", Arity: 3, Flags: FlagReadOnly | FlagHash,
		Hint: "key field", Summary: "Report whether a hash field exists.",
		Handler: cmdHExists,
	})
	r.Register(&Spec{
		Name: "HLEN", Arity: 2, Flags: FlagReadOnly | FlagHash,
		Hint: "key", Summary: "Count the fields of a hash.",
		Handler: cmdHLen,
	})
	r.Register(&Spec{
		Name: "HGETALL", Arity: 2, Flags: FlagReadOnly | FlagHash,
		Hint: "key", Summary: "Return every field and value, sorted by field name.",
		Handler: cmdHGetAll,
	})
}

func cmdHSet(c *Context) (protocol.Value, error) {
	// HSET key field value [field value ...]: the arguments after the key must
	// pair up exactly, so the count after the key has to be even.
	if c.NumArgs() < 3 || (c.NumArgs()-1)%2 != 0 {
		return protocol.Value{}, errorf("ERR wrong number of arguments for 'hset' command")
	}
	pairs := make([]store.FieldValue, 0, (c.NumArgs()-1)/2)
	for i := 1; i+1 < c.NumArgs(); i += 2 {
		pairs = append(pairs, store.FieldValue{
			Name:  string(c.Arg(i)),
			Value: c.Arg(i + 1),
		})
	}
	added, err := c.Store.HashSet(string(c.Arg(0)), pairs)
	if err != nil {
		return protocol.Value{}, err
	}
	return protocol.Int(int64(added)), nil
}

func cmdHGet(c *Context) (protocol.Value, error) {
	value, ok, err := c.Store.HashGet(string(c.Arg(0)), string(c.Arg(1)))
	if err != nil {
		return protocol.Value{}, err
	}
	if !ok {
		return protocol.NullBulk(), nil
	}
	return protocol.Bulk(value), nil
}

func cmdHMGet(c *Context) (protocol.Value, error) {
	fields := make([]string, 0, c.NumArgs()-1)
	for i := 1; i < c.NumArgs(); i++ {
		fields = append(fields, string(c.Arg(i)))
	}
	values, err := c.Store.HashGetMany(string(c.Arg(0)), fields)
	if err != nil {
		return protocol.Value{}, err
	}
	out := make([]protocol.Value, len(values))
	for i, v := range values {
		if v == nil {
			out[i] = protocol.NullBulk()
			continue
		}
		out[i] = protocol.Bulk(v)
	}
	return protocol.Arr(out...), nil
}

func cmdHDel(c *Context) (protocol.Value, error) {
	fields := make([]string, 0, c.NumArgs()-1)
	for i := 1; i < c.NumArgs(); i++ {
		fields = append(fields, string(c.Arg(i)))
	}
	removed, err := c.Store.HashDel(string(c.Arg(0)), fields)
	if err != nil {
		return protocol.Value{}, err
	}
	return protocol.Int(int64(removed)), nil
}

func cmdHExists(c *Context) (protocol.Value, error) {
	exists, err := c.Store.HashExists(string(c.Arg(0)), string(c.Arg(1)))
	if err != nil {
		return protocol.Value{}, err
	}
	if exists {
		return protocol.Int(1), nil
	}
	return protocol.Int(0), nil
}

func cmdHLen(c *Context) (protocol.Value, error) {
	n, ok, err := c.Store.HashLen(string(c.Arg(0)))
	if err != nil {
		return protocol.Value{}, err
	}
	if !ok {
		return protocol.Int(0), nil
	}
	return protocol.Int(int64(n)), nil
}

func cmdHGetAll(c *Context) (protocol.Value, error) {
	pairs, ok, err := c.Store.HashGetAll(string(c.Arg(0)))
	if err != nil {
		return protocol.Value{}, err
	}
	if !ok {
		// A missing key produces an empty array, matching Redis.
		return protocol.Arr(), nil
	}
	out := make([]protocol.Value, 0, len(pairs)*2)
	for _, p := range pairs {
		out = append(out, protocol.BulkString(p.Name), protocol.Bulk(p.Value))
	}
	return protocol.Arr(out...), nil
}
