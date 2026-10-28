# Protocol

MiniRedis speaks **RESP2**, the Redis serialization protocol, version 2. It
implements the subset that its commands need, and this document lists both the
supported frames and the deliberate incompatibilities. Anything not listed as
supported is not implemented, and the server says so rather than guessing.

## 1. Requests

A client request **must** be a RESP2 array of bulk strings:

```
*3\r\n
$3\r\nSET\r\n
$3\r\nkey\r\n
$5\r\nvalue\r\n
```

* The command name is case-insensitive; handlers receive the upper-cased name.
* Arguments are binary safe: a bulk string may contain NUL, CR, LF and any
  other byte. `TestServerAcceptsBinaryBytes` verifies this over a real socket.
* **Inline commands are not supported.** A request that does not start with
  `*` is answered with a protocol error and the connection is closed:

  ```
  -ERR Protocol error: expected an array of bulk strings, found ...
  ```

  `redis-cli` always sends arrays, so this does not affect it. The
  `mini-cli` client tokenises a command line locally (including quoting and
  `\xHH` escapes) and then sends an array, which is the same approach.

## 2. Replies

| Frame | Example | Used for |
| --- | --- | --- |
| Simple string | `+OK\r\n` | Status replies (`SET`, `AUTH`, `SELECT`, `QUIT`) |
| Error | `-ERR ...\r\n` | Command and protocol errors |
| Integer | `:42\r\n` | Counts and `INCR`/`DECR` results |
| Bulk string | `$5\r\nvalue\r\n` | Values, `INFO`, `ECHO` |
| Null bulk string | `$-1\r\n` | Missing keys, missing hash fields |
| Array | `*2\r\n...` | `MGET`, `KEYS`, `HGETALL`, `COMMAND` |
| Null array | `*-1\r\n` | Reserved; the current commands return an empty array instead |

RESP3 is **not** implemented. There is no `HELLO`, no push messages, no
attribute or map type. A client that sends `HELLO 3` receives an unknown-command
error and can fall back to RESP2, which is what most clients do by default.

## 3. Pipelining and framing

* Pipelining works: the server reads every request already buffered before
  flushing replies, so N pipelined commands cost roughly one write instead of N.
  `internal/server.TestPipelinedRequests` covers this.
* A request may arrive in any number of TCP segments; the parser is streaming
  and does not require a request to fit in one read.
* A **protocol error desynchronises the stream** — the server cannot know where
  the next frame starts — so it replies with the error, closes that connection
  and counts a `protocol_errors` event. Malformed input never stops the server.

## 4. Limits

Every client-controlled length is validated before allocation. Values are the
defaults; each is configurable by flag or `MINIREDIS_*` environment variable
(see the README).

| Limit | Default | Applies to |
| --- | --- | --- |
| `--max-line-bytes` | 64 KiB | One protocol line (lengths, integers, simple strings) |
| `--max-bulk-bytes` | 64 MiB | One bulk string argument |
| `--max-request-bytes` | 64 MiB | Sum of a request's argument bytes |
| `--max-array-len` | 1 048 576 | Arguments in one request |
| `--max-value-bytes` | 64 MiB | A stored string value |
| `--max-key-bytes` | 64 KiB | A key name |
| `--max-hash-fields` | 1 048 576 | Fields in one hash |
| `--max-field-bytes` | 64 MiB | A hash field name or value |

Exceeding a protocol limit produces `-ERR Protocol error: ...` and closes the
connection. Exceeding a data limit produces a normal command error and leaves
the connection open.

## 5. Commands

39 commands are implemented. Every one has tests.

### Connection and server

| Command | Returns |
| --- | --- |
| `PING [message]` | `PONG`, or the message as a bulk string. Allowed before `AUTH`. |
| `ECHO message` | The message. Allowed before `AUTH`. |
| `QUIT` | `OK`, then closes that connection only. |
| `AUTH [username] password` | `OK`, or an error. See below. |
| `SELECT index` | `OK` for index 0; an error for any other index, because MiniRedis has one database. |
| `INFO [section]` | Measured server, client, keyspace, persistence, command and runtime statistics as one bulk string. |
| `SHUTDOWN [NOSAVE\|SAVE]` | `OK`, then the process stops through the graceful-shutdown path. |
| `COMMAND` / `COMMAND COUNT` / `COMMAND LIST` / `COMMAND INFO [name ...]` | Registry introspection: name, arity, flags and a one-line summary. This is what `mini-cli help` prints. |

**AUTH details.** With no password configured, `AUTH` replies
`ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?`
With a password configured, `AUTH <password>` and `AUTH default <password>` are
accepted; any other username is rejected. Failed attempts are counted, and a
connection that keeps failing is closed. The comparison is constant-time.

**SHUTDOWN details.** Unlike Redis, MiniRedis replies `OK` before stopping, so a
client sees confirmation. `NOSAVE` skips the shutdown snapshot; it does not skip
flushing the journal.

### Strings

| Command | Notes |
| --- | --- |
| `SET key value [EX seconds \| PX milliseconds] [NX \| XX] [KEEPTTL]` | Returns `OK`, or a null bulk string when `NX`/`XX` fails (Redis behaviour). `EX`/`PX` must be positive; `KEEPTTL` cannot be combined with `EX`/`PX`. |
| `GET key` | Null bulk string when missing. `WRONGTYPE` if the key holds a hash. |
| `MSET key value [key value ...]` | Atomic; returns `OK`. **Clears the deadline of every key it writes**, which differs from Redis (which also clears them — see the incompatibility list). |
| `MGET key [key ...]` | Missing keys and wrong-type keys both return a null element, as in Redis. |
| `APPEND key value` | Returns the resulting length. Creates the key when missing, preserving any deadline. |
| `STRLEN key` | Returns 0 for a missing key. |
| `INCR`, `DECR`, `INCRBY`, `DECRBY` | Integer arithmetic on the stored value; a missing key starts from 0. Non-integer values give `ERR value is not an integer or out of range`; overflow is reported, never wrapped. |

### Hashes

| Command | Notes |
| --- | --- |
| `HSET key field value [field value ...]` | Returns the number of fields created. Requires an even number of field/value arguments. |
| `HGET key field` | Null bulk string when the field or key is missing. |
| `HMGET key field [field ...]` | One element per requested field; missing fields are null. |
| `HDEL key field [field ...]` | Returns how many fields were removed; deletes the key when the hash becomes empty. |
| `HEXISTS key field` | `1` or `0`. |
| `HLEN key` | Number of fields; `0` for a missing key. |
| `HGETALL key` | Field/value pairs, **sorted by field name**. |

### Expiration

| Command | Notes |
| --- | --- |
| `EXPIRE key seconds`, `PEXPIRE key milliseconds` | Relative deadline. Returns `1` if set, `0` if the key is missing. A non-positive value deletes the key immediately, as in Redis. |
| `EXPIREAT key unix-seconds`, `PEXPIREAT key unix-milliseconds` | Absolute deadline. |
| `TTL key`, `PTTL key` | Remaining lifetime in seconds or milliseconds; `-1` when the key has no deadline, `-2` when the key is missing. |
| `PERSIST key` | Removes the deadline; `1` if it was removed, `0` otherwise. |

Deadlines are stored as absolute Unix milliseconds, so they survive a restart
without restarting the countdown.

### Keyspace

| Command | Notes |
| --- | --- |
| `DEL key [key ...]` | Returns how many keys were removed. |
| `EXISTS key [key ...]` | Counts the arguments that exist (a repeated key counts once per occurrence, as in Redis). |
| `TYPE key` | `string`, `hash` or `none`. |
| `DBSIZE` | Number of live keys. Exact, and expired keys are excluded. |
| `KEYS pattern` | Glob match (`*`, `?`, `[...]`, `\` escape). Full keyspace scan; sorted output. |
| `SCAN cursor [MATCH pattern] [COUNT n]` | Weakly consistent cursor traversal. Each call returns keys and the cursor for the next call; a returned cursor of `0` means the traversal finished. Keys added or removed while a scan is running may or may not appear. |
| `FLUSHDB` | Removes every key. Recorded in the journal like any other mutation. |

## 6. Deliberate incompatibilities with Redis

These are choices, not oversights. Everything else that MiniRedis accepts it
accepts with Redis semantics.

1. **No inline commands.** Requests must be RESP2 arrays of bulk strings.
2. **No RESP3.** No `HELLO`, no push frames, no client-side caching protocol.
3. **No `MULTI`/`EXEC`/`DISCARD`/`WATCH`, no `SUBSCRIBE`, no scripting
   (`EVAL`), no replication (`REPLICAOF`), no clustering, no streams, no
   lists, sets, sorted sets or bitmaps.** Only strings and hashes are
   implemented, and each unimplemented command returns an unknown-command
   error.
4. **One database.** `SELECT 0` succeeds; any other index is an error.
5. **One user.** There are no ACLs; `AUTH <username> <password>` accepts only
   the username `default`.
6. **`KEYS`, `SCAN`, `HGETALL`, `COMMAND LIST` and snapshot output are
   deterministically ordered** (lexicographic by key or field). Redis makes no
   ordering promise for `KEYS`/`HGETALL`; code that relies on MiniRedis order
   is relying on more than Redis guarantees.
7. **`SET` returns `OK`, not the previous value**, and does not implement the
   `GET` option, `EXAT`/`PXAT`, `IDLE` or `EX`/`PX` with `NX` *and* `XX`
   combined in one command.
8. **`SHUTDOWN` replies before stopping** instead of closing the connection
   silently.
9. **`INFO` is not Redis-shaped.** Sections and field names are MiniRedis's own
   (`journal_appends`, `recovery_journal_skipped`, ...). Parsers written for
   Redis `INFO` will not understand it. `INFO commandstats` includes real
   per-command call counts and average microseconds.
10. **`DBSIZE` excludes expired-but-not-yet-collected keys.** It reports the
    logical size of the database, which is what a user means by "how many keys",
    rather than the number of map entries.
11. **No `CONFIG`, `CLIENT`, `DEBUG`, `SAVE`/`BGSAVE`, `LASTSAVE`, `WAIT` or
    `OBJECT`.** Persistence is controlled by configuration at startup, not by
    protocol commands. `SHUTDOWN SAVE` is accepted as a synonym for the
    default behaviour.

## 7. Client compatibility

Anything that speaks RESP2 arrays works: `redis-cli` (for the supported
commands), and the bundled `mini-cli`, which additionally offers `help`
(fetched from the server's own `COMMAND` table), `--raw` output, and a pipeline
friendly stdin mode:

```bash
printf 'SET a 1\nINCR a\nGET a\n' | mini-cli
```

Clients that send a `HELLO` handshake and continue in RESP2 (most do) work
unchanged; clients that require RESP3 do not.
