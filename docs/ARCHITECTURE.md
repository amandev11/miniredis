# MiniRedis architecture

This document explains how MiniRedis is put together: what happens to a request
from the moment a client writes bytes to the socket until a reply is on the
wire, how the two persistence mechanisms interact, and how the process starts
and stops.

It describes the code in this repository, not an aspiration.

```
                        +-----------------------------+
   TCP clients  ------> |  internal/server            |
   (RESP2)              |  accept loop, one goroutine |
                        |  per connection, limits,    |
                        |  timeouts, HTTP diagnostics |
                        +--------------+--------------+
                                       |
                        +--------------v--------------+
                        |  internal/protocol          |
                        |  streaming RESP2 parser     |
                        |  and buffered encoder       |
                        +--------------+--------------+
                                       |
                        +--------------v--------------+
                        |  internal/command           |
                        |  registry: name -> handler, |
                        |  arity, flags, auth gate    |
                        +--------------+--------------+
                                       |
                        +--------------v--------------+
                        |  internal/store            |
                        |  map[string]*Entry behind  |
                        |  one sync.RWMutex, TTLs,   |
                        |  lazy + active expiration  |
                        +--------------+--------------+
                                       |
                     Recorder.Record(mutation) under the write lock
                                       |
                        +--------------v--------------+
                        |  internal/persistence      |
                        |  append-only journal,      |
                        |  versioned snapshots,      |
                        |  startup recovery          |
                        +-----------------------------+
```

## 1. Request lifecycle

1. **Accept.** `Server.acceptLoop` accepts a TCP connection. When the number of
   live connections has reached `--max-clients`, the connection is closed
   immediately and counted in `rejected_connections`; the server keeps serving
   everyone else.
2. **Connection goroutine.** Each accepted connection is served by its own
   goroutine. The goroutine owns a `protocol.Parser`, which owns the read
   buffer, so no parser state is shared between connections.
3. **Parse.** `Parser.ReadCommand` reads exactly one request. A request must be
   a RESP2 array of bulk strings. Every length the client declares is checked
   against the configured limits *before* it is allocated; a bulk string is
   read in bounded chunks rather than in one allocation of the declared size.
4. **Dispatch.** The command name is upper-cased and looked up in the registry
   built by `command.NewDefaultRegistry`. The lookup is case-insensitive, as
   in Redis. An unknown name produces an error reply and the connection stays
   open.
5. **Authorise.** Commands without `FlagNoAuth` require an authenticated
   connection when a password is configured. An unauthenticated attempt is
   answered with `NOAUTH` and counted in `auth_failures`.
6. **Arity check.** The number of arguments must match the declared arity: an
   exact count, or "at least n" for negative arities. A mismatch produces the
   Redis-compatible `wrong number of arguments` error.
7. **Execute.** The handler runs. Handlers never touch the network and never
   block on it; they work against the store and return a `protocol.Value`.
   Write commands go through the store's write path, which records the
   mutation in the journal *before* the change becomes visible (section 5).
8. **Reply.** The reply is encoded by `protocol.Writer`, which buffers the
   frame. The goroutine flushes when the parser has no more buffered input, so
   a pipeline of N requests produces one write instead of N.
9. **Record.** The command name, its duration and whether it failed are
   recorded in `internal/observability`, which is what `INFO` and the HTTP
   `/metrics` endpoint report.

A protocol error is different from a command error: the stream is no longer
aligned to a frame boundary, so the server replies with the error, closes that
connection, and counts a `protocol_errors` event. The process is unaffected,
which `TestMalformedInputIsRejectedWithoutStoppingTheServer` demonstrates.

## 2. Protocol parser

`internal/protocol` is a streaming RESP2 decoder and encoder written from
scratch:

* `ReadCommand` reads one request: `*N\r\n` followed by N bulk strings. It
  rejects inline commands, RESP3 frames, negative counts and lengths that
  exceed the configured limits.
* `ReadValue` reads any supported reply frame, including nested arrays up to
  `MaxDepth`. The CLI and the tests use it; the server uses it to read tests'
  replies, never to read client requests.
* `Writer` encodes replies. Integer framing and bulk framing are written with
  pre-sized scratch buffers, so encoding a reply does not allocate a new slice
  per field.
* `Buffered()` reports how many bytes are already in the parser's read buffer.
  The connection loop uses this to decide whether another request is waiting
  and therefore whether flushing now is worth a syscall.

Limits are enforced in the parser, not in the handlers, so a hostile client
cannot make the server allocate memory proportional to a declared length.

## 3. Data model

The keyspace is `map[string]*Entry` in `internal/store`.

* `Entry` holds exactly one value: a binary-safe byte string (`KindString`) or a
  map of binary-safe field/value pairs (`KindHash`), plus an absolute
  expiration deadline in Unix milliseconds (zero means "no deadline").
* Keys, fields and values are Go strings and `[]byte`, so they may contain NUL,
  CR, LF and any other byte. `TestServerAcceptsBinaryBytes` covers this.
* Commands that would need a different kind fail with the Redis-compatible
  `WRONGTYPE` error rather than converting the value.
* Values handed to a caller are deep copies (`Entry.Clone`), so a reply cannot
  alias stored state and a later write cannot mutate a value a reader already
  returned.
* Sizes are bounded: `--max-key-bytes`, `--max-value-bytes`,
  `--max-hash-fields`, `--max-field-bytes` and the hard ceilings in
  `internal/store` that apply even if the configuration is raised.

## 4. Synchronization

One `sync.RWMutex` protects the whole keyspace. It is the simplest design that
is obviously correct, and it makes each command atomic with respect to every
other command: a command's reads and writes either all happen before another
command or all happen after it.

The consequences, and the parts that are deliberately *not* claimed, are
documented in [CONCURRENCY.md](CONCURRENCY.md). The short version:

* reads take the read lock and copy what they return;
* writes take the write lock, record the mutation, then apply it;
* no handler performs network I/O while holding a lock;
* the only blocking call under the write lock is the journal append, and the
  ordering that requires is explained next.

Sharding the keyspace, or using per-key locks, is a change worth making only
when a measurement shows the single lock is the bottleneck. `make bench`
measures it.

## 5. Expiration

Two mechanisms cooperate, and both use the same absolute deadline stored in the
entry.

**Lazy expiration.** Every read path calls `liveLocked`, which returns nil for a
key whose deadline has passed. An expired key therefore behaves exactly like a
missing key: `GET` returns a null bulk string, `EXISTS` returns 0, `TTL` returns
-2 and `TYPE` returns `none`. Lazy expiration deliberately does *not* delete the
entry: deleting on a read path would require upgrading the read lock to a write
lock. The entry is instead removed by the next write to that key, by `DEL`, or
by the active cycle.

**Active expiration.** `Server.startExpireLoop` runs a bounded cycle every
`--expire-sweep-interval` (default 100 ms). Each cycle calls
`Store.ActiveExpireCycle(--expire-sample-size)`, which examines at most that
many entries using Go's randomised map iteration order and removes the ones
that have expired. The cycle is bounded work, so it can never freeze the
database while scanning a large keyspace; the price is that an expired key may
retain memory until a sample reaches it. That is a memory-retention question,
not a correctness one.

Deadlines are absolute (Unix milliseconds), so a key that is restored from the
journal keeps the deadline it had before the restart instead of restarting its
TTL. `TestExpirationOverTheWire` and
`TestJournalReplayRestoresStateAfterRestart` cover both paths.

Setting `--expire-sweep-interval=0` disables the active cycle and leaves only
lazy expiration, which is a supported configuration.

## 6. Journal writes and commit ordering

Every state-changing command builds a `store.Mutation` and hands it to the
`Recorder` (the persistence `Manager`) **while the store's write lock is held**,
immediately before the in-memory change is applied.

That gives two properties:

* **Journal order equals commit order.** Two writers cannot interleave their
  journal records, because the same lock serialises both the append and the
  in-memory change.
* **No silent divergence.** If the append fails with an error that proves
  nothing reached the file (for example, a full disk that rejected the write
  before any byte landed), the command fails and the in-memory state is *not*
  changed. If the append fails in a way that leaves the outcome unknown (for
  example the write succeeded but the `fsync` failed), the store applies the
  mutation anyway and returns an `AmbiguousAppendError`: the journal may already
  contain the record, and recovery replays every record it can read, so
  applying it keeps memory and disk consistent. Either way the client sees an
  error rather than a silent success.

Mutations describe the *committed state*, not the client's wording:
`SET key value EX 60` is recorded as the value plus the absolute deadline it
produced, and `INCR` is recorded as the resulting value. Replaying such a
journal days later produces exactly the same keyspace. `FLUSHDB` is recorded,
and `DEL` records the keys it removed.

The record format, durability modes and corruption policy are documented in
[PERSISTENCE.md](PERSISTENCE.md).

## 7. Snapshot consistency

A snapshot is a consistent point-in-time view:

1. `Store.Snapshot` takes the write lock and copies the keyspace into a
   `store.Snapshot`, dropping keys that are already expired and recording the
   sequence number of the last mutation included (`Snapshot.Seq`). The lock is
   held only for the copy — never for serialisation — so a large snapshot does
   not block the server while it is encoded.
2. The view is streamed to `miniredis.mrs.tmp` in the data directory. The writer
   computes a CRC32 checksum as it goes, so memory use does not grow with the
   keyspace.
3. The temporary file is `fsync`ed and closed.
4. The file is renamed over `miniredis.mrs`, and the containing directory is
   `fsync`ed so the rename survives a power loss.

Because the rename is atomic, a crash at any point leaves either the previous
snapshot or the new one, never a mixture. The only file the server ever writes
in place is the journal, which is append-only by construction.

Snapshots can be taken while clients keep writing: records appended after
`Snapshot.Seq` are still in the journal, and startup replays exactly those.

## 8. Startup recovery

`server.New` performs recovery before the listener is opened, so a corrupt
persistence file fails startup with a clear error instead of surfacing later.

1. **Load the snapshot**, if `miniredis.mrs` exists. The file is validated in
   two passes: the whole-file checksum first, then the content. A file that
   fails either pass is reported as corrupt and startup stops. A corrupt
   snapshot is never silently treated as an empty database.
2. **Restore its sequence number.** `Snapshot.Seq` is the sequence of the last
   mutation the snapshot contains.
3. **Replay the journal.** Every valid record with a sequence number greater
   than the snapshot's is applied in order. Records already covered by the
   snapshot are counted as skipped rather than re-applied, which is what keeps
   `SET` idempotent and makes the ordering policy observable in
   `INFO persistence`.
4. **Repair a torn tail.** A crash during an append leaves a partial record at
   the end of the file, which is the expected outcome, not corruption. With
   `--journal-repair=truncate` (the default) the partial tail is discarded, the
   file is truncated back to the last valid record boundary, and the number of
   bytes discarded is reported in the recovery report and by `INFO`. With
   `--journal-repair=fail` the server refuses to start and leaves the file
   untouched for inspection. A record that fails validation in the *middle* of
   the file, or a file whose header is not a MiniRedis journal, is an error in
   both modes: recovery never skips arbitrary corruption and then claims that
   everything was recovered.
5. **Report.** `RecoveryReport` records the status (`empty`, `ok`, `repaired`),
   how many records were read, applied, skipped and orphaned, how many bytes
   were discarded, and how long it took. `INFO persistence` exposes all of it,
   and the HTTP readiness endpoint reports a repair as a reason without
   claiming the server is unusable.

## 9. Shutdown sequence

`Server.Shutdown` runs once (it is idempotent) and in this order:

1. Mark the server as closing, so readiness reports `503` and new connections
   are refused.
2. Close the listener and stop the active-expiration cycle.
3. Drain the live connections: in-flight commands finish and their replies are
   written; the connection goroutines then exit. A timeout bounds this step.
4. Stop the HTTP diagnostics endpoint.
5. Close persistence: if `--snapshot-on-shutdown` is set, write a snapshot
   (using the atomic sequence above), flush and close the journal, and stop the
   background `everysec` flusher.
6. Close the store and release the data directory.

`SIGINT` and `SIGTERM` trigger this path, so a container can stop gracefully
within its grace period; `make demo` shows a clean shutdown followed by a
restart that recovers the data. A second signal skips the drain and exits.

`SHUTDOWN` over the protocol replies `OK` and asks the process to stop through
the same path; `SHUTDOWN NOSAVE` skips the shutdown snapshot but still flushes
the journal.
