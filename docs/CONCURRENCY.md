# Concurrency and atomicity

This document states what MiniRedis guarantees about concurrent clients, how
those guarantees are implemented, and — just as importantly — what is *not*
claimed. Every guarantee here is covered by a test that runs under `-race`
(`make race`).

## 1. Model

* One process, one keyspace, one `sync.RWMutex` (`store.Store.mu`) protecting it.
* One goroutine per connection. Goroutines never share a parser, a buffer or a
  client-visible value: replies are deep copies.
* Commands are executed **one at a time with respect to each other**. A command
  takes the store lock, completes, and releases it. There is no command that
  performs part of its work under the lock and the rest outside it.

The practical consequence is that MiniRedis is *single-key and
multi-key atomic per command*, but it has no transactions spanning several
commands (`MULTI`/`EXEC` is not implemented). Two commands from the same client
can be interleaved with another client's commands.

## 2. Per-command guarantees

| Command group | Guarantee |
| --- | --- |
| `SET`, `SET ... NX/XX/EX/PX/KEEPTTL` | Atomic with respect to every other command. The condition (`NX`/`XX`) is evaluated under the same lock that performs the write, so `SET k v NX` from two clients cannot both succeed. A reader never observes a partial value. |
| `GET`, `MGET`, `STRLEN`, `TYPE`, `EXISTS`, `TTL`, `PTTL`, `HGET`, `HMGET`, `HGETALL`, `HLEN`, `HEXISTS` | Each command sees the keyspace as of the instant it acquires the lock. Returned values are copies, so a concurrent write cannot change what a client already received. |
| `INCR`, `DECR`, `INCRBY`, `DECRBY` | Read-modify-write under the write lock: no increment is lost, and the arithmetic cannot interleave with another increment on the same key. Overflow is detected and reported (`increment or decrement would overflow`) rather than wrapping. One journal record is written per increment, carrying the resulting value. |
| `APPEND` | Append under the write lock; the journal record carries the resulting value and deadline. |
| `MSET` | Atomic in the sense that every pair is written under one lock acquisition and one journal record describes all of them, so a concurrent reader sees either none of the pairs or all of them. Unlike Redis, `MSET` clears the deadline of every key it writes; that is documented in [PROTOCOL.md](PROTOCOL.md). |
| `HSET`, `HDEL` | One lock acquisition per command, so a multi-field `HSET` cannot be observed half-applied, and `HDEL` that empties a hash deletes the key in the same critical section. |
| `DEL`, `FLUSHDB` | Atomic; the keys are removed and recorded as one mutation each. |
| `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `PERSIST` | The deadline update is atomic with respect to reads and writes of the same key. A key can expire between two commands, exactly as Redis documents; nothing can make an expired key readable. |
| `KEYS`, `SCAN`, `DBSIZE` | `KEYS` and `DBSIZE` take one consistent view. `SCAN` is a weakly consistent cursor traversal: it may miss keys that are created or deleted while it runs, and it never guarantees completeness across concurrent writes (the same guarantee Redis provides). `KEYS` output is sorted, which Redis does not promise. |
| `AUTH`, `PING`, `ECHO`, `QUIT`, `SELECT`, `INFO`, `COMMAND` | Per-connection state (authentication, selected database) is owned by that connection's goroutine and is not shared. `COMMAND` reads an immutable registry. |
| `SHUTDOWN` | Idempotent; the shutdown path itself runs exactly once. |

`INCR` correctness under contention is checked by
`TestConcurrentIncrementsAreNotLost`, which runs four concurrent clients that
issue 500 increments each and asserts the final value is exactly 2000, and by
`BenchmarkINCRSingleKey`, which measures the throughput of that serialised path.

## 3. Lock discipline

Rules the code follows:

1. **One lock order.** The only nesting is `store.mu` (held by the store) →
   `journal.mu` (held by the persistence layer). Nothing in the persistence
   package acquires another MiniRedis lock while holding `journal.mu`, so the
   order cannot be reversed and there is no cycle to deadlock on.
2. **No lock is held across network I/O.** Handlers never write to a socket;
   they return a value and the connection goroutine encodes and flushes it
   after the handler has returned and the lock has been released. A slow or
   stalled client cannot block the keyspace for other clients. This is why the
   write path has a `--write-timeout`: a client that stops reading is
   disconnected rather than allowed to pin a goroutine.
3. **No unbounded goroutines.** One goroutine per connection (bounded by
   `--max-clients`), one active-expiration goroutine, one HTTP server
   goroutine, and one background fsync goroutine when `--appendfsync=everysec`
   is selected. All of them are stopped by `Server.Shutdown`.
4. **The lock is never held while reading a value a caller will use.** Reads
   copy out of the entry (`Entry.Clone`), so no caller can observe or mutate
   stored state through a returned slice or map.

The only blocking call made while holding a lock is the journal append in
`Recorder.Record`, and that is deliberate: see the next section.

## 4. Ordering between memory and persistence

The store calls `Recorder.Record` **before** applying the mutation, with the
write lock held. Two invariants follow:

* **Commit order is journal order.** A mutation visible to a client has already
  been written to the journal file (its durability then depends on
  `--appendfsync`). Replaying the journal reproduces the keyspace exactly,
  including the order of conflicting writes to the same key.
* **A failed append does not silently change memory.** If the append fails
  before any byte can have reached the file, the command returns an error and
  memory is unchanged. If the failure leaves the outcome unknown (a write that
  landed but whose `fsync` failed), the mutation *is* applied and an
  `AmbiguousAppendError` is returned: rejecting the write could contradict what
  a subsequent recovery reads from the file. The error is always surfaced to
  the client and recorded as a journal write error in `INFO persistence`.

`fsync` happens inside the lock in `always` mode. That is what makes the
durability claim true for an acknowledged write, and it is also why `always`
is dramatically slower than `everysec` — the measured difference is in
[BENCHMARKS.md](BENCHMARKS.md). Selecting `always` is a deliberate trade of
throughput for durability, not an accident.

## 5. Durability windows

Concurrency and durability intersect in one place: when a client may consider
a write durable.

| `--appendfsync` | Guarantee |
| --- | --- |
| `always` | The reply is sent only after the record has been written *and* `fsync`ed. An acknowledged write survives a process crash and a machine power loss, subject to the storage device honouring `fsync` and to the file system's behaviour. |
| `everysec` | The reply is sent after the record has been written to the file. A background flusher `fsync`s at most once per second, so a power loss can lose up to about one second of acknowledged writes. A process crash without power loss loses nothing that the operating system has already written. |
| `no` | The reply is sent after the record has been written to the file. Flushing is left to the operating system, so the window is unbounded from MiniRedis's point of view. |

Even in `always` mode, MiniRedis does not claim that a write is on physical
media: it claims that `fsync` returned successfully for that record. That is the
strongest statement a userspace program can make.

## 6. What is deliberately not claimed

* **No distributed guarantees.** There is no replication, no clustering and no
  quorum: MiniRedis is a single-instance engine, and nothing here should be read
  as a consensus or availability claim.
* **No linearizability claim across a crash.** The in-memory order is linear,
  and the journal preserves that order, but a crash can truncate the tail of a
  record that was in flight, and `everysec`/`no` have the durability windows
  above.
* **No multi-command transactions.** `MULTI`, `EXEC`, `WATCH` and scripts are
  not implemented, so there is no way to make several commands atomic together.
  A client that needs that must serialise on its own side.
* **No fairness guarantee.** Whoever acquires the lock next goes next; Go's
  mutexes make no first-come-first-served promise, so a heavily loaded key can
  starve a light one.
* **No lock-free data structures.** The store is a mutex-protected map. The
  sentence "MiniRedis is lock-free" would be false, and nothing in this project
  says it.

## 7. Testing the claims

| Claim | Test |
| --- | --- |
| Concurrent increments are never lost | `tests.TestConcurrentIncrementsAreNotLost` |
| Mixed concurrent workloads keep the keyspace self-consistent | `tests.TestConcurrentMixedWorkloadKeepsInvariants` |
| Many clients at once, with no lost replies | `internal/server.TestManyClientsConcurrently` |
| A stalled or disconnecting client cannot stop the server | `internal/server.TestClientDisconnectDoesNotStopTheServer` |
| Expiration racing with access | `internal/store` expiry tests, `tests.TestExpirationOverTheWire` |
| Persistence under concurrent writes | `internal/persistence` journal tests, `tests.TestJournalReplayRestoresStateAfterRestart` |
| No data races anywhere | `make race` (`go test -race ./...`) |

If a run of `-race` reports a race, that is a bug in this project and must be
fixed rather than documented away.
