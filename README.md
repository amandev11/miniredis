# MiniRedis

[![ci](https://github.com/amandev11/miniredis/actions/workflows/ci.yml/badge.svg)](https://github.com/amandev11/miniredis/actions/workflows/ci.yml)

A Redis-inspired, in-memory key-value database engine written from scratch in
Go. It speaks RESP2 over TCP, stores binary-safe strings and hashes, expires
keys with lazy and active expiration, and persists changes with its own
append-only journal and versioned snapshots — all implemented in this
repository, with no database library and no third-party dependencies.

MiniRedis is an **educational systems project**. It is not a Redis replacement
and it is not production ready; the [limitations](#known-limitations) section
says exactly where the edges are.

```
$ make build && ./bin/miniredis &
$ ./bin/mini-cli SET greeting "hello"
OK
$ ./bin/mini-cli GET greeting
"hello"
$ ./bin/mini-cli HSET user:1 name aman role engineer
(integer) 2
$ ./bin/mini-cli --raw INFO persistence | head -5
```

## Why this project exists

The goal was to build the parts of a database that are usually hidden behind a
library: a streaming protocol parser, a command dispatcher, an in-memory data
model with real concurrency, an expiration engine, a crash-safe persistence
format, and startup recovery — each one written, tested and measured here
rather than imported. The result is a small system that can be read end to end
in an afternoon, plus the engineering documents that explain the trade-offs
that were actually made.

## Features

* **Protocol**: a streaming RESP2 parser and buffered encoder, with hard limits
  on every client-controlled length, pipelining, and deterministic behaviour on
  malformed input (error reply, connection closed, server unaffected).
* **Data model**: binary-safe strings and hashes behind one `sync.RWMutex`;
  values are copied out, so no reply aliases stored state.
* **39 commands** covering strings, hashes, expiry, keyspace and server
  introspection. Every command has tests.
* **Expiration**: lazy expiration on every access path plus a bounded active
  cycle with configurable interval and sample size. Deadlines are absolute, so
  they survive a restart.
* **Persistence**: an append-only journal with sequence numbers, CRC32C
  checksums, a documented binary record format and three fsync modes, plus
  versioned snapshots written with the atomic temp-file-and-rename sequence.
* **Recovery**: snapshot load → journal replay of newer records only → torn-tail
  repair with a reported byte count → explicit failure on real corruption. A
  damaged snapshot stops startup instead of silently producing an empty
  database.
* **Security**: loopback binding by default, an explicit refusal to serve a
  public listener without a password, constant-time password comparison,
  `0700`/`0600` file modes, symlink refusal, and no stored values in logs.
* **Operations**: `INFO` with measured statistics, an HTTP `/healthz`, `/ready`
  and `/metrics` endpoint that can never return stored data, graceful shutdown
  with connection draining, structured logs, and a Dockerfile that runs as a
  non-root user.
* **Tooling**: a standalone `mini-cli` client, a `mini-bench` load generator
  that reports real throughput and latency percentiles, Go benchmarks, and an
  end-to-end demo script.

## Architecture

```
 TCP clients (RESP2)
        |
        v
 internal/server      accept loop, one goroutine per connection, limits,
                      timeouts, HTTP diagnostics, lifecycle
        |
        v
 internal/protocol    streaming RESP2 parser, buffered encoder
        |
        v
 internal/command     registry: name -> handler, arity, flags, auth gate,
                      39 commands
        |
        v
 internal/store       map[string]*Entry behind one sync.RWMutex, absolute
                      TTLs, lazy + active expiration, mutation objects
        |
        v   Recorder.Record(mutation)  (called under the write lock,
        |                              before the in-memory change)
 internal/persistence append-only journal, versioned snapshots, recovery
```

[ARCHITECTURE.md](docs/ARCHITECTURE.md) walks through a request from socket to
reply, the storage model, snapshot consistency and the shutdown sequence.

## Requirements

* **Go 1.24 or newer** (`go.mod` records the minimum; the project has no
  third-party dependencies, so `go build` needs no network access).
* Optional: Docker and Docker Compose for the container workflow.
* Optional: `redis-cli` for cross-checking protocol compatibility.

If you are on a machine without a Go toolchain and without root access,
`scripts/install-go.sh` downloads a toolchain into `$HOME/tools` and prints how
to put it on `PATH`.

## Installation

```bash
git clone https://github.com/amandev11/miniredis.git
cd miniredis
go build ./...          # or: make build
```

## Build and run

```bash
make build              # bin/miniredis, bin/mini-cli, bin/mini-bench
make run                # go run ./cmd/miniredis  (defaults: 127.0.0.1:6379)

./bin/miniredis \
  --addr 127.0.0.1:6379 \
  --data-dir ./data \
  --appendonly --appendfsync everysec \
  --requirepass "$MINIREDIS_PASSWORD"
```

Useful flags: `--help` prints the full list, and every flag has an
`MINIREDIS_*` environment equivalent (for example `MINIREDIS_ADDR`,
`MINIREDIS_REQUIREPASS`), which is what makes the container configurable.

## Client examples

```bash
# One command at a time
./bin/mini-cli PING
./bin/mini-cli SET greeting hello
./bin/mini-cli GET greeting
./bin/mini-cli -a "$MINIREDIS_PASSWORD" INFO server

# Point it anywhere
./bin/mini-cli --addr 10.0.0.5:6379 -a secret GET greeting

# Interactive session (prompt appears only on a terminal)
./bin/mini-cli

# Script mode: one command per line from stdin
printf 'SET a 1\nINCR a\nGET a\n' | ./bin/mini-cli

# The command table, fetched from the server itself
./bin/mini-cli help
```

`mini-cli` is an ordinary TCP client: it imports only `internal/protocol` and
never the storage engine.

## Supported commands

| Group | Commands |
| --- | --- |
| Connection | `PING` `ECHO` `QUIT` `AUTH` `SELECT` |
| Server | `INFO` `SHUTDOWN` `COMMAND` |
| Strings | `SET` `GET` `MSET` `MGET` `APPEND` `STRLEN` `INCR` `DECR` `INCRBY` `DECRBY` |
| Hashes | `HSET` `HGET` `HMGET` `HDEL` `HEXISTS` `HLEN` `HGETALL` |
| Expiration | `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `TTL` `PTTL` `PERSIST` |
| Keyspace | `DEL` `EXISTS` `TYPE` `DBSIZE` `KEYS` `SCAN` `FLUSHDB` |

`SET` supports `EX`, `PX`, `NX`, `XX` and `KEEPTTL`. Per-command semantics and
the deliberate differences from Redis are documented in
[PROTOCOL.md](docs/PROTOCOL.md); the notable ones are that requests must be
RESP2 arrays (no inline commands), there is no RESP3, no
`MULTI`/`EXEC`/scripting/replication, one database and one user, and `INFO` uses
MiniRedis's own field names.

## Protocol compatibility

* RESP2 only: simple strings, errors, integers, bulk strings, arrays, null bulk
  strings. No RESP3, no push frames.
* Requests must be arrays of bulk strings. `redis-cli` works for the supported
  commands because it always sends arrays; `mini-cli` tokenises command lines
  locally and then sends arrays.
* Pipelining is supported and is the reason throughput scales the way
  [BENCHMARKS.md](docs/BENCHMARKS.md) measures.
* A protocol error closes that connection; command errors leave it open.

## Persistence configuration

| Flag | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `--appendonly` | `MINIREDIS_APPENDONLY` | `true` | Record every mutation in the journal |
| `--appendfsync` | `MINIREDIS_APPENDFSYNC` | `everysec` | `always`, `everysec` or `no` |
| `--journal-repair` | `MINIREDIS_JOURNAL_REPAIR` | `truncate` | Damaged tail: `truncate` or `fail` |
| `--snapshot-interval` | `MINIREDIS_SNAPSHOT_INTERVAL` | `0` (off) | Periodic snapshot period |
| `--snapshot-on-shutdown` | `MINIREDIS_SNAPSHOT_ON_SHUTDOWN` | `true` | Snapshot on a clean shutdown |
| `--data-dir` | `MINIREDIS_DATA_DIR` | `./data` | Where the journal and snapshots live |

Durability, in one line each: `always` fsyncs every acknowledged write;
`everysec` can lose about a second of acknowledged writes after a power loss;
`no` leaves flushing to the operating system. The formats, the corruption
policy and the crash-recovery guarantees are specified in
[PERSISTENCE.md](docs/PERSISTENCE.md).

## Docker

```bash
# Compose (recommended): a required password and a persistent volume
cp .env.example .env      # set MINIREDIS_PASSWORD
docker compose up --build
docker compose exec miniredis mini-cli --addr 127.0.0.1:6379 \
  -a "$MINIREDIS_PASSWORD" SET greeting hello

# Or directly
make docker-build
docker run --rm -p 127.0.0.1:6379:6379 -p 127.0.0.1:7379:7379 \
  -e MINIREDIS_REQUIREPASS=change-me \
  -v miniredis-data:/data miniredis:local

# Client in a second container
docker run --rm --network container:miniredis miniredis:local \
  mini-cli --addr 127.0.0.1:6379 -a change-me PING
```

The image builds static binaries in a `golang` stage and runs them as an
unprivileged user with `/data` as a volume. `SIGTERM` is handled: the server
stops accepting, drains connections, writes a snapshot and exits.

## Testing

```bash
make test          # go test ./...
make race          # go test -race ./...   (the concurrency claims are checked here)
make vet           # go vet ./...
make fmt-check     # gofmt -l . must be empty
make integration   # go test -race ./tests/...
make check         # fmt-check + vet + build + test + race + integration
```

The suite covers the protocol parser (including fuzz targets), the store and
expiration engine, the command layer, the server over real sockets, persistence
and recovery with real files in real temporary directories, and end-to-end
scenarios that restart a real server and read the data back through a second
client.

## Benchmarking

```bash
make bench         # Go benchmarks over loopback TCP; they start their own server
make bench-cli     # the standalone load generator against 127.0.0.1:6379

# Or with explicit parameters
./bin/mini-bench --addr 127.0.0.1:6379 -a "$MINIREDIS_PASSWORD" \
  -test SET -n 200000 -c 50 -p 16 -keys 10000 -d 64
```

`mini-bench` reports what it measured: completed requests, reply errors, wall
time, throughput and per-batch latency percentiles, always labelled with the
pipeline depth and the environment. Procedures, the exact commands and the
measurements taken on the development machine are in
[BENCHMARKS.md](docs/BENCHMARKS.md). No number in this repository is estimated.

## Demonstration

`make demo` runs the end-to-end script. It starts a server with
`--appendfsync always`, writes with one client, reads with a second process,
shows atomic increments, stores a hash, watches a TTL expire, queries the HTTP
endpoints, shuts the server down cleanly, restarts it, and shows that the data
came back — printing every command and every reply, and failing if any check
does not match.

## Project layout

```
cmd/miniredis      server executable
cmd/mini-cli       standalone client
cmd/mini-bench     load generator and latency reporter
internal/protocol  RESP2 parser, encoder, limits
internal/store     keyspace, TTL metadata, expiration, mutation objects
internal/persistence  journal, snapshots, recovery
internal/command   command registry and the 39 handlers
internal/server    listener, connection lifecycle, HTTP diagnostics, shutdown
internal/config    flags and environment parsing, validation
internal/observability  counters reported by INFO and /metrics
tests              end-to-end tests that restart real servers
scripts            demo.sh, install-go.sh
docs               architecture, protocol, persistence, concurrency, security, testing, benchmarks
```

## Documentation

| Document | Contents |
| --- | --- |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Request lifecycle, data model, synchronization, expiration, journal ordering, snapshot consistency, startup recovery, shutdown |
| [PROTOCOL.md](docs/PROTOCOL.md) | Supported RESP2 frames, limits, every command, deliberate incompatibilities |
| [PERSISTENCE.md](docs/PERSISTENCE.md) | Journal and snapshot byte formats, durability modes, corruption policy, recovery guarantees |
| [CONCURRENCY.md](docs/CONCURRENCY.md) | Locking model, per-command atomicity, what is not claimed |
| [SECURITY.md](docs/SECURITY.md) | Binding, authentication, data handling, filesystem protections, limitations |
| [TESTING.md](docs/TESTING.md) | Test inventory and levels, what each suite asserts, determinism policy, what is not covered |
| [BENCHMARKS.md](docs/BENCHMARKS.md) | How to reproduce the measurements, and the results |

## Known limitations

* **Not production ready.** MiniRedis is an educational engine. Do not put data
  in it that you cannot lose.
* **No TLS.** Authentication is a plaintext password on an unencrypted TCP
  connection. Use a private network or a TLS-terminating proxy.
* **Single instance.** No replication, clustering, failover or persistence
  replication. One process, one keyspace, one disk.
* **No transactions or scripting.** No `MULTI`/`EXEC`, no `WATCH`, no `EVAL`.
  Each command is atomic on its own; groups of commands are not.
* **Two data types.** Strings and hashes only. No lists, sets, sorted sets,
  streams, bitmaps, geospatial indexes or HyperLogLog.
* **No journal compaction.** Snapshots shorten recovery time, but the journal
  grows until it is removed. See
  [PERSISTENCE.md §8](docs/PERSISTENCE.md).
* **No multi-user access control.** A single shared password, or none, and no
  per-key permissions.
* **In-memory only.** The whole keyspace must fit in RAM; there is no eviction
  policy, so a full process fails writes rather than evicting keys.
* **One database.** `SELECT 0` only.
* **`--appendfsync always` is slow** — it fsyncs every write, which on a slow
  disk means a few thousand writes per second instead of a hundred thousand or
  more. That is the price of the durability guarantee, and
  [BENCHMARKS.md](docs/BENCHMARKS.md) shows the measured difference.
* **No online configuration changes.** Everything is set at startup; there is
  no `CONFIG SET`, and persistence cannot be triggered over the protocol.

## License

MIT — see [LICENSE](LICENSE).

Redis is a trademark of Redis Ltd. This project is not affiliated with or
endorsed by Redis Ltd., and it is not a Redis distribution.
