# Testing MiniRedis

MiniRedis is tested at four levels, and the boundaries between them are
deliberate. A test that needs a socket uses a socket; a test that needs a
restart restarts a real server. Nothing reaches into a running server to
pre-populate state, because that would test the test harness instead of the
engine.

| Level | Package | What it can catch |
| --- | --- | --- |
| Parser and encoder | `internal/protocol` | Framing bugs, length validation, allocation bounds |
| Engine | `internal/store` | TTL semantics, type errors, atomicity, replay correctness |
| Command layer | `internal/command` | Arity, option parsing, error text, auth gating |
| Durability | `internal/persistence` | Journal framing, CRC detection, snapshot validation, recovery |
| Socket and lifecycle | `internal/server` | Real TCP behaviour, pipelining, limits, shutdown, HTTP endpoints |
| End to end | `tests` | Two independent clients, concurrent writers, real restarts |

## Running the suite

```bash
make test          # go test -count=1 ./...
make race          # go test -race -count=1 ./...
make cover         # coverage profile + per-function report
make integration   # go test -count=1 -v ./tests
make bench         # Go benchmarks against a real in-process server
make demo          # scripts/demo.sh: end-to-end acceptance run
```

`go test ./...` is enough for day-to-day work. `-race` is mandatory before
merging anything that touches `internal/store`, `internal/persistence` or
`internal/server`, because that is where the lock discipline lives.

To run a single level:

```bash
go test -count=1 ./internal/protocol
go test -count=1 -run TestExpiration ./internal/store
go test -count=1 -v ./tests -run TestSnapshotPlusJournalReplayComposes
```

## Inventory

Counts are `func Test*` symbols, current as of this commit.

| Package | Tests | Notes |
| --- | ---: | --- |
| `internal/protocol` | 26 | plus `FuzzReadCommand` |
| `internal/store` | 50 | includes concurrent read/write, `INCR`, expiry and snapshot races |
| `internal/command` | 42 | registry completeness, arity, options, `INFO` sections, `GLOB` matcher |
| `internal/persistence` | 42 | torn tails, checksum damage, symlinks, snapshot atomicity |
| `internal/config` | 13 | flag/env precedence, validation, password redaction |
| `internal/server` | 24 | real sockets, pipelining, limits, shutdown, HTTP, restarts |
| `tests` (integration) | 12 | multi-client, concurrent writers, journal and snapshot recovery |
| **Total** | **209** | plus 7 benchmarks and 1 fuzz target |

Statement coverage across the module is **68.6 %** (`go tool cover -func`).
That figure is reported, not targeted: the uncovered remainder is mostly
`cmd/` entry points, the benchmark client's reporting code, and defensive
branches for conditions that cannot be produced deterministically from a
test — a failed `rename`, a disk that disappears mid-write.

## What the suites actually assert

**Protocol** (`internal/protocol/protocol_test.go`). Fragmented delivery, one
read per byte, several commands in one read, empty strings, binary payloads
containing `\r\n`, negative and overflowing lengths, bulk strings exactly at
the limit and one byte over, overlong header lines, and nesting depth. Two
tests read the parser's allocation behaviour (`TestDeclaredLengthDoesNotAllocateUpfront`,
`TestDeclaredArrayLengthDoesNotAllocateUpfront`) because "declared 2 GB, sent
nothing" is the memory-exhaustion attack this parser has to survive.

`FuzzReadCommand` runs the parser against arbitrary bytes and asserts only
that it never panics and that every reported length is within limits:

```bash
go test -run=^$ -fuzz=FuzzReadCommand -fuzztime=30s ./internal/protocol
```

**Engine** (`internal/store/store_test.go`). Deadline arithmetic at the exact
boundary (`ExpireAt == now` is expired), `TTL` rounding, `SET` clearing or
keeping expiry depending on flags, `INCR` overflow and canonical-integer
rules, wrong-type errors, `SETNX`/`SETXX` outcome reporting, and value
isolation (a caller mutating a returned slice cannot change stored bytes).
Concurrency tests run real goroutines and are only meaningful under `-race`:

- `TestConcurrentIncrDoesNotLoseUpdates` — no lost updates on one key;
- `TestConcurrentReadWrite` — readers and writers interleaved;
- `TestConcurrentExpirationAndWrites` — sweeper racing a writer;
- `TestConcurrentSnapshotsDuringWrites` — snapshot copy while writes land.

**Command layer** (`internal/command/command_test.go`).
`TestRegistryContainsEveryDocumentedCommand` compares the registry against a
hard-coded list, so adding a command without documenting it fails the build.
`TestEveryCommandIsFullySpecified` rejects an empty help string, a nil handler
or a missing arity. `TestPersistenceFailureIsReportedNotSwallowed` drives a
recorder that returns an error and asserts the client sees an error rather
than a silent `OK`.

**Durability** (`internal/persistence/persistence_test.go`). Every failure
mode is constructed on disk: a flipped byte inside a record body, a record cut
in half, a frame claiming a 4 GB body, a bad magic, an unsupported version, a
symlink standing in for the journal, a snapshot whose checksum does not match,
a leftover `.tmp` file from an interrupted snapshot. Recovery tests assert the
report *and* the resulting keyspace: deleted keys stay deleted, expired keys
do not come back, binary values survive, and `repair=fail` leaves the file
untouched.

**Socket level** (`internal/server/server_test.go`). The harness starts a real
server on `127.0.0.1:0` with a `t.TempDir()` data directory and
`LogLevel: "error"`, then shuts it down with a five-second timeout on cleanup.
Tests cover pipelining, a request delivered one byte at a time, a malformed
request closing only its own connection, oversized requests, the client cap,
repeated auth failures, `QUIT` on one connection, disconnect of one client
while others continue, idempotent shutdown that releases the port, `INFO`
values derived from real counters, and `/healthz`, `/ready`, `/metrics`.

**End to end** (`tests/integration_test.go`). These are the acceptance tests.
Two clients share one keyspace; concurrent `INCR` from many clients is not
lost; a mixed workload keeps `DBSIZE`, `KEYS` and `EXISTS` mutually
consistent; a key with `PX` disappears over the wire; a restart with the
journal restores state; snapshot plus journal composes in sequence order; a
truncated tail is repaired and reported; a corrupt snapshot stops startup
instead of silently starting empty.

## Determinism

Tests must not depend on wall-clock racing:

- ephemeral ports (`127.0.0.1:0`) rather than `6379`, so parallel runs and a
  developer's own server do not collide;
- `t.TempDir()` per test, so no test sees another's journal;
- the store takes a `clock.Clock`, and TTL-sensitive tests use a fake clock
  rather than `time.Sleep` wherever the behaviour allows it;
- where a real sleep is unavoidable (over-the-wire expiry), the wait is
  generous relative to the TTL and the assertion is on the observable state,
  not on elapsed time;
- no test asserts an exact `ops/sec` or latency figure. Benchmarks report
  numbers; tests assert behaviour.

The suite is expected to pass repeatedly. If a test flakes, that is treated as
a bug in the test or the code, not something to retry in CI.

## Continuous integration

`.github/workflows/ci.yml` runs, on every push and pull request:

1. `gofmt -l` (must be empty), `go vet ./...`, `go build ./...`;
2. `go test -count=1 ./...`;
3. `go test -race -count=1 ./...`;
4. a benchmark smoke run (`-benchtime=200ms`) so benchmarks cannot rot;
5. a demo run (`scripts/demo.sh`) that fails on any mismatched reply;
6. a Docker job that builds the image, starts it with a password and a
   volume, checks an authenticated `SET`/`GET` works, checks an
   unauthenticated `GET` is refused, and checks `SIGTERM` stops it.

The Docker job is the only acceptance check that has never run in the
development environment used for this repository (no daemon available there);
it runs in CI, or locally with `make docker-build`.

## What is not tested

Stated so the coverage figure is not mistaken for a guarantee:

- **No crash-injection matrix.** Torn tails and truncated records are
  constructed byte by byte, not produced by `kill -9` at random points during
  a write. The recovery paths are exercised; the probability distribution of
  real power loss is not.
- **No TLS tests**, because TLS is not implemented.
- **No multi-instance tests**, because replication is not implemented.
- **No disk-full or read-only-filesystem tests.** Those branches exist and are
  defensive; they are not covered.
- **No long soak test** in CI. `mini-bench` can run one by hand.
- **No cross-platform assertion.** The code has no OS-specific paths beyond
  file permissions, but CI runs Linux only.
- **Performance is measured, not asserted.** See
  [BENCHMARKS.md](BENCHMARKS.md) for the numbers and their variance.

## Adding a test

1. Put it at the lowest level that can observe the behaviour.
2. Use the existing harness (`startServer`, `testConfig`, the command
   `exec` helpers) instead of building a new one.
3. Assert the observable reply and the resulting state, not internal fields.
4. For anything with a goroutine or a lock, add it to the `-race` path and
   think about what happens when the scheduler runs it in the worst order.
5. If the behaviour is a documented guarantee, reference the doc section in a
   comment so the two stay in sync.
