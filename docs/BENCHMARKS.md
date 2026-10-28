# Benchmarks

Every number on this page was measured on the machine described below, with the
commands shown, and pasted from the tools' own output. Nothing is estimated, no
figure was produced by a different build, and no figure is asserted as a target
or a guarantee.

Measured environment:

| | |
| --- | --- |
| Go | `go1.27.3 linux/amd64` |
| CPU | 8 vCPU cloud VM, Linux 6.1.102 |
| Storage | the VM's virtual disk (this matters for `appendfsync=always`) |
| Server | `bin/miniredis` built with `-trimpath`, default flags except where stated |
| Client | `bin/mini-bench` on the same host, over loopback TCP |

Loopback benchmarks measure the whole path — TCP stack, RESP parser, dispatch,
store, journal — but they do not include a real network. Numbers from another
machine will differ; the point of this page is that the procedure is
reproducible and the results are real.

## 1. How to reproduce

```bash
make build

# Go benchmarks: these start their own server on an ephemeral port.
go test -run '^$' -bench . -benchtime=1s -benchmem ./internal/server

# Load generator against a running server.
./bin/miniredis --addr 127.0.0.1:6379 --data-dir /tmp/mr-bench \
  --http-addr "" --appendonly --appendfsync everysec &
./bin/mini-bench --addr 127.0.0.1:6379 -test SET   -n 50000 -c 25 -p 16 -keys 10000 -d 64
./bin/mini-bench --addr 127.0.0.1:6379 -test GET   -n 50000 -c 25 -p 16 -keys 10000 -d 64
./bin/mini-bench --addr 127.0.0.1:6379 -test INCR  -n 50000 -c 25 -p 16 -keys 10000 -d 64
./bin/mini-bench --addr 127.0.0.1:6379 -test HSET  -n 50000 -c 25 -p 16 -keys 10000 -d 64
./bin/mini-bench --addr 127.0.0.1:6379 -test MIXED -n 50000 -c 25 -p 16 -keys 10000 -d 64
```

`mini-bench` flags: `-addr`, `-a` (password), `-c` connections, `-n` total
requests, `-p` pipeline depth, `-d` value size in bytes, `-keys` key-space size,
`-test SET|GET|INCR|HSET|MIXED`, `-timeout`.

## 2. What the numbers mean

* **Throughput** is completed requests divided by wall time, measured by the
  client from the first write to the last reply.
* **Latency is per pipeline batch**, not per request, because a pipelined client
  cannot attribute a completion to one command. Each batch's wall time is
  recorded, and the report also prints the batch mean divided by the pipeline
  depth as an estimate of per-request service time. That estimate is labelled in
  the output; the percentiles are the raw batch measurements.
* **Latency percentiles use nearest-rank**, the conservative definition for tail
  latency, and each run prints the sample count it used.
* Benchmarks measure a **closed-loop** client: each connection waits for its
  replies before sending the next batch. Latency under saturation (coordinated
  omission) is therefore not captured, and no claim is made about it.
* Workloads use distinct key prefixes (`bench:SET:*`, `bench:GET:*`, ...), so
  running one after another in the same server is safe; the numbers below were
  taken exactly as the commands above show, on one server, in one session.

## 3. Go benchmarks (server over loopback TCP)

`go test -run '^$' -bench . -benchtime=1s -benchmem ./internal/server`

| Benchmark | Iterations | ns/op | Allocations |
| --- | ---: | ---: | --- |
| `BenchmarkSETRoundTrip` | 88 713 | 12 923 | 748 B, 24 allocs |
| `BenchmarkGETRoundTrip` | 179 044 | 6 455 | 416 B, 17 allocs |
| `BenchmarkHSETRoundTrip` | 87 418 | 13 622 | 1 004 B, 28 allocs |
| `BenchmarkINCRSpreadParallel` (many keys) | 98 089 | 12 434 | 576 B, 19 allocs |
| `BenchmarkINCRSingleKey` (every client on one key) | 91 840 | 12 183 | 558 B, 20 allocs |
| `BenchmarkPipelinedSET` (16 commands per op) | 8 883 | 133 805 | 10 928 B, 321 allocs, **16.00 commands/op**, 3.35 MB/s |
| `BenchmarkPipelinedGET` (16 commands per op) | 112 428 | 10 928 | 6 180 B, 225 allocs, **16.00 commands/op** |

Read `ns/op` divided by `commands/op` to compare pipelined and round-trip
benchmarks: pipelined GET is 10 928 ns for 16 commands (≈ 683 ns per command)
versus 6 455 ns for one round trip, i.e. pipelining is worth roughly 9× on that
workload. The `BenchmarkPipelined*` benchmarks run in parallel across
`GOMAXPROCS` connections, so their figures reflect aggregate throughput, not a
single connection's.

The two INCR benchmarks are the interesting comparison: spreading increments
across many keys and hammering a single key produce the same latency within
noise (12 434 vs 12 183 ns/op). At this scale the single keyspace lock is **not**
the bottleneck — the network round trip is. That is the measurement that decides
whether sharding the keyspace would help, and it says it would not.

## 4. Load generator, per workload

Command shape: `./bin/mini-bench -test <T> -n 50000 -c 25 -p 16 -keys 10000 -d 64
appendfsync=everysec`, journal enabled. All runs reported **0 reply errors**.

| Workload | Throughput (ops/sec) | Wall time | Batch mean | p50 | p99 | p99.9 | Service time/request |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `SET` (64-byte values) | 101 850 | 490.9 ms | 3.750 ms | 3.110 ms | 13.143 ms | 19.645 ms | 0.234 ms |
| `GET` | 970 737 | 51.5 ms | 0.359 ms | 0.317 ms | 1.483 ms | 2.604 ms | 0.022 ms |
| `INCR` | 110 742 | 451.5 ms | 3.433 ms | 2.613 ms | 14.765 ms | 27.351 ms | 0.215 ms |
| `HSET` | 99 450 | 502.8 ms | 3.845 ms | 3.120 ms | 13.204 ms | 18.564 ms | 0.240 ms |
| `MIXED` (GET/SET alternating) | 133 312 | 375.1 ms | 2.870 ms | 2.188 ms | 10.330 ms | 26.427 ms | 0.179 ms |

Observations, each of which the numbers support:

* **Reads are an order of magnitude faster than writes** (970 k vs 102 k
  ops/sec). Writes pay for a journal record per mutation; `GET` never touches
  the journal.
* **`MIXED` lands between the two**, as expected for an even split, and its
  lower p50 reflects the cheaper half of its batches.
* **Tail latency on write workloads is around 13–20 ms at p99**, which is the
  `everysec` flusher and the operating system's writeback appearing in the
  pipeline, not a lock convoy: 25 connections over 8 vCPUs produce these tails
  with the *same* profile for `SET`, `HSET` and `INCR`.
* Run-to-run variance on this shared VM is roughly 5–10 %. An earlier run of the
  same `SET` workload measured 108 181 ops/sec; treat differences of that size
  as noise, not regressions.

## 5. Durability mode comparison

Each mode was measured on a **fresh server** with `-test SET -n 30000 -c 25
-p 16 -keys 5000`, 0 reply errors in every run.

| `--appendfsync` | Throughput (ops/sec) | Batch mean | p99 | Estimated service time per request |
| --- | ---: | ---: | ---: | ---: |
| `always` | 2 408 | 166.005 ms | 874.068 ms | 10.375 ms |
| `everysec` | 100 523 | 3.777 ms | 12.863 ms | 0.236 ms |
| `no` | 106 367 | 3.529 ms | 13.015 ms | 0.221 ms |

This is the clearest cost/benefit table in the project:

* `always` is **~42× slower than `everysec`** here because every acknowledged
  write waits for an `fsync` on this VM's virtual disk. That is the price of
  "an acknowledged write survives a power loss", and it is why `everysec` is the
  default.
* `everysec` and `no` differ by about 6 %, which is within run-to-run variance:
  on this machine the per-write cost is the file write, and the background
  fsync is not the bottleneck. That is a property of this disk, not a general
  law — on a slower device the gap would be larger.

## 6. What is deliberately not measured

* **No comparison against Redis.** Running both engines to produce a
  "faster than X %" headline would need a controlled environment, identical
  workloads and a discussion of the many features MiniRedis does not have. This
  page reports MiniRedis's own numbers instead of implying a contest.
* **No multi-GB keyspace snapshot timings.** Snapshot cost scales with data
  size; the correctness of the atomic replace is tested, but no large-scale
  timing was recorded, so none is published.
* **No latency-under-saturation figures.** The client is closed-loop (see
  section 2), so p99 under overload is not something these runs can measure.
* **No memory measurements beyond what `INFO` and `/metrics` report.** The
  keyspace gauge is an approximation of payload bytes and says so; the process
  heap figures come from the Go runtime and are reported as-is.
* **No restart-time measurements for large journals**, because journal
  compaction is not implemented
  ([PERSISTENCE.md §8](PERSISTENCE.md)) and a large journal is exactly the case
  where restart time would matter most.

## 7. Reproducing the correctness results

Performance is only meaningful if the engine is correct, so the same run that
produced these figures also produced:

```bash
go test -count=1 ./...        # all packages pass
go test -race -count=1 ./...  # no data races reported
go vet ./...                  # clean
gofmt -l .                    # empty
./scripts/demo.sh             # two clients, expiration, restart recovery, all checks pass
```

The demo is the reproducibility check a reader is most likely to run: it starts
a real server with `--appendfsync always`, writes with one client, reads with a
second process, waits for a TTL, stops the server, restarts it, and verifies
that the data is back — printing every command and reply.
