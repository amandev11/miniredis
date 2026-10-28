// Command mini-bench is MiniRedis's load generator and latency reporter.
//
// It is an ordinary RESP client: it opens N TCP connections, pipelines
// commands, reads every reply, and reports only measurements it actually took.
// Latency is attributed per pipeline batch and is also printed divided by the
// pipeline depth, so the figure cannot be mistaken for a bare round-trip time.
//
// Usage:
//
//	mini-bench -c 50 -n 200000 -p 16 -test SET
//	mini-bench -addr 127.0.0.1:6379 -test GET -keys 10000 -d 64
//	mini-bench -a "$MINIREDIS_PASSWORD" -test MIXED -c 8
//
// The tool never invents numbers: if the server replies with an error or the
// connection fails, the failure is reported and the run exits non-zero.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amandev11/miniredis/internal/protocol"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

type options struct {
	addr     string
	password string
	conns    int
	requests int
	pipeline int
	payload  int
	keys     int
	test     string
	timeout  time.Duration
}

func parse(args []string, stderr io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet("mini-bench", flag.ContinueOnError)
	fs.SetOutput(stderr)

	fs.StringVar(&o.addr, "addr", "127.0.0.1:6379", "server address")
	fs.StringVar(&o.password, "a", "", "password for AUTH (never logged)")
	fs.StringVar(&o.password, "password", "", "password for AUTH (never logged)")
	fs.IntVar(&o.conns, "c", 50, "concurrent connections")
	fs.IntVar(&o.requests, "n", 100000, "total requests to issue")
	fs.IntVar(&o.pipeline, "p", 16, "commands written per batch before reading replies")
	fs.IntVar(&o.payload, "d", 64, "value size in bytes")
	fs.IntVar(&o.keys, "keys", 10000, "number of distinct keys the load is spread over")
	fs.StringVar(&o.test, "test", "SET", "workload: SET, GET, INCR, HSET or MIXED")
	fs.DurationVar(&o.timeout, "timeout", 10*time.Second, "per-operation network timeout")

	fs.Usage = func() {
		fmt.Fprint(stderr, "mini-bench measures a running MiniRedis server.\n\nFlags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return o, flag.ErrHelp
		}
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if o.conns <= 0 {
		return o, fmt.Errorf("connections must be positive, got %d", o.conns)
	}
	if o.requests <= 0 {
		return o, fmt.Errorf("requests must be positive, got %d", o.requests)
	}
	if o.pipeline <= 0 {
		return o, fmt.Errorf("pipeline depth must be positive, got %d", o.pipeline)
	}
	if o.keys <= 0 {
		return o, fmt.Errorf("key count must be positive, got %d", o.keys)
	}
	if o.payload < 0 {
		return o, fmt.Errorf("payload size must not be negative, got %d", o.payload)
	}
	return o, nil
}

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

type workload uint8

const (
	workloadSet workload = iota
	workloadGet
	workloadIncr
	workloadHSet
	workloadMixed
)

func parseWorkload(name string) (workload, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "SET":
		return workloadSet, nil
	case "GET":
		return workloadGet, nil
	case "INCR":
		return workloadIncr, nil
	case "HSET":
		return workloadHSet, nil
	case "MIXED":
		return workloadMixed, nil
	default:
		return 0, fmt.Errorf("unknown test %q: want SET, GET, INCR, HSET or MIXED", name)
	}
}

func (w workload) String() string {
	switch w {
	case workloadSet:
		return "SET"
	case workloadGet:
		return "GET"
	case workloadIncr:
		return "INCR"
	case workloadHSet:
		return "HSET"
	case workloadMixed:
		return "MIXED (GET and SET alternating)"
	default:
		return "unknown"
	}
}

// keyName labels the keys a workload uses, so two workloads running against
// the same server cannot collide on a key that already holds the wrong type.
func (w workload) keyName() string {
	switch w {
	case workloadSet:
		return "SET"
	case workloadGet:
		return "GET"
	case workloadIncr:
		return "INCR"
	case workloadHSet:
		return "HSET"
	case workloadMixed:
		return "MIXED"
	default:
		return "UNKNOWN"
	}
}

// needsPrefill reports whether the workload reads keys that must already
// exist, so that it does not silently measure "key not found" replies.
func (w workload) needsPrefill() bool {
	return w == workloadGet || w == workloadMixed
}

// command renders one request for this workload. index is the global request
// number, which is what spreads the load across the key space.
func (w workload) command(keys [][]byte, payload []byte, index int) [][]byte {
	key := keys[index%len(keys)]
	switch w {
	case workloadGet:
		return [][]byte{[]byte("GET"), key}
	case workloadIncr:
		return [][]byte{[]byte("INCR"), key}
	case workloadHSet:
		return [][]byte{[]byte("HSET"), key, []byte("field"), payload}
	case workloadMixed:
		if index%2 == 0 {
			return [][]byte{[]byte("GET"), key}
		}
		return [][]byte{[]byte("SET"), key, payload}
	default:
		return [][]byte{[]byte("SET"), key, payload}
	}
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

type bench struct {
	o       options
	wl      workload
	payload []byte
	keys    [][]byte
}

func newBench(o options, wl workload) *bench {
	// The key prefix includes the workload name. Two workloads running against
	// the same server must not share keys: an INCR over keys a SET run filled
	// with non-numeric strings would measure type errors, not increments.
	prefix := "bench:" + wl.keyName() + ":"
	keys := make([][]byte, o.keys)
	for i := range keys {
		keys[i] = []byte(prefix + strconv.Itoa(i))
	}
	return &bench{
		o:       o,
		wl:      wl,
		payload: bytes.Repeat([]byte("v"), o.payload),
		keys:    keys,
	}
}

func run(args []string, stdout, stderr io.Writer) int {
	o, err := parse(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "mini-bench: %v\n", err)
		return 2
	}
	wl, err := parseWorkload(o.test)
	if err != nil {
		fmt.Fprintf(stderr, "mini-bench: %v\n", err)
		return 2
	}
	b := newBench(o, wl)

	// Probe first: a wrong address or password must fail before any
	// measurement starts, not after a million failed requests.
	if err := b.probe(); err != nil {
		fmt.Fprintf(stderr, "mini-bench: %v\n", err)
		return 1
	}

	prefilled := 0
	if wl.needsPrefill() {
		prefilled, err = b.prefill()
		if err != nil {
			fmt.Fprintf(stderr, "mini-bench: prefill: %v\n", err)
			return 1
		}
	}

	counts := distribute(o.requests, o.conns)
	results := make([]*workerResult, o.conns)
	for i := range results {
		results[i] = &workerResult{}
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	first := 0
	wallStart := time.Now()
	for id := 0; id < o.conns; id++ {
		wg.Add(1)
		go func(id, firstIndex, todo int) {
			defer wg.Done()
			b.worker(id, firstIndex, todo, start, results[id])
		}(id, first, counts[id])
		first += counts[id]
	}
	close(start)
	wg.Wait()
	wall := time.Since(wallStart)

	return report(b, results, wall, prefilled, stdout, stderr)
}

// distribute splits n requests over connections as evenly as possible.
func distribute(n, conns int) []int {
	counts := make([]int, conns)
	for i := range counts {
		counts[i] = n / conns
	}
	for i := 0; i < n%conns; i++ {
		counts[i]++
	}
	return counts
}

type workerResult struct {
	latencies []int64 // microseconds per batch
	completed atomic.Int64
	errors    atomic.Int64
	mu        sync.Mutex
	firstErr  string
	fatal     error
}

func (r *workerResult) setError(reply string) {
	r.mu.Lock()
	if r.firstErr == "" {
		r.firstErr = reply
	}
	r.mu.Unlock()
}

func (r *workerResult) firstError() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.firstErr
}

// worker issues todo requests over its own connection, in pipeline batches.
func (b *bench) worker(id, firstIndex, todo int, start <-chan struct{}, res *workerResult) {
	if todo <= 0 {
		return
	}

	conn, err := net.DialTimeout("tcp", b.o.addr, b.o.timeout)
	if err != nil {
		res.fatal = fmt.Errorf("connection %d: %w", id, err)
		return
	}
	defer conn.Close()

	parser := protocol.NewParser(conn, protocol.DefaultLimits())
	if b.o.password != "" {
		if err := b.authenticate(conn, parser); err != nil {
			res.fatal = fmt.Errorf("connection %d: %w", id, err)
			return
		}
	}

	res.latencies = make([]int64, 0, todo/b.o.pipeline+1)
	batch := make([][][]byte, 0, b.o.pipeline)
	buf := make([]byte, 0, b.o.pipeline*128)

	done := 0
	<-start
	for done < todo {
		depth := b.o.pipeline
		if todo-done < depth {
			depth = todo - done
		}

		batch = batch[:0]
		buf = buf[:0]
		for i := 0; i < depth; i++ {
			cmd := b.wl.command(b.keys, b.payload, firstIndex+done+i)
			batch = append(batch, cmd)
			buf = append(buf, protocol.EncodeCommand(cmd...)...)
		}

		t0 := time.Now()
		if b.o.timeout > 0 {
			_ = conn.SetWriteDeadline(t0.Add(b.o.timeout))
		}
		if _, err := conn.Write(buf); err != nil {
			res.fatal = fmt.Errorf("connection %d: write: %w", id, err)
			return
		}
		for i := 0; i < depth; i++ {
			if b.o.timeout > 0 {
				_ = conn.SetReadDeadline(time.Now().Add(b.o.timeout))
			}
			reply, err := parser.ReadValue()
			if err != nil {
				res.fatal = fmt.Errorf("connection %d: read: %w", id, err)
				return
			}
			if reply.IsError() {
				res.errors.Add(1)
				res.setError(reply.Render())
			}
		}
		res.latencies = append(res.latencies, time.Since(t0).Microseconds())
		done += depth
	}
	res.completed.Store(int64(todo))
}

// probe verifies that the server is reachable and the credentials are accepted.
func (b *bench) probe() error {
	conn, err := net.DialTimeout("tcp", b.o.addr, b.o.timeout)
	if err != nil {
		return fmt.Errorf("cannot connect to %s: %w", b.o.addr, err)
	}
	defer conn.Close()
	parser := protocol.NewParser(conn, protocol.DefaultLimits())

	if b.o.password != "" {
		if err := b.authenticate(conn, parser); err != nil {
			return err
		}
	}
	if _, err := conn.Write(protocol.EncodeCommand([]byte("PING"))); err != nil {
		return fmt.Errorf("PING: %w", err)
	}
	if b.o.timeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(b.o.timeout))
	}
	reply, err := parser.ReadValue()
	if err != nil {
		return fmt.Errorf("PING: %w", err)
	}
	if reply.IsError() {
		return fmt.Errorf("PING: %s", reply.Render())
	}
	return nil
}

// authenticate performs AUTH on an established connection.
func (b *bench) authenticate(conn net.Conn, parser *protocol.Parser) error {
	if _, err := conn.Write(protocol.EncodeCommand([]byte("AUTH"), []byte(b.o.password))); err != nil {
		return fmt.Errorf("AUTH: %w", err)
	}
	if b.o.timeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(b.o.timeout))
	}
	reply, err := parser.ReadValue()
	if err != nil {
		return fmt.Errorf("AUTH: %w", err)
	}
	if reply.IsError() {
		return fmt.Errorf("AUTH rejected: %s", reply.Render())
	}
	return nil
}

// prefill writes one value per key so a read workload has something to read.
func (b *bench) prefill() (int, error) {
	conn, err := net.DialTimeout("tcp", b.o.addr, b.o.timeout)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	parser := protocol.NewParser(conn, protocol.DefaultLimits())
	if b.o.password != "" {
		if err := b.authenticate(conn, parser); err != nil {
			return 0, err
		}
	}

	const chunk = 256
	buf := make([]byte, 0, chunk*128)
	sent := 0
	for i := 0; i < len(b.keys); {
		buf = buf[:0]
		depth := 0
		for depth < chunk && i < len(b.keys) {
			buf = append(buf, protocol.EncodeCommand([]byte("SET"), b.keys[i], b.payload)...)
			i++
			depth++
		}
		if b.o.timeout > 0 {
			_ = conn.SetWriteDeadline(time.Now().Add(b.o.timeout))
		}
		if _, err := conn.Write(buf); err != nil {
			return sent, err
		}
		for j := 0; j < depth; j++ {
			if b.o.timeout > 0 {
				_ = conn.SetReadDeadline(time.Now().Add(b.o.timeout))
			}
			reply, err := parser.ReadValue()
			if err != nil {
				return sent, err
			}
			if reply.IsError() {
				return sent, fmt.Errorf("prefill: %s", reply.Render())
			}
		}
		sent += depth
	}
	return sent, nil
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

func report(b *bench, results []*workerResult, wall time.Duration, prefilled int, stdout, stderr io.Writer) int {
	var (
		completed int64
		errors    int64
		fatal     error
		firstErr  string
		latencies []int64
	)
	for _, r := range results {
		if r.fatal != nil && fatal == nil {
			fatal = r.fatal
		}
		if msg := r.firstError(); msg != "" && firstErr == "" {
			firstErr = msg
		}
		completed += r.completed.Load()
		errors += r.errors.Load()
		latencies = append(latencies, r.latencies...)
	}

	o := b.o
	fmt.Fprintf(stdout, "\nMiniRedis mini-bench\n")
	fmt.Fprintf(stdout, "  %-20s %s\n", "address", o.addr)
	fmt.Fprintf(stdout, "  %-20s %s\n", "test", b.wl.String())
	fmt.Fprintf(stdout, "  %-20s %d\n", "connections", o.conns)
	fmt.Fprintf(stdout, "  %-20s %d\n", "pipeline depth", o.pipeline)
	fmt.Fprintf(stdout, "  %-20s %d\n", "payload bytes", o.payload)
	fmt.Fprintf(stdout, "  %-20s %d\n", "keyspace keys", o.keys)
	fmt.Fprintf(stdout, "  %-20s %d\n", "requests requested", o.requests)
	fmt.Fprintf(stdout, "  %-20s %d\n", "requests completed", completed)
	fmt.Fprintf(stdout, "  %-20s %d\n", "reply errors", errors)
	if prefilled > 0 {
		fmt.Fprintf(stdout, "  %-20s %d\n", "keys prefilled", prefilled)
	}
	fmt.Fprintf(stdout, "  %-20s %s\n", "wall time", wall.Round(time.Microsecond))
	if wall > 0 && completed > 0 {
		fmt.Fprintf(stdout, "  %-20s %.2f\n", "throughput (ops/sec)", float64(completed)/wall.Seconds())
	}

	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		mean := meanMicros(latencies)
		fmt.Fprintf(stdout, "\n  latency per batch of %d commands, milliseconds\n", o.pipeline)
		for _, p := range []struct {
			label string
			value int64
		}{
			{"mean", int64(math.Round(mean))},
			{"p50", percentile(latencies, 50)},
			{"p99", percentile(latencies, 99)},
			{"p99.9", percentile(latencies, 99.9)},
			{"max", latencies[len(latencies)-1]},
		} {
			fmt.Fprintf(stdout, "    %-6s %.3f\n", p.label, float64(p.value)/1000)
		}
		fmt.Fprintf(stdout, "  mean service time per request: %.3f ms (batch mean / depth)\n",
			mean/1000/float64(o.pipeline))
	}

	fmt.Fprintf(stdout, "\n  %-20s %s %s/%s, %d CPUs, GOMAXPROCS=%d\n", "environment",
		runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0))
	fmt.Fprintf(stdout, "  latency is measured per pipeline batch and divided by the depth;\n"+
		"  a batch of one command is therefore a plain round trip.\n")

	if errors > 0 && firstErr != "" {
		fmt.Fprintf(stderr, "mini-bench: %d replies were errors, first: %s\n", errors, firstErr)
	}
	if fatal != nil {
		fmt.Fprintf(stderr, "mini-bench: %v\n", fatal)
		return 1
	}
	if errors > 0 {
		return 1
	}
	return 0
}

func meanMicros(sorted []int64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	var sum int64
	for _, v := range sorted {
		sum += v
	}
	return float64(sum) / float64(len(sorted))
}

// percentile returns the p-th percentile of an ascending slice, using the
// nearest-rank definition (no interpolation), which is the conservative
// choice for tail latency reporting.
func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
