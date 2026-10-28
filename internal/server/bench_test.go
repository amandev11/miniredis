package server_test

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/amandev11/miniredis/internal/protocol"
)

// These benchmarks measure the server over a real loopback socket, so the
// numbers include TCP, the RESP parser and encoder, command dispatch and the
// store. That is deliberate: it is the figure the README and docs/BENCHMARKS.md
// quote, and it is reproducible with
//
//	go test -run '^$' -bench . -benchmem ./internal/server
//
// They are measurements, not claims: nothing here asserts a performance
// target, and the store benchmarks in internal/store measure the engine
// without the network so the two can be compared.

// benchClient is a minimal RESP client for benchmarks.
//
// It returns errors instead of calling b.Fatal, because b.Fatal must not be
// called from a goroutine other than the one running the benchmark. Failures
// are reported with b.Errorf, which is safe from any goroutine.
type benchClient struct {
	conn   net.Conn
	parser *protocol.Parser
}

func newBenchClient(b *testing.B, addr string) (*benchClient, error) {
	b.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	b.Cleanup(func() { _ = conn.Close() })
	return &benchClient{conn: conn, parser: protocol.NewParser(conn, protocol.DefaultLimits())}, nil
}

// roundTrip sends one command and waits for its reply.
func (c *benchClient) roundTrip(args ...string) (protocol.Value, error) {
	raw := make([][]byte, len(args))
	for i, a := range args {
		raw[i] = []byte(a)
	}
	if _, err := c.conn.Write(protocol.EncodeCommand(raw...)); err != nil {
		return protocol.Value{}, err
	}
	return c.parser.ReadValue()
}

// writeBatch writes every command in cmds as one write.
func (c *benchClient) writeBatch(cmds [][][]byte) error {
	buf := make([]byte, 0, len(cmds)*64)
	for _, cmd := range cmds {
		buf = append(buf, protocol.EncodeCommand(cmd...)...)
	}
	_, err := c.conn.Write(buf)
	return err
}

// readBatch reads n replies and reports how many of them were error replies.
func (c *benchClient) readBatch(n int) (int, error) {
	errs := 0
	for i := 0; i < n; i++ {
		reply, err := c.parser.ReadValue()
		if err != nil {
			return errs, err
		}
		if reply.IsError() {
			errs++
		}
	}
	return errs, nil
}

// prefillKeys writes one value per key so read benchmarks never measure a miss.
func prefillKeys(b *testing.B, addr string, n int) {
	b.Helper()
	c, err := newBenchClient(b, addr)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	for i := 0; i < n; i += 128 {
		cmds := make([][][]byte, 0, 128)
		for j := 0; j < 128 && i+j < n; j++ {
			key := []byte("bench:key:" + strconv.Itoa(i+j))
			cmds = append(cmds, [][]byte{[]byte("SET"), key, []byte("value-0123456789")})
		}
		if err := c.writeBatch(cmds); err != nil {
			b.Fatalf("prefill write: %v", err)
		}
		if errs, err := c.readBatch(len(cmds)); err != nil || errs > 0 {
			b.Fatalf("prefill: %d errors, err=%v", errs, err)
		}
	}
}

// keyFor spreads the load over a small key space, which is the common case for
// a cache workload.
func keyFor(prefix string, i int) string {
	return prefix + strconv.Itoa(i%1024)
}

// ---------------------------------------------------------------------------
// Round-trip benchmarks: one command per write, one reply per read
// ---------------------------------------------------------------------------

func BenchmarkSETRoundTrip(b *testing.B) {
	srv := startServer(b, nil)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c, err := newBenchClient(b, srv.Addr())
		if err != nil {
			b.Errorf("dial: %v", err)
			return
		}
		for i := 0; pb.Next(); i++ {
			if _, err := c.roundTrip("SET", keyFor("bench:rt:", i), "value-0123456789"); err != nil {
				b.Errorf("SET: %v", err)
				return
			}
		}
	})
}

func BenchmarkGETRoundTrip(b *testing.B) {
	srv := startServer(b, nil)
	prefillKeys(b, srv.Addr(), 1024)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c, err := newBenchClient(b, srv.Addr())
		if err != nil {
			b.Errorf("dial: %v", err)
			return
		}
		for i := 0; pb.Next(); i++ {
			if _, err := c.roundTrip("GET", keyFor("bench:key:", i)); err != nil {
				b.Errorf("GET: %v", err)
				return
			}
		}
	})
}

func BenchmarkHSETRoundTrip(b *testing.B) {
	srv := startServer(b, nil)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c, err := newBenchClient(b, srv.Addr())
		if err != nil {
			b.Errorf("dial: %v", err)
			return
		}
		for i := 0; pb.Next(); i++ {
			if _, err := c.roundTrip("HSET", keyFor("bench:hash:", i), "field", "value-0123456789"); err != nil {
				b.Errorf("HSET: %v", err)
				return
			}
		}
	})
}

// BenchmarkINCRSpreadParallel measures increments across many keys, where the
// only contention is the store's single write lock.
func BenchmarkINCRSpreadParallel(b *testing.B) {
	srv := startServer(b, nil)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c, err := newBenchClient(b, srv.Addr())
		if err != nil {
			b.Errorf("dial: %v", err)
			return
		}
		for i := 0; pb.Next(); i++ {
			if _, err := c.roundTrip("INCR", keyFor("bench:ctr:", i)); err != nil {
				b.Errorf("INCR: %v", err)
				return
			}
		}
	})
}

// BenchmarkINCRSingleKey measures the worst case for INCR: every client
// contends on one key. Nothing is lost, so the result is a throughput figure
// for a fully serialised update path.
func BenchmarkINCRSingleKey(b *testing.B) {
	srv := startServer(b, nil)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c, err := newBenchClient(b, srv.Addr())
		if err != nil {
			b.Errorf("dial: %v", err)
			return
		}
		for pb.Next() {
			if _, err := c.roundTrip("INCR", "bench:hot"); err != nil {
				b.Errorf("INCR: %v", err)
				return
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Pipelined benchmarks: one write and one batch of reads per iteration
// ---------------------------------------------------------------------------

const benchPipelineDepth = 16

func BenchmarkPipelinedSET(b *testing.B) {
	srv := startServer(b, nil)
	b.SetBytes(int64(benchPipelineDepth * 28))
	cmds := make([][][]byte, benchPipelineDepth)
	for i := range cmds {
		key := []byte("bench:pipe:" + strconv.Itoa(i))
		cmds[i] = [][]byte{[]byte("SET"), key, []byte("value-0123456789")}
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c, err := newBenchClient(b, srv.Addr())
		if err != nil {
			b.Errorf("dial: %v", err)
			return
		}
		for pb.Next() {
			if err := c.writeBatch(cmds); err != nil {
				b.Errorf("pipelined write: %v", err)
				return
			}
			if errs, err := c.readBatch(len(cmds)); err != nil || errs > 0 {
				b.Errorf("pipelined read: %d errors, err=%v", errs, err)
				return
			}
		}
	})
	b.ReportMetric(benchPipelineDepth, "commands/op")
}

func BenchmarkPipelinedGET(b *testing.B) {
	srv := startServer(b, nil)
	prefillKeys(b, srv.Addr(), 1024)
	cmds := make([][][]byte, benchPipelineDepth)
	for i := range cmds {
		cmds[i] = [][]byte{[]byte("GET"), []byte("bench:key:" + strconv.Itoa(i))}
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c, err := newBenchClient(b, srv.Addr())
		if err != nil {
			b.Errorf("dial: %v", err)
			return
		}
		for pb.Next() {
			if err := c.writeBatch(cmds); err != nil {
				b.Errorf("pipelined write: %v", err)
				return
			}
			if errs, err := c.readBatch(len(cmds)); err != nil || errs > 0 {
				b.Errorf("pipelined read: %d errors, err=%v", errs, err)
				return
			}
		}
	})
	b.ReportMetric(benchPipelineDepth, "commands/op")
}
