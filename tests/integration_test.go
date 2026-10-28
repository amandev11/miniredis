// Package integration_test holds MiniRedis's end-to-end tests.
//
// Every test here starts a real server on a loopback socket and talks to it
// with real TCP clients. Nothing reaches into the server's internals to set
// state up: the tests do what a user does, which is why they can catch
// integration mistakes that unit tests cannot.
//
// The persistence tests use real temporary directories and real restarts, so
// they verify recovery rather than describing it.
package integration_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amandev11/miniredis/internal/config"
	"github.com/amandev11/miniredis/internal/persistence"
	"github.com/amandev11/miniredis/internal/protocol"
	"github.com/amandev11/miniredis/internal/server"
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// testConfig returns a configuration for a server on an ephemeral loopback
// port with a temporary data directory.
func testConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.DataDir = t.TempDir()
	cfg.HTTPAddr = "" // the HTTP endpoint has its own tests
	cfg.LogLevel = "error"
	return cfg
}

// serve starts a server and registers its shutdown for the end of the test.
func serve(t *testing.T, cfg config.Config) *server.Server {
	t.Helper()
	srv, err := server.New(cfg, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("server.Start: %v", err)
	}
	t.Cleanup(func() { shutdown(t, srv) })
	return srv
}

// shutdown stops a server and waits for it to finish. Shutdown is idempotent,
// so calling it explicitly in a restart test is safe.
func shutdown(t *testing.T, srv *server.Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

// client is an independent protocol client.
type client struct {
	t      *testing.T
	conn   net.Conn
	parser *protocol.Parser
}

func dialTo(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &client{t: t, conn: conn, parser: protocol.NewParser(conn, protocol.DefaultLimits())}
}

// do sends one command and returns the reply, failing the test on a transport
// error.
func (c *client) do(args ...string) protocol.Value {
	c.t.Helper()
	reply, err := c.try(args...)
	if err != nil {
		c.t.Fatalf("%v: %v", args, err)
	}
	return reply
}

// try sends one command and reports a transport error to the caller, which
// protocol-error tests need because the server closes the connection.
func (c *client) try(args ...string) (protocol.Value, error) {
	raw := make([][]byte, len(args))
	for i, a := range args {
		raw[i] = []byte(a)
	}
	if _, err := c.conn.Write(protocol.EncodeCommand(raw...)); err != nil {
		return protocol.Value{}, err
	}
	return c.parser.ReadValue()
}

// write sends raw bytes, for malformed-input tests.
func (c *client) write(raw string) error {
	_, err := c.conn.Write([]byte(raw))
	return err
}

// str asserts a non-error reply and returns it as a string.
func (c *client) str(args ...string) string {
	c.t.Helper()
	reply := c.do(args...)
	if reply.IsError() {
		c.t.Fatalf("%v returned an error: %s", args, reply.Render())
	}
	return string(reply.Str)
}

// num asserts an integer reply.
func (c *client) num(args ...string) int64 {
	c.t.Helper()
	reply := c.do(args...)
	if reply.IsError() {
		c.t.Fatalf("%v returned an error: %s", args, reply.Render())
	}
	if reply.Type != protocol.TypeInteger {
		c.t.Fatalf("%v returned %s, want an integer", args, reply.Render())
	}
	return reply.Int
}

// strNum reads a bulk-string reply that must contain an integer. GET returns a
// bulk string, so a counter check parses it instead of expecting an integer
// frame.
func (c *client) strNum(args ...string) int64 {
	c.t.Helper()
	raw := c.str(args...)
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		c.t.Fatalf("%v returned %q, which is not an integer: %v", args, raw, err)
	}
	return n
}

// ---------------------------------------------------------------------------
// Strings, hashes and concurrency
// ---------------------------------------------------------------------------

func TestTwoClientsShareOneKeyspace(t *testing.T) {
	t.Parallel()
	srv := serve(t, testConfig(t))

	writer := dialTo(t, srv.Addr())
	reader := dialTo(t, srv.Addr())

	if got := writer.str("SET", "greeting", "hello from client one"); got != "OK" {
		t.Fatalf("SET replied %q, want OK", got)
	}
	if got := reader.str("GET", "greeting"); got != "hello from client one" {
		t.Fatalf("second client read %q", got)
	}

	writer.do("HSET", "user:1", "name", "aman", "role", "engineer")
	reader.do("HSET", "user:1", "shirt", "blue")
	all := writer.do("HGETALL", "user:1")
	if len(all.Array) != 6 {
		t.Fatalf("HGETALL returned %d elements, want 6: %s", len(all.Array), all.Render())
	}
	if got := reader.str("HGET", "user:1", "role"); got != "engineer" {
		t.Fatalf("HGET role = %q", got)
	}
	if got := writer.str("TYPE", "user:1"); got != "hash" {
		t.Fatalf("TYPE = %q, want hash", got)
	}
}

// TestConcurrentIncrementsAreNotLost is the core atomicity test: four clients
// hammer one key and the final value must equal the number of INCRs issued.
func TestConcurrentIncrementsAreNotLost(t *testing.T) {
	t.Parallel()
	srv := serve(t, testConfig(t))

	const (
		clients         = 4
		perClient       = 500
		expected  int64 = clients * perClient
	)

	var wg sync.WaitGroup
	errs := make(chan string, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := dialTo(t, srv.Addr())
			for j := 0; j < perClient; j++ {
				reply, err := c.try("INCR", "counter")
				if err != nil {
					errs <- err.Error()
					return
				}
				if reply.IsError() {
					errs <- reply.Render()
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for msg := range errs {
		t.Fatalf("concurrent INCR failed: %s", msg)
	}

	checker := dialTo(t, srv.Addr())
	if got := checker.strNum("GET", "counter"); got != expected {
		t.Fatalf("counter = %d after %d concurrent INCRs, want %d", got, expected, expected)
	}
}

// TestConcurrentMixedWorkloadKeepsInvariants runs several kinds of write and
// read at once and checks that the keyspace still agrees with itself.
func TestConcurrentMixedWorkloadKeepsInvariants(t *testing.T) {
	t.Parallel()
	srv := serve(t, testConfig(t))

	const workers = 6
	const ops = 200

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := dialTo(t, srv.Addr())
			key := "mixed:" + strconv.Itoa(w%3)
			for i := 0; i < ops; i++ {
				var reply protocol.Value
				var err error
				switch i % 5 {
				case 0:
					reply, err = c.try("SET", key, "v"+strconv.Itoa(i))
				case 1:
					reply, err = c.try("INCR", "mixed:counter:"+strconv.Itoa(w%3))
				case 2:
					reply, err = c.try("HSET", "mixed:hash:"+strconv.Itoa(w%3), "f"+strconv.Itoa(i%8), "v")
				case 3:
					reply, err = c.try("GET", key)
				default:
					reply, err = c.try("EXPIRE", key, "60")
				}
				if err != nil {
					t.Errorf("worker %d: %v", w, err)
					return
				}
				if reply.IsError() {
					t.Errorf("worker %d: %s", w, reply.Render())
					return
				}
			}
		}(w)
	}
	wg.Wait()

	checker := dialTo(t, srv.Addr())
	size := checker.num("DBSIZE")
	keys := checker.do("KEYS", "*")
	if int64(len(keys.Array)) != size {
		t.Fatalf("DBSIZE reports %d keys but KEYS returned %d", size, len(keys.Array))
	}
	for i := 0; i < 3; i++ {
		counterKey := "mixed:counter:" + strconv.Itoa(i)
		value := checker.strNum("GET", counterKey)
		if value <= 0 || value > ops {
			t.Fatalf("%s = %d, which is outside the possible range (1..%d)", counterKey, value, ops)
		}
	}
}

// ---------------------------------------------------------------------------
// Expiration
// ---------------------------------------------------------------------------

func TestExpirationOverTheWire(t *testing.T) {
	t.Parallel()
	srv := serve(t, testConfig(t))
	c := dialTo(t, srv.Addr())

	c.do("SET", "short", "value", "PX", "150")
	c.do("SET", "long", "value", "EX", "60")
	c.do("SET", "forever", "value")

	if got := c.num("TTL", "short"); got != -1 && got != 0 {
		t.Fatalf("TTL short = %d, want 0 or -1 for a sub-second TTL", got)
	}
	if got := c.num("TTL", "long"); got < 1 || got > 60 {
		t.Fatalf("TTL long = %d, want 1..60", got)
	}
	if got := c.num("TTL", "forever"); got != -1 {
		t.Fatalf("TTL forever = %d, want -1", got)
	}

	if got := c.str("GET", "short"); got != "value" {
		t.Fatalf("GET before the deadline = %q", got)
	}

	time.Sleep(300 * time.Millisecond)
	if reply := c.do("GET", "short"); !reply.IsNull {
		t.Fatalf("GET after the deadline = %s, want a null bulk string", reply.Render())
	}
	if got := c.num("EXISTS", "short"); got != 0 {
		t.Fatalf("EXISTS short = %d after the deadline, want 0", got)
	}
	if got := c.num("TTL", "short"); got != -2 {
		t.Fatalf("TTL short = %d after the deadline, want -2", got)
	}
	// A key with no deadline is unaffected by the expiration machinery.
	if got := c.str("GET", "forever"); got != "value" {
		t.Fatalf("GET forever = %q", got)
	}
}

// ---------------------------------------------------------------------------
// Persistence and restart
// ---------------------------------------------------------------------------

// TestJournalReplayRestoresStateAfterRestart writes with the journal enabled
// and no snapshots at all, so the only way the data can come back is by
// replaying the journal.
func TestJournalReplayRestoresStateAfterRestart(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	cfg.AppendOnly = true
	cfg.AppendFsync = "always"
	cfg.SnapshotOnShutdown = false
	cfg.SnapshotInterval = 0

	first := serve(t, cfg)
	c := dialTo(t, first.Addr())
	c.do("SET", "alpha", "one")
	c.do("APPEND", "alpha", "-two")
	c.do("SET", "beta", "temporary")
	c.do("DEL", "beta")
	c.do("INCR", "visits")
	c.do("INCR", "visits")
	c.do("HSET", "user:1", "name", "aman", "role", "engineer")
	c.do("HDEL", "user:1", "role")
	c.do("SET", "expiring", "value", "EX", "120")
	shutdown(t, first)

	// No snapshot should exist: this test is about the journal alone.
	if _, err := os.Stat(filepath.Join(cfg.DataDir, persistence.SnapshotFileName)); !os.IsNotExist(err) {
		t.Fatalf("expected no snapshot file, stat returned %v", err)
	}
	journalInfo, err := os.Stat(filepath.Join(cfg.DataDir, persistence.JournalFileName))
	if err != nil {
		t.Fatalf("journal file: %v", err)
	}
	if journalInfo.Size() == 0 {
		t.Fatal("journal file is empty after writer commands")
	}

	second := serve(t, cfg)
	c2 := dialTo(t, second.Addr())
	if got := c2.str("GET", "alpha"); got != "one-two" {
		t.Fatalf("replayed GET alpha = %q, want one-two", got)
	}
	if got := c2.num("EXISTS", "beta"); got != 0 {
		t.Fatalf("EXISTS beta = %d after a replayed DEL, want 0", got)
	}
	if got := c2.strNum("GET", "visits"); got != 2 {
		t.Fatalf("replayed visits = %d, want 2", got)
	}
	if got := c2.str("HGET", "user:1", "name"); got != "aman" {
		t.Fatalf("replayed HGET name = %q", got)
	}
	if got := c2.num("HEXISTS", "user:1", "role"); got != 0 {
		t.Fatalf("HEXISTS role = %d after a replayed HDEL, want 0", got)
	}
	// The absolute deadline must survive the restart, not restart its clock.
	if got := c2.num("TTL", "expiring"); got < 1 || got > 120 {
		t.Fatalf("replayed TTL = %d, want 1..120", got)
	}
	info := second.Persistence().Status()
	if info.Recovery == nil {
		t.Fatal("expected a recovery report after a restart")
	}
	if info.Recovery.JournalApplied == 0 {
		t.Fatalf("recovery applied %d journal records, want more than zero", info.Recovery.JournalApplied)
	}
}

// TestSnapshotPlusJournalReplayComposes covers the documented startup order:
// load the snapshot, then replay only the journal records that are newer.
func TestSnapshotPlusJournalReplayComposes(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	cfg.AppendOnly = true
	cfg.AppendFsync = "always"
	cfg.SnapshotOnShutdown = true

	// Phase 1: two writes, then a clean shutdown, which writes a snapshot.
	first := serve(t, cfg)
	c := dialTo(t, first.Addr())
	c.do("SET", "from-snapshot:1", "one")
	c.do("SET", "from-snapshot:2", "two")
	shutdown(t, first)

	snapshotPath := filepath.Join(cfg.DataDir, persistence.SnapshotFileName)
	before, err := os.Stat(snapshotPath)
	if err != nil {
		t.Fatalf("snapshot after a clean shutdown: %v", err)
	}

	// Phase 2: restart, write only to the journal, and shut down without
	// touching the snapshot.
	cfgNoSnapshot := cfg
	cfgNoSnapshot.SnapshotOnShutdown = false
	second := serve(t, cfgNoSnapshot)
	c2 := dialTo(t, second.Addr())
	c2.do("SET", "from-journal", "three")
	shutdown(t, second)

	after, err := os.Stat(snapshotPath)
	if err != nil {
		t.Fatalf("snapshot after the second phase: %v", err)
	}
	if after.ModTime() != before.ModTime() || after.Size() != before.Size() {
		t.Fatal("the snapshot changed during a phase that was not supposed to write one")
	}

	// Phase 3: both sources must be visible at once.
	third := serve(t, cfg)
	c3 := dialTo(t, third.Addr())
	for key, want := range map[string]string{
		"from-snapshot:1": "one",
		"from-snapshot:2": "two",
		"from-journal":    "three",
	} {
		if got := c3.str("GET", key); got != want {
			t.Fatalf("GET %s = %q, want %q", key, got, want)
		}
	}
	report := third.Persistence().Status().Recovery
	if report == nil {
		t.Fatal("expected a recovery report")
	}
	if !report.SnapshotLoaded || report.SnapshotKeys != 2 {
		t.Fatalf("recovery report says snapshotLoaded=%v keys=%d, want true and 2",
			report.SnapshotLoaded, report.SnapshotKeys)
	}
	if report.JournalSkipped == 0 {
		t.Log("note: no journal records were skipped, so every record was newer than the snapshot")
	}
}

// TestTruncatedJournalTailIsRepaired keeps a real journal file, cuts a record
// in half, and checks that the server starts on the last consistent state and
// reports the repair instead of pretending nothing happened.
func TestTruncatedJournalTailIsRepaired(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	cfg.AppendOnly = true
	cfg.AppendFsync = "always"
	cfg.SnapshotOnShutdown = false
	cfg.JournalRepair = "truncate"

	first := serve(t, cfg)
	c := dialTo(t, first.Addr())
	c.do("SET", "kept", "value")
	shutdown(t, first)

	journalPath := filepath.Join(cfg.DataDir, persistence.JournalFileName)
	before, err := os.Stat(journalPath)
	if err != nil {
		t.Fatalf("journal file: %v", err)
	}
	// Simulate a crash in the middle of an append: three bytes of a new frame
	// that never completed.
	f, err := os.OpenFile(journalPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	if _, err := f.Write([]byte{0x20, 0x00, 0x00}); err != nil {
		t.Fatalf("append partial record: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close journal: %v", err)
	}

	second := serve(t, cfg)
	c2 := dialTo(t, second.Addr())
	if got := c2.str("GET", "kept"); got != "value" {
		t.Fatalf("GET kept = %q after repair, want value", got)
	}
	report := second.Persistence().Status().Recovery
	if report == nil || report.Status != "repaired" {
		t.Fatalf("recovery status = %v, want repaired", report)
	}
	if report.DiscardedBytes == 0 {
		t.Fatal("recovery reported a repair but discarded zero bytes")
	}
	after, err := os.Stat(journalPath)
	if err != nil {
		t.Fatalf("journal after repair: %v", err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("journal is %d bytes after repair, want the %d valid bytes back", after.Size(), before.Size())
	}
}

// TestCorruptSnapshotStopsStartup checks that a damaged snapshot is refused
// rather than silently treated as an empty database.
func TestCorruptSnapshotStopsStartup(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	cfg.AppendOnly = true
	cfg.SnapshotOnShutdown = true

	first := serve(t, cfg)
	c := dialTo(t, first.Addr())
	c.do("SET", "important", "value")
	shutdown(t, first)

	snapshotPath := filepath.Join(cfg.DataDir, persistence.SnapshotFileName)
	if err := os.WriteFile(snapshotPath, []byte("not a snapshot at all"), 0o600); err != nil {
		t.Fatalf("corrupt the snapshot: %v", err)
	}

	if _, err := server.New(cfg, nil); err == nil {
		t.Fatal("server.New accepted a corrupt snapshot; a damaged snapshot must stop startup")
	} else if !strings.Contains(strings.ToLower(err.Error()), "snapshot") {
		t.Fatalf("startup error does not mention the snapshot: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Security and robustness
// ---------------------------------------------------------------------------

func TestAuthenticationIsEnforced(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	cfg.RequirePass = "integration-secret"
	srv := serve(t, cfg)

	c := dialTo(t, srv.Addr())

	// PING and AUTH are allowed before authenticating; data commands are not.
	if got := c.str("PING"); got != "PONG" {
		t.Fatalf("PING before AUTH = %q, want PONG", got)
	}
	reply := c.do("GET", "anything")
	if !reply.IsError() || !strings.Contains(strings.ToUpper(reply.Render()), "NOAUTH") {
		t.Fatalf("GET before AUTH = %s, want a NOAUTH error", reply.Render())
	}
	reply = c.do("AUTH", "wrong-password")
	if !reply.IsError() {
		t.Fatalf("AUTH with a wrong password = %s, want an error", reply.Render())
	}
	reply = c.do("AUTH", "integration-secret")
	if reply.IsError() {
		t.Fatalf("AUTH with the right password failed: %s", reply.Render())
	}

	c.do("SET", "k", "v")
	if got := c.str("GET", "k"); got != "v" {
		t.Fatalf("GET after AUTH = %q", got)
	}

	// The password must not appear in any diagnostic output.
	info := c.do("INFO").Render()
	if strings.Contains(info, "integration-secret") {
		t.Fatal("INFO disclosed the configured password")
	}
}

// TestMalformedInputIsRejectedWithoutStoppingTheServer sends a request that
// violates the protocol and checks that the server replies with an error,
// closes that connection, and keeps serving everyone else.
func TestMalformedInputIsRejectedWithoutStoppingTheServer(t *testing.T) {
	t.Parallel()
	srv := serve(t, testConfig(t))

	bad := dialTo(t, srv.Addr())
	if err := bad.write("-1\r\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	reply, err := bad.parser.ReadValue()
	if err != nil {
		t.Fatalf("expected an error reply before the close, got %v", err)
	}
	if !reply.IsError() {
		t.Fatalf("malformed input produced %s, want an error", reply.Render())
	}
	if !strings.Contains(strings.ToUpper(reply.Render()), "PROTO") {
		t.Fatalf("error reply %q does not identify a protocol error", reply.Render())
	}

	// The next frame must not be readable: the connection is closed.
	_ = bad.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := bad.parser.ReadValue(); err == nil {
		t.Fatal("the connection stayed open after a protocol error")
	}

	// The server itself is unharmed.
	good := dialTo(t, srv.Addr())
	if got := good.str("PING"); got != "PONG" {
		t.Fatalf("PING after a malformed request = %q", got)
	}
}

// TestLargeValueRoundTrip stores a value well beyond the default read buffer,
// which exercises the parser's chunked bulk handling.
func TestLargeValueRoundTrip(t *testing.T) {
	t.Parallel()
	srv := serve(t, testConfig(t))
	c := dialTo(t, srv.Addr())

	value := strings.Repeat("A", 512*1024)
	c.do("SET", "big", value)
	if got := c.str("GET", "big"); got != value {
		t.Fatalf("large value round trip differs: got %d bytes, want %d", len(got), len(value))
	}
	if got := c.num("STRLEN", "big"); got != int64(len(value)) {
		t.Fatalf("STRLEN = %d, want %d", got, len(value))
	}
}

// TestGracefulShutdownIsIdempotentAndReleasesThePort stops a server twice and
// checks that the address can be reused, which is what a container restart
// depends on.
func TestGracefulShutdownIsIdempotentAndReleasesThePort(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	// Bind a fixed port so that a leaked listener would actually collide.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	addr := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatalf("release the port: %v", err)
	}
	cfg.Addr = addr

	first := serve(t, cfg)
	shutdown(t, first)
	shutdown(t, first) // idempotent

	second := serve(t, cfg)
	c := dialTo(t, second.Addr())
	if got := c.str("PING"); got != "PONG" {
		t.Fatalf("PING on the reused port = %q", got)
	}
	shutdown(t, second)
}
