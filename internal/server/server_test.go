package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amandev11/miniredis/internal/command"
	"github.com/amandev11/miniredis/internal/config"
	"github.com/amandev11/miniredis/internal/protocol"
	"github.com/amandev11/miniredis/internal/server"
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// startServer runs a real server on an ephemeral loopback port.
//
// The data directory is either the one in cfg or a fresh temporary directory.
// startServer takes a testing.TB so that benchmarks can reuse the same
// harness: the server under test is identical in both cases.
func startServer(t testing.TB, mutate func(*config.Config)) *server.Server {
	t.Helper()

	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.DataDir = t.TempDir()
	cfg.HTTPAddr = "" // disabled unless a test asks for it
	cfg.LogLevel = "error"
	if mutate != nil {
		mutate(&cfg)
	}

	srv, err := server.New(cfg, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("server.Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	return srv
}

// tcpClient is an independent protocol client: it speaks RESP over a socket and
// knows nothing about the server's internals.
type tcpClient struct {
	t      testing.TB
	conn   net.Conn
	parser *protocol.Parser
	writer *protocol.Writer
}

func dial(t testing.TB, addr string) *tcpClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	limits := protocol.DefaultLimits()
	return &tcpClient{
		t:      t,
		conn:   conn,
		parser: protocol.NewParser(conn, limits),
		writer: protocol.NewWriter(conn),
	}
}

// do sends one command and reads one reply.
func (c *tcpClient) do(args ...string) protocol.Value {
	c.t.Helper()
	c.send(args...)
	return c.read()
}

func (c *tcpClient) send(args ...string) {
	c.t.Helper()
	raw := make([][]byte, len(args))
	for i, a := range args {
		raw[i] = []byte(a)
	}
	if err := c.write(protocol.EncodeCommand(raw...)); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

// write sends raw bytes and flushes them.
func (c *tcpClient) write(b []byte) error {
	if _, err := c.conn.Write(b); err != nil {
		return err
	}
	return nil
}

func (c *tcpClient) read() protocol.Value {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	v, err := c.parser.ReadValue()
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	return v
}

// readError reads a reply and requires it to be an error frame.
func (c *tcpClient) readError() string {
	c.t.Helper()
	v := c.read()
	if v.Type != protocol.TypeError {
		c.t.Fatalf("got %s, want an error frame", v.Render())
	}
	return string(v.Str)
}

// ---------------------------------------------------------------------------
// Basic operation
// ---------------------------------------------------------------------------

func TestServerServesPingAndEcho(t *testing.T) {
	srv := startServer(t, nil)
	c := dial(t, srv.Addr())

	if v := c.do("PING"); v.Type != protocol.TypeSimpleString || string(v.Str) != "PONG" {
		t.Fatalf("PING = %s", v.Render())
	}
	if v := c.do("PING", "hello"); string(v.Str) != "hello" {
		t.Fatalf("PING hello = %s", v.Render())
	}
	if v := c.do("ECHO", "round trip"); string(v.Str) != "round trip" {
		t.Fatalf("ECHO = %s", v.Render())
	}
}

func TestServerStoresAndReturnsStringsAndHashes(t *testing.T) {
	srv := startServer(t, nil)
	c := dial(t, srv.Addr())

	if v := c.do("SET", "greeting", "hello"); string(v.Str) != "OK" {
		t.Fatalf("SET = %s", v.Render())
	}
	if v := c.do("GET", "greeting"); string(v.Str) != "hello" {
		t.Fatalf("GET = %s", v.Render())
	}
	if v := c.do("GET", "missing"); !v.IsNull {
		t.Fatalf("GET missing = %s, want a null bulk string", v.Render())
	}

	if v := c.do("HSET", "user:1", "name", "aman", "role", "engineer"); v.Int != 2 {
		t.Fatalf("HSET = %s", v.Render())
	}
	if v := c.do("HGET", "user:1", "name"); string(v.Str) != "aman" {
		t.Fatalf("HGET = %s", v.Render())
	}
	if v := c.do("HGETALL", "user:1"); len(v.Array) != 4 {
		t.Fatalf("HGETALL = %s", v.Render())
	}
	if v := c.do("TYPE", "user:1"); string(v.Str) != "hash" {
		t.Fatalf("TYPE = %s", v.Render())
	}
	if v := c.do("DBSIZE"); v.Int != 2 {
		t.Fatalf("DBSIZE = %d, want 2", v.Int)
	}
}

func TestServerAcceptsBinaryBytes(t *testing.T) {
	srv := startServer(t, nil)
	c := dial(t, srv.Addr())

	key := "bin\x00\xff\x01"
	value := string([]byte{0x00, 0x0d, 0x0a, 0xff, 0x7f})

	if v := c.do("SET", key, value); string(v.Str) != "OK" {
		t.Fatalf("SET = %s", v.Render())
	}
	if v := c.do("GET", key); string(v.Str) != value {
		t.Fatalf("GET = %x, want %x", v.Str, []byte(value))
	}
}

func TestMultipleCommandsOnOneConnection(t *testing.T) {
	srv := startServer(t, nil)
	c := dial(t, srv.Addr())

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("k%d", i)
		if v := c.do("SET", key, fmt.Sprint(i)); string(v.Str) != "OK" {
			t.Fatalf("SET %s = %s", key, v.Render())
		}
	}
	total := 0
	for i := 0; i < 20; i++ {
		if v := c.do("INCR", "counter"); v.Int != int64(i+1) {
			t.Fatalf("INCR = %d, want %d", v.Int, i+1)
		}
		total++
	}
	if total != 20 {
		t.Fatalf("ran %d increments", total)
	}
	if v := c.do("DBSIZE"); v.Int != 21 {
		t.Fatalf("DBSIZE = %d, want 21", v.Int)
	}
}

// TestPipelinedRequests sends several commands in one write and checks that
// every reply arrives in order.
func TestPipelinedRequests(t *testing.T) {
	srv := startServer(t, nil)
	c := dial(t, srv.Addr())

	var buf bytes.Buffer
	for i := 0; i < 50; i++ {
		buf.Write(protocol.EncodeCommandStrings("SET", fmt.Sprintf("p%02d", i), fmt.Sprint(i)))
	}
	buf.Write(protocol.EncodeCommandStrings("DBSIZE"))
	if err := c.write(buf.Bytes()); err != nil {
		t.Fatalf("pipelined write: %v", err)
	}

	for i := 0; i < 50; i++ {
		v := c.read()
		if string(v.Str) != "OK" {
			t.Fatalf("reply %d = %s, want OK", i, v.Render())
		}
	}
	if v := c.read(); v.Int != 50 {
		t.Fatalf("DBSIZE after the pipeline = %d, want 50", v.Int)
	}
}

// TestFragmentedRequest sends one command one byte at a time, which is the
// worst case for a parser that assumes one read equals one command.
func TestFragmentedRequest(t *testing.T) {
	srv := startServer(t, nil)
	c := dial(t, srv.Addr())

	wire := protocol.EncodeCommandStrings("SET", "fragmented", "value")
	for _, b := range wire {
		if err := c.write([]byte{b}); err != nil {
			t.Fatalf("byte write: %v", err)
		}
	}
	// The reply to the fragmented request must still arrive.
	if v := c.read(); string(v.Str) != "OK" {
		t.Fatalf("SET reply = %s", v.Render())
	}
	if v := c.do("GET", "fragmented"); string(v.Str) != "value" {
		t.Fatalf("GET = %s", v.Render())
	}
}

// ---------------------------------------------------------------------------
// Malformed and hostile input
// ---------------------------------------------------------------------------

func TestMalformedRequestIsReportedAndConnectionCloses(t *testing.T) {
	srv := startServer(t, nil)

	cases := map[string]string{
		"wrong type byte":      "+OK\r\n",
		"bad array length":     "*abc\r\n",
		"negative bulk length": "*1\r\n$-5\r\n",
		"missing CRLF":         "*1\r\n$4\r\nPINGXY",
		"zero array elements":  "*0\r\n",
	}

	for name, wire := range cases {
		t.Run(name, func(t *testing.T) {
			c := dial(t, srv.Addr())
			if err := c.write([]byte(wire)); err != nil {
				t.Fatalf("write: %v", err)
			}
			msg := c.readError()
			if !strings.Contains(msg, "Protocol error") {
				t.Fatalf("reply %q should be a protocol error", msg)
			}

			// The connection must be closed after a protocol error.
			_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, err := c.conn.Read(make([]byte, 1)); err == nil {
				t.Fatal("the connection stayed open after a protocol error")
			}
		})
	}

	// The server must still be healthy.
	c := dial(t, srv.Addr())
	if v := c.do("PING"); string(v.Str) != "PONG" {
		t.Fatalf("PING after protocol errors = %s", v.Render())
	}
	if v := c.do("DBSIZE"); v.Int != 0 {
		t.Fatalf("the keyspace was affected by malformed input: DBSIZE = %d", v.Int)
	}
}

func TestOversizedRequestsAreRejected(t *testing.T) {
	srv := startServer(t, func(cfg *config.Config) {
		cfg.MaxBulkBytes = 1024
		cfg.MaxRequestBytes = 2048
		cfg.MaxArrayLen = 16
	})

	t.Run("bulk string above the limit", func(t *testing.T) {
		c := dial(t, srv.Addr())
		// Declare a 4 MiB bulk string; the server must refuse it before it
		// reads or allocates the body.
		if err := c.write([]byte("*2\r\n$3\r\nGET\r\n$4194304\r\n")); err != nil {
			t.Fatalf("write: %v", err)
		}
		if msg := c.readError(); !strings.Contains(msg, "limit") {
			t.Fatalf("reply %q should mention the configured limit", msg)
		}
	})

	t.Run("too many arguments", func(t *testing.T) {
		c := dial(t, srv.Addr())
		if err := c.write([]byte("*1000\r\n")); err != nil {
			t.Fatalf("write: %v", err)
		}
		if msg := c.readError(); !strings.Contains(msg, "argument limit") {
			t.Fatalf("reply %q should mention the argument limit", msg)
		}
	})

	t.Run("length integer overflow", func(t *testing.T) {
		c := dial(t, srv.Addr())
		if err := c.write([]byte("*99999999999999999999999\r\n")); err != nil {
			t.Fatalf("write: %v", err)
		}
		if msg := c.readError(); !strings.Contains(msg, "overflow") {
			t.Fatalf("reply %q should mention the overflow", msg)
		}
	})

	// The server survives all of it.
	c := dial(t, srv.Addr())
	if v := c.do("PING"); string(v.Str) != "PONG" {
		t.Fatalf("PING = %s", v.Render())
	}
}

func TestUnknownCommandIsReportedWithoutClosing(t *testing.T) {
	srv := startServer(t, nil)
	c := dial(t, srv.Addr())

	msg := c.readErrorFor("NOPE", "arg")
	if !strings.Contains(msg, "unknown command") {
		t.Fatalf("reply %q should report an unknown command", msg)
	}
	// A command error must not close the connection.
	if v := c.do("PING"); string(v.Str) != "PONG" {
		t.Fatalf("PING after an unknown command = %s", v.Render())
	}
}

// readErrorFor sends a command and requires an error reply.
func (c *tcpClient) readErrorFor(args ...string) string {
	c.t.Helper()
	c.send(args...)
	return c.readError()
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

func TestManyClientsConcurrently(t *testing.T) {
	srv := startServer(t, nil)

	const clients = 16
	const opsPerClient = 50

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", srv.Addr(), 5*time.Second)
			if err != nil {
				t.Errorf("client %d: dial: %v", id, err)
				return
			}
			defer conn.Close()

			parser := protocol.NewParser(conn, protocol.DefaultLimits())
			for j := 0; j < opsPerClient; j++ {
				cmd := protocol.EncodeCommandStrings("INCR", "shared")
				if _, err := conn.Write(cmd); err != nil {
					t.Errorf("client %d: write: %v", id, err)
					return
				}
				_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				if _, err := parser.ReadValue(); err != nil {
					t.Errorf("client %d: read: %v", id, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	c := dial(t, srv.Addr())
	want := int64(clients * opsPerClient)
	if v := c.do("GET", "shared"); v.IsNull || string(v.Str) != fmt.Sprint(want) {
		t.Fatalf("shared = %s, want %d: increments were lost", v.Render(), want)
	}
}

// ---------------------------------------------------------------------------
// Expiration over the wire
// ---------------------------------------------------------------------------

func TestKeyExpiresOverTheWire(t *testing.T) {
	srv := startServer(t, nil)
	c := dial(t, srv.Addr())

	if v := c.do("SET", "short", "lived", "PX", "40"); string(v.Str) != "OK" {
		t.Fatalf("SET = %s", v.Render())
	}
	if v := c.do("GET", "short"); string(v.Str) != "lived" {
		t.Fatalf("GET before expiry = %s", v.Render())
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		if v := c.do("EXISTS", "short"); v.Int == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the key never expired")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if v := c.do("GET", "short"); !v.IsNull {
		t.Fatalf("GET after expiry = %s, want a null bulk string", v.Render())
	}
	if v := c.do("TTL", "short"); v.Int != -2 {
		t.Fatalf("TTL after expiry = %d, want -2", v.Int)
	}
	if v := c.do("DBSIZE"); v.Int != 0 {
		t.Fatalf("DBSIZE = %d, want 0", v.Int)
	}
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

func TestAuthenticationOverTheWire(t *testing.T) {
	const password = "correct-horse-battery-staple"
	srv := startServer(t, func(cfg *config.Config) { cfg.RequirePass = password })

	c := dial(t, srv.Addr())

	// A protected command is refused before AUTH.
	if msg := c.readErrorFor("GET", "k"); !strings.HasPrefix(msg, "NOAUTH") {
		t.Fatalf("reply %q should start with NOAUTH", msg)
	}
	// PING is allowed before authentication.
	if v := c.do("PING"); string(v.Str) != "PONG" {
		t.Fatalf("PING before AUTH = %s", v.Render())
	}
	// A wrong password is refused.
	if msg := c.readErrorFor("AUTH", "wrong"); !strings.HasPrefix(msg, "WRONGPASS") {
		t.Fatalf("reply %q should start with WRONGPASS", msg)
	}
	// The right password unlocks the connection.
	if v := c.do("AUTH", password); string(v.Str) != "OK" {
		t.Fatalf("AUTH = %s", v.Render())
	}
	if v := c.do("SET", "k", "v"); string(v.Str) != "OK" {
		t.Fatalf("SET after AUTH = %s", v.Render())
	}
	if v := c.do("GET", "k"); string(v.Str) != "v" {
		t.Fatalf("GET after AUTH = %s", v.Render())
	}

	// Authentication is per connection, not global.
	other := dial(t, srv.Addr())
	if msg := other.readErrorFor("GET", "k"); !strings.HasPrefix(msg, "NOAUTH") {
		t.Fatalf("a new connection was authenticated: %q", msg)
	}

	// The password must never appear in INFO.
	if v := other.do("AUTH", password); string(v.Str) != "OK" {
		t.Fatalf("AUTH = %s", v.Render())
	}
	if info := string(other.do("INFO", "all").Str); strings.Contains(info, password) {
		t.Fatal("INFO exposed the configured password")
	}
}

func TestRepeatedAuthFailuresCloseTheConnection(t *testing.T) {
	srv := startServer(t, func(cfg *config.Config) { cfg.RequirePass = "right" })
	c := dial(t, srv.Addr())

	for i := 0; i < 5; i++ {
		if msg := c.readErrorFor("AUTH", "wrong"); !strings.HasPrefix(msg, "WRONGPASS") {
			t.Fatalf("attempt %d: reply %q", i, msg)
		}
	}
	// The next read must fail: the server closed the connection.
	_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("the connection stayed open after repeated authentication failures")
	}
}

func TestMaxClientsIsEnforced(t *testing.T) {
	srv := startServer(t, func(cfg *config.Config) { cfg.MaxClients = 1 })

	first := dial(t, srv.Addr())
	if v := first.do("PING"); string(v.Str) != "PONG" {
		t.Fatalf("first client PING = %s", v.Render())
	}

	// The second connection is accepted and immediately rejected with a
	// message, rather than dropped silently.
	conn, err := net.DialTimeout("tcp", srv.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("expected a rejection message, got %v", err)
	}
	if !strings.Contains(string(buf[:n]), "max number of clients") {
		t.Fatalf("rejection message = %q", buf[:n])
	}

	// The first client is unaffected.
	if v := first.do("PING"); string(v.Str) != "PONG" {
		t.Fatalf("first client PING = %s", v.Render())
	}
}

// ---------------------------------------------------------------------------
// Connection lifecycle
// ---------------------------------------------------------------------------

func TestQuitClosesOnlyThatConnection(t *testing.T) {
	srv := startServer(t, nil)
	quitter := dial(t, srv.Addr())
	other := dial(t, srv.Addr())

	if v := quitter.do("QUIT"); string(v.Str) != "OK" {
		t.Fatalf("QUIT = %s", v.Render())
	}
	_ = quitter.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := quitter.conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("the connection stayed open after QUIT")
	}
	// The server and the other client keep working.
	if v := other.do("PING"); string(v.Str) != "PONG" {
		t.Fatalf("other client PING = %s", v.Render())
	}
}

// TestClientDisconnectDoesNotStopTheServer covers the abrupt case: a client
// vanishes mid-command.
func TestClientDisconnectDoesNotStopTheServer(t *testing.T) {
	srv := startServer(t, nil)

	conn, err := net.DialTimeout("tcp", srv.Addr(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Send a truncated request and close.
	if _, err := conn.Write([]byte("*2\r\n$3\r\nGET\r\n$100\r\nhalf")); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	c := dial(t, srv.Addr())
	if v := c.do("PING"); string(v.Str) != "PONG" {
		t.Fatalf("PING after an abrupt disconnect = %s", v.Render())
	}
}

func TestShutdownClosesConnectionsAndIsIdempotent(t *testing.T) {
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.DataDir = t.TempDir()
	cfg.HTTPAddr = ""
	cfg.LogLevel = "error"

	srv, err := server.New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}

	c := dial(t, srv.Addr())
	if v := c.do("SET", "k", "v"); string(v.Str) != "OK" {
		t.Fatalf("SET = %s", v.Render())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	// The client connection must have been closed.
	_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("the connection survived shutdown")
	}
	// New connections must be refused.
	if conn, err := net.DialTimeout("tcp", srv.Addr(), time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("the listener still accepts connections after shutdown")
	}
	// Shutting down twice must be safe.
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Durability through the server
// ---------------------------------------------------------------------------

func TestDataSurvivesAServerRestart(t *testing.T) {
	dir := t.TempDir()
	newServer := func() *server.Server {
		cfg := config.Default()
		cfg.Addr = "127.0.0.1:0"
		cfg.DataDir = dir
		cfg.HTTPAddr = ""
		cfg.LogLevel = "error"
		srv, err := server.New(cfg, nil)
		if err != nil {
			t.Fatalf("server.New: %v", err)
		}
		if err := srv.Start(); err != nil {
			t.Fatalf("server.Start: %v", err)
		}
		return srv
	}

	first := newServer()
	c1 := dial(t, first.Addr())
	if v := c1.do("SET", "durable", "value"); string(v.Str) != "OK" {
		t.Fatalf("SET = %s", v.Render())
	}
	if v := c1.do("HSET", "hash", "field", "value"); v.Int != 1 {
		t.Fatalf("HSET = %s", v.Render())
	}
	if v := c1.do("SET", "volatile", "value", "EX", "600"); string(v.Str) != "OK" {
		t.Fatalf("SET EX = %s", v.Render())
	}
	if v := c1.do("INCR", "counter"); v.Int != 1 {
		t.Fatalf("INCR = %s", v.Render())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := first.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	cancel()

	// A brand new process image over the same directory.
	second := newServer()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = second.Shutdown(ctx)
	}()

	c2 := dial(t, second.Addr())
	if v := c2.do("GET", "durable"); v.IsNull || string(v.Str) != "value" {
		t.Fatalf("GET after restart = %s, want \"value\"", v.Render())
	}
	if v := c2.do("HGET", "hash", "field"); v.IsNull || string(v.Str) != "value" {
		t.Fatalf("HGET after restart = %s", v.Render())
	}
	if v := c2.do("GET", "counter"); v.IsNull || string(v.Str) != "1" {
		t.Fatalf("counter after restart = %s", v.Render())
	}
	// The deadline is restored as an absolute timestamp, so it is still in the
	// future but shorter than the original ten minutes.
	if v := c2.do("TTL", "volatile"); v.Int <= 0 || v.Int > 600 {
		t.Fatalf("TTL after restart = %d, want a positive value at most 600", v.Int)
	}
}

func TestSnapshotSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.DataDir = dir
	cfg.HTTPAddr = ""
	cfg.LogLevel = "error"
	cfg.AppendOnly = false // durability comes from the snapshot only
	cfg.SnapshotOnShutdown = true

	srv, err := server.New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	c := dial(t, srv.Addr())
	if v := c.do("SET", "snapshotted", "yes"); string(v.Str) != "OK" {
		t.Fatalf("SET = %s", v.Render())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	again, err := server.New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := again.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = again.Shutdown(ctx)
	}()

	c2 := dial(t, again.Addr())
	if v := c2.do("GET", "snapshotted"); v.IsNull || string(v.Str) != "yes" {
		t.Fatalf("GET after a snapshot restart = %s", v.Render())
	}
	if v := c2.do("INFO", "persistence"); !strings.Contains(string(v.Str), "recovery_snapshot_loaded:1") {
		t.Fatalf("INFO should report that a snapshot was loaded:\n%s", v.Str)
	}
}

// TestCorruptSnapshotStopsStartup proves a corrupt persistence file is never
// silently treated as an empty database.
func TestCorruptSnapshotStopsStartup(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.DataDir = dir
	cfg.HTTPAddr = ""
	cfg.AppendOnly = false
	cfg.SnapshotOnShutdown = true

	srv, err := server.New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	c := dial(t, srv.Addr())
	c.do("SET", "k", "v")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()

	// Corrupt the snapshot's checksum.
	path := filepath.Join(dir, "miniredis.mrs")
	raw, err := readFile(path)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	raw[len(raw)-1] ^= 0xff
	if err := writeFile(path, raw); err != nil {
		t.Fatal(err)
	}

	if _, err := server.New(cfg, nil); err == nil {
		t.Fatal("the server started with a corrupt snapshot")
	} else if !strings.Contains(err.Error(), "snapshot") {
		t.Fatalf("error %v should mention the snapshot", err)
	}
}

// ---------------------------------------------------------------------------
// Observability
// ---------------------------------------------------------------------------

func TestInfoReportsMeasuredState(t *testing.T) {
	srv := startServer(t, nil)
	c := dial(t, srv.Addr())

	for i := 0; i < 5; i++ {
		c.do("SET", fmt.Sprintf("k%d", i), "v")
	}
	c.do("SET", "withttl", "v", "EX", "100")
	c.do("NOTACOMMAND")

	raw := string(c.do("INFO", "all").Str)
	for _, want := range []string{
		"miniredis_version:",
		"uptime_in_seconds:",
		"connected_clients:1",
		"db0:keys=6,expires=1",
		"total_commands_processed:",
		"cmdstat_set:calls=6",
		"cmdstat_notacommand:calls=1",
		"journal_enabled:1",
		"recovery_status:",
	} {
		if !strings.Contains(raw, want) {
			t.Errorf("INFO is missing %q\n%s", want, raw)
		}
	}
	// A command error must be counted as an error, not silently dropped.
	if !strings.Contains(raw, "total_command_errors:1") {
		t.Errorf("INFO should report one command error:\n%s", raw)
	}
}

func TestMetricsCountProtocolErrorsAndConnections(t *testing.T) {
	srv := startServer(t, nil)

	good := dial(t, srv.Addr())
	good.do("PING")
	good.do("PING")

	bad := dial(t, srv.Addr())
	bad.write([]byte("nonsense\r\n"))
	bad.readError()

	deadline := time.Now().Add(2 * time.Second)
	for {
		snap := srv.Metrics().Snapshot()
		if snap.ProtocolErrors >= 1 && snap.ConnectionsAccepted >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics did not record the events: %+v", snap)
		}
		time.Sleep(10 * time.Millisecond)
	}

	snap := srv.Metrics().Snapshot()
	if snap.CommandsProcessed < 2 {
		t.Errorf("CommandsProcessed = %d, want at least 2", snap.CommandsProcessed)
	}
	if snap.UptimeSeconds < 0 {
		t.Errorf("UptimeSeconds = %d", snap.UptimeSeconds)
	}
}

// ---------------------------------------------------------------------------
// HTTP diagnostics
// ---------------------------------------------------------------------------

func TestHTTPDiagnostics(t *testing.T) {
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.DataDir = t.TempDir()
	cfg.HTTPAddr = "127.0.0.1:0"
	cfg.LogLevel = "error"

	srv, err := server.New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	// The TCP server must work while HTTP diagnostics are running.
	c := dial(t, srv.Addr())
	if v := c.do("SET", "k", "v"); string(v.Str) != "OK" {
		t.Fatalf("SET = %s", v.Render())
	}

	httpAddr := srv.HTTPAddr()
	if httpAddr == "" {
		t.Fatal("HTTP diagnostics did not start")
	}

	base := "http://" + httpAddr
	get := func(path string) (int, []byte) {
		t.Helper()
		client := &http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get(base + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return resp.StatusCode, body
	}

	if status, body := get("/healthz"); status != http.StatusOK || !strings.Contains(string(body), "ok") {
		t.Fatalf("/healthz = %d %s", status, body)
	}
	if status, body := get("/ready"); status != http.StatusOK {
		t.Fatalf("/ready = %d %s", status, body)
	}

	status, body := get("/metrics")
	if status != http.StatusOK {
		t.Fatalf("/metrics = %d %s", status, body)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("/metrics is not valid JSON: %v (%s)", err, body)
	}
	keyspace, ok := payload["keyspace"].(map[string]any)
	if !ok {
		t.Fatalf("/metrics has no keyspace section: %s", body)
	}
	if keys, _ := keyspace["keys"].(float64); keys != 1 {
		t.Fatalf("keyspace.keys = %v, want 1", keyspace["keys"])
	}
}

// TestReadiness covers every branch of the readiness decision.
//
// A poisoned journal cannot be staged portably in a test, because an already
// open file descriptor keeps its permissions when the directory or file mode
// changes. The decision is therefore a pure function of measured state, and
// this table drives it directly.
func TestReadiness(t *testing.T) {
	cases := []struct {
		name       string
		info       command.Info
		closing    bool
		wantReady  bool
		wantReason string
	}{
		{name: "healthy", wantReady: true},
		{
			name:      "shutdown in progress",
			closing:   true,
			wantReady: false, wantReason: "shutdown in progress",
		},
		{
			name: "poisoned journal",
			info: command.Info{Persistence: command.PersistenceInfo{
				JournalPoisoned: true, JournalPoisonReason: "no space left on device",
			}},
			wantReady: false, wantReason: "no space left on device",
		},
		{
			name: "repaired tail is a warning, not a failure",
			info: command.Info{Persistence: command.PersistenceInfo{
				Recovery: &command.RecoverySummary{Status: "repaired", DiscardedBytes: 42},
			}},
			wantReady: true, wantReason: "42 bytes discarded",
		},
		{
			name: "clean recovery",
			info: command.Info{Persistence: command.PersistenceInfo{
				Recovery: &command.RecoverySummary{Status: "ok"},
			}},
			wantReady: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ready, reasons := server.Readiness(tc.info, tc.closing)
			if ready != tc.wantReady {
				t.Fatalf("ready = %v, want %v (reasons %v)", ready, tc.wantReady, reasons)
			}
			if tc.wantReason != "" {
				joined := strings.Join(reasons, "; ")
				if !strings.Contains(joined, tc.wantReason) {
					t.Fatalf("reasons %q should mention %q", joined, tc.wantReason)
				}
			}
		})
	}
}

// readFile and writeFile keep the corruption test self-contained.
func readFile(path string) ([]byte, error) { return os.ReadFile(path) }

func writeFile(path string, data []byte) error { return os.WriteFile(path, data, 0o600) }
