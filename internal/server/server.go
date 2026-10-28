// Package server owns MiniRedis's network listener, connection lifecycle and
// background maintenance.
//
// Responsibilities:
//
//   - accept TCP connections and hand each one to its own goroutine;
//   - enforce connection, request and response limits;
//   - build the store, the persistence manager and the command registry;
//   - run startup recovery before any client is served;
//   - run the bounded active expiration cycle;
//   - serve the optional HTTP diagnostics endpoint;
//   - shut everything down in a defined order, releasing every resource.
//
// The server never parses protocol bytes itself: that is internal/protocol's
// job. It never decides command semantics either: that is internal/command's.
package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/amandev11/miniredis/internal/clock"
	"github.com/amandev11/miniredis/internal/command"
	"github.com/amandev11/miniredis/internal/config"
	"github.com/amandev11/miniredis/internal/observability"
	"github.com/amandev11/miniredis/internal/persistence"
	"github.com/amandev11/miniredis/internal/protocol"
	"github.com/amandev11/miniredis/internal/store"
)

// Server is the running MiniRedis instance.
type Server struct {
	cfg   config.Config
	log   *slog.Logger
	clk   clock.Clock
	met   *observability.Metrics
	store *store.Store
	reg   *command.Registry

	// persist is nil when every durability feature is disabled.
	persist *persistence.Manager
	// recovery is the report from the startup replay, if there was one.
	recovery *persistence.RecoveryReport

	listener net.Listener
	addr     string
	httpAddr string

	mu         sync.Mutex
	conns      map[uint64]*connection
	nextConnID uint64
	closing    bool

	httpSrv *http.Server

	wg          sync.WaitGroup
	expireStop  chan struct{}
	expireDone  chan struct{}
	startedAt   time.Time
	started     bool
	shutdownOne sync.Once
	shutdownCh  chan struct{}
}

// New builds a server from cfg. It runs startup recovery, so a corrupt
// persistence file surfaces here rather than after the listener is open.
func New(cfg config.Config, logger *slog.Logger) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		// A nil logger means "build one from the configuration", so that
		// cfg.LogLevel is honoured everywhere. The tests and benchmarks rely
		// on that: they set LogLevel to error and expect quiet output.
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.Level(cfg.SlogLevel())}))
	}

	s := &Server{
		cfg:        cfg,
		log:        logger,
		clk:        clock.System{},
		met:        observability.New(),
		reg:        command.NewDefaultRegistry(),
		conns:      make(map[uint64]*connection),
		shutdownCh: make(chan struct{}),
	}

	if cfg.PersistenceEnabled() {
		mgr, err := persistence.Open(persistence.Config{
			Dir:                cfg.DataDir,
			EnableJournal:      cfg.AppendOnly,
			Fsync:              persistence.FsyncMode(cfg.AppendFsync),
			Repair:             persistence.RepairPolicy(cfg.JournalRepair),
			MaxRecordBytes:     cfg.MaxJournalRecordBytes,
			SnapshotInterval:   cfg.SnapshotInterval,
			SnapshotOnShutdown: cfg.SnapshotOnShutdown,
			Logger:             logger,
		})
		if err != nil {
			return nil, fmt.Errorf("open persistence: %w", err)
		}
		s.persist = mgr
	}

	var recorder store.Recorder
	if s.persist != nil {
		recorder = s.persist
	}
	s.store = store.New(s.clk, store.Options{
		Recorder:      recorder,
		MaxValueBytes: cfg.MaxValueBytes,
		MaxKeyBytes:   cfg.MaxKeyBytes,
		MaxHashFields: cfg.MaxHashFields,
		MaxFieldBytes: cfg.MaxFieldBytes,
	})

	if s.persist != nil {
		s.persist.Attach(s.store)
		report, err := s.persist.Recover()
		if err != nil {
			_ = s.persist.Close()
			return nil, fmt.Errorf("recovery failed: %w", err)
		}
		s.recovery = report
		if len(report.Warnings) > 0 {
			for _, w := range report.Warnings {
				logger.Warn("recovery warning", "detail", w)
			}
		}
		logger.Info("recovery complete",
			"status", report.Status,
			"keys", report.SnapshotKeys,
			"journal_records", report.JournalRecords,
			"journal_applied", report.JournalApplied,
			"duration_ms", report.DurationMS)
	}
	return s, nil
}

// Store exposes the database, which integration tests use to inspect state
// directly after driving the wire protocol.
func (s *Server) Store() *store.Store { return s.store }

// Registry exposes the command table.
func (s *Server) Registry() *command.Registry { return s.reg }

// Metrics exposes the counters.
func (s *Server) Metrics() *observability.Metrics { return s.met }

// Persistence exposes the durability manager, which may be nil.
func (s *Server) Persistence() *persistence.Manager { return s.persist }

// Addr returns the address the server is listening on. It is only meaningful
// after Start.
func (s *Server) Addr() string { return s.addr }

// HTTPAddr returns the bound address of the HTTP diagnostics endpoint, or an
// empty string when it is disabled. It is only meaningful after Start.
func (s *Server) HTTPAddr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.httpAddr
}

// Start opens the listener and launches the accept loop and background work.
func (s *Server) Start() error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("server is already started")
	}
	s.mu.Unlock()

	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		s.releasePersistence()
		return fmt.Errorf("listen on %s: %w", s.cfg.Addr, err)
	}

	s.mu.Lock()
	s.listener = ln
	s.addr = ln.Addr().String()
	s.startedAt = time.Now()
	s.started = true
	s.mu.Unlock()

	if s.persist != nil {
		s.persist.Start()
	}
	s.startExpireLoop()

	if s.cfg.HTTPAddr != "" {
		if err := s.startHTTP(); err != nil {
			// The diagnostic endpoint is optional, so a failure there must not
			// stop the database from serving.
			s.log.Warn("HTTP diagnostics unavailable", "addr", s.cfg.HTTPAddr, "err", err)
		}
	}

	s.wg.Add(1)
	go s.acceptLoop()

	s.log.Info("miniredis listening",
		"addr", s.addr,
		"path", s.cfg.DataDir,
		"appendonly", s.cfg.AppendOnly,
		"appendfsync", s.cfg.AppendFsync,
		"snapshot_interval", s.cfg.SnapshotInterval.String(),
		"auth_required", s.cfg.RequirePass != "",
		"commands", s.reg.Len())
	return nil
}

// acceptLoop accepts connections until the listener is closed.
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	var backoff time.Duration
	for {
		nc, err := s.listener.Accept()
		if err != nil {
			if s.isClosing() {
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				s.log.Warn("accept timed out", "err", err)
				continue
			}
			// A transient accept failure (EMFILE, for example) must not kill
			// the server: back off briefly and try again.
			if backoff == 0 {
				backoff = 5 * time.Millisecond
			} else {
				backoff *= 2
			}
			if backoff > time.Second {
				backoff = time.Second
			}
			s.log.Error("accept failed", "err", err, "retry_in", backoff.String())
			select {
			case <-time.After(backoff):
			case <-s.shutdownCh:
				return
			}
			continue
		}
		backoff = 0
		s.met.ConnectionsAccepted.Add(1)

		if current := s.met.ConnectionsCurrent.Load(); current >= int64(s.cfg.MaxClients) {
			s.met.ConnectionsRejected.Add(1)
			// Tell the client why instead of dropping the socket silently.
			_, _ = nc.Write([]byte("-ERR max number of clients reached\r\n"))
			_ = nc.Close()
			s.log.Warn("connection rejected: at max-clients", "limit", s.cfg.MaxClients, "addr", nc.RemoteAddr().String())
			continue
		}

		s.serveConn(nc)
	}
}

// serveConn registers a new connection and starts its goroutine.
func (s *Server) serveConn(nc net.Conn) {
	if tcp, ok := nc.(*net.TCPConn); ok {
		// Replies are small and latency-sensitive: never wait to coalesce.
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(30 * time.Second)
	}

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		_ = nc.Close()
		return
	}
	s.nextConnID++
	id := s.nextConnID
	conn := newConnection(s, id, nc)
	s.conns[id] = conn
	s.mu.Unlock()

	s.met.ConnectionsCurrent.Add(1)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		conn.serve()
		s.mu.Lock()
		delete(s.conns, id)
		s.mu.Unlock()
		s.met.ConnectionsCurrent.Add(-1)
	}()
}

// startExpireLoop runs the bounded active expiration cycle.
func (s *Server) startExpireLoop() {
	if s.cfg.ExpireSweepInterval <= 0 {
		s.log.Info("active expiration disabled: only expired keys that are touched are removed")
		return
	}
	s.expireStop = make(chan struct{})
	s.expireDone = make(chan struct{})

	interval := s.cfg.ExpireSweepInterval
	sample := s.cfg.ExpireSampleSize

	go func() {
		defer close(s.expireDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.expireStop:
				return
			case <-ticker.C:
				// Each cycle is bounded by the sample size, so a large
				// keyspace never causes a long stall.
				removed, _ := s.store.ActiveExpireCycle(sample)
				s.met.AddExpired(int64(removed))
			}
		}
	}()
}

// isClosing reports whether shutdown has been requested.
func (s *Server) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

// Shutdown stops the server in a defined order.
//
// Sequence:
//
//  1. stop accepting new connections and wake any blocked accept;
//  2. stop the active expiration loop;
//  3. wait for in-flight commands (up to the context deadline), closing only
//     connections that are idle so a command in progress can finish;
//  4. force-close anything still open;
//  5. stop the HTTP diagnostics endpoint;
//  6. flush and close persistence, writing a final snapshot when configured.
func (s *Server) Shutdown(ctx context.Context) error {
	var joined error

	s.shutdownOne.Do(func() {
		s.log.Info("shutdown requested")

		s.mu.Lock()
		s.closing = true
		ln := s.listener
		s.mu.Unlock()
		close(s.shutdownCh)

		if ln != nil {
			_ = ln.Close()
		}

		if s.expireStop != nil {
			close(s.expireStop)
			<-s.expireDone
		}

		joined = errors.Join(joined, s.drainConnections(ctx))
		joined = errors.Join(joined, s.stopHTTP(ctx))
		joined = errors.Join(joined, s.releasePersistence())
		s.wg.Wait()
		s.log.Info("shutdown complete")
	})

	return joined
}

// drainConnections waits for idle connections to close and stops the rest.
//
// A connection that is executing a command is left alone until the deadline:
// interrupting it would report a failure for a mutation that may already be
// committed.
func (s *Server) drainConnections(ctx context.Context) error {
	deadline := time.Now().Add(5 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}

	for {
		open := s.snapshotConnections()
		if len(open) == 0 {
			return nil
		}
		busy := 0
		for _, c := range open {
			if c.isBusy() {
				busy++
				continue
			}
			c.close()
		}
		if time.Now().After(deadline) {
			for _, c := range s.snapshotConnections() {
				c.close()
			}
			if ctx.Err() != nil {
				return fmt.Errorf("shutdown deadline reached with %d connection(s) still closing", busy)
			}
			return nil
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (s *Server) snapshotConnections() []*connection {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*connection, 0, len(s.conns))
	for _, c := range s.conns {
		out = append(out, c)
	}
	return out
}

// Close stops the server immediately, without waiting for in-flight commands.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.Shutdown(ctx)
}

// releasePersistence closes journal and snapshot files exactly once.
func (s *Server) releasePersistence() error {
	s.mu.Lock()
	mgr := s.persist
	s.persist = nil
	s.mu.Unlock()
	if mgr == nil {
		return nil
	}
	if err := mgr.Close(); err != nil {
		s.log.Error("closing persistence failed", "err", err)
		return fmt.Errorf("close persistence: %w", err)
	}
	return nil
}

// RequestShutdown implements command.RuntimeInfo: a client asked the server to
// stop, so shutdown runs asynchronously to let the reply reach the client.
func (s *Server) RequestShutdown() {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			s.log.Error("shutdown reported an error", "err", err)
		}
	}()
}

// CheckPassword implements command.RuntimeInfo using a constant-time compare.
func (s *Server) CheckPassword(supplied []byte) bool {
	want := s.cfg.RequirePass
	if want == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(want), supplied) == 1
}

// Uptime returns how long the server has been running.
func (s *Server) Uptime() time.Duration {
	s.mu.Lock()
	started := s.startedAt
	s.mu.Unlock()
	if started.IsZero() {
		return 0
	}
	return time.Since(started)
}

// ServerInfo implements command.RuntimeInfo.
func (s *Server) ServerInfo() command.Info {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	info := command.Info{
		Version:       config.Version,
		Mode:          "standalone",
		GoVersion:     runtime.Version(),
		OS:            runtime.GOOS,
		Arch:          runtime.GOARCH,
		ProcessID:     os.Getpid(),
		UptimeSeconds: int64(s.Uptime().Seconds()),
		DataDir:       s.cfg.DataDir,
	}
	host, port := splitAddr(s.addr)
	if port == 0 {
		host, port = splitAddr(s.cfg.Addr)
	}
	info.Bind = host
	info.Port = port

	metrics := s.met.Snapshot()
	info.Clients = command.ClientStats{
		ConnectedClients:    int(metrics.ConnectionsCurrent),
		MaxClients:          s.cfg.MaxClients,
		TotalConnections:    uint64(metrics.ConnectionsAccepted),
		RejectedConnections: uint64(metrics.ConnectionsRejected),
	}
	info.Store = command.StoreStats{
		Keys:           s.store.Len(),
		Expires:        s.store.ExpiresCount(),
		ApproxBytes:    s.store.ApproxBytes(),
		HeapAllocBytes: memStats.HeapAlloc,
		HeapSysBytes:   memStats.HeapSys,
	}
	info.Exec = command.ExecStats{
		CommandsProcessed: uint64(metrics.CommandsProcessed),
		CommandErrors:     uint64(metrics.CommandErrors),
		ProtocolErrors:    uint64(metrics.ProtocolErrors),
		KeysExpired:       uint64(metrics.KeysExpired),
		AuthFailures:      uint64(metrics.AuthFailures),
		AvgCommandMicros:  metrics.AverageCommandMicros(),
		CommandsPerSecond: metrics.CommandsPerSecond(),
	}
	info.Persistence = s.persistenceInfo()
	info.CommandStat = s.met.CommandStats()
	return info
}

// persistenceInfo maps the durability layer's status onto the INFO section.
func (s *Server) persistenceInfo() command.PersistenceInfo {
	out := command.PersistenceInfo{
		Enabled:   s.cfg.PersistenceEnabled(),
		FsyncMode: s.cfg.AppendFsync,
		SnapshotPath: func() string {
			if s.persist == nil {
				return ""
			}
			return s.persist.SnapshotPath()
		}(),
	}
	if s.persist == nil {
		return out
	}

	status := s.persist.Status()
	out.JournalEnabled = status.JournalEnabled
	out.JournalPath = status.Journal.Path
	out.JournalSizeBytes = status.Journal.SizeBytes
	out.JournalSeq = status.Journal.Seq
	out.JournalAppends = status.Journal.Appends
	out.JournalFsyncs = status.Journal.Fsyncs
	out.JournalLastFsyncMS = status.Journal.LastFsyncMS
	out.JournalWriteErrors = status.Journal.WriteFailures
	out.JournalPoisoned = status.Journal.Poisoned
	out.JournalPoisonReason = status.Journal.PoisonedReason
	out.SnapshotExists = status.SnapshotExists
	out.SnapshotCount = status.SnapshotCount
	out.SnapshotLastMS = status.LastSnapshotMS
	out.SnapshotLastSeq = status.LastSnapshotSeq
	out.SnapshotLastBytes = status.LastSnapshotBytes
	out.LastError = status.LastError
	if r := status.Recovery; r != nil {
		out.Recovery = &command.RecoverySummary{
			Status:         r.Status,
			DurationMS:     r.DurationMS,
			CompletedAtMS:  r.CompletedAtMS,
			SnapshotLoaded: r.SnapshotLoaded,
			SnapshotSeq:    r.SnapshotSeq,
			SnapshotKeys:   r.SnapshotKeys,
			JournalRecords: r.JournalRecords,
			JournalApplied: r.JournalApplied,
			JournalSkipped: r.JournalSkipped,
			JournalOrphans: r.JournalOrphans,
			DiscardedBytes: r.DiscardedBytes,
			Repaired:       r.TruncationRepaired,
			Warnings:       r.Warnings,
		}
	}
	return out
}

// splitAddr splits host:port, returning 0 for an unparseable port.
func splitAddr(addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, 0
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return host, 0
	}
	return host, port
}

// newProtocolParser builds a parser with the configured limits, so the wire
// layer enforces exactly the bounds the operator chose.
func (s *Server) newProtocolParser(r net.Conn) *protocol.Parser {
	return protocol.NewParser(r, protocol.Limits{
		MaxBulkBytes:    s.cfg.MaxBulkBytes,
		MaxArrayLen:     s.cfg.MaxArrayLen,
		MaxRequestBytes: s.cfg.MaxRequestBytes,
		MaxLineBytes:    s.cfg.MaxLineBytes,
		MaxDepth:        8,
	})
}
