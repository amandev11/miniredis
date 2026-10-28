package server

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/amandev11/miniredis/internal/command"
)

// startHTTP launches the optional diagnostics endpoint.
//
// It is bound to a loopback address by default. The endpoint is deliberately
// narrow: liveness, readiness and measured counters. It can never return a
// stored value, and it never exposes the configured password.
func (s *Server) startHTTP() error {
	ln, err := net.Listen("tcp", s.cfg.HTTPAddr)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/metrics", s.handleMetrics)

	srv := &http.Server{
		Handler: mux,
		// Bounds how long a client may take to send its headers, so a stalled
		// peer cannot hold a connection open indefinitely.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	s.mu.Lock()
	s.httpSrv = srv
	s.httpAddr = ln.Addr().String()
	s.mu.Unlock()

	go func() {
		err := srv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("HTTP diagnostics stopped", "err", err)
		}
	}()

	s.log.Info("HTTP diagnostics listening", "addr", ln.Addr().String())
	return nil
}

// stopHTTP shuts the diagnostics endpoint down.
func (s *Server) stopHTTP(ctx context.Context) error {
	s.mu.Lock()
	srv := s.httpSrv
	s.httpSrv = nil
	s.httpAddr = ""
	s.mu.Unlock()
	if srv == nil {
		return nil
	}
	if err := srv.Shutdown(ctx); err != nil {
		_ = srv.Close()
		return err
	}
	return nil
}

// handleHealth answers whether the process is alive and serving. It never
// touches the store, so it stays cheap.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"uptime_seconds": int64(s.Uptime().Seconds()),
	})
}

// Readiness decides whether the server can safely accept database operations.
//
// It is a pure function of measured state so that every branch can be tested
// directly: the endpoint's behaviour must not depend on being able to stage a
// real disk failure.
//
// A repaired journal tail is reported as a reason but does not make the server
// unready: the data that survived is consistent, and the operator still needs
// to know that bytes were discarded.
func Readiness(info command.Info, closing bool) (ready bool, reasons []string) {
	ready = true
	reasons = make([]string, 0, 2)

	if closing {
		ready = false
		reasons = append(reasons, "shutdown in progress")
	}
	if info.Persistence.JournalPoisoned {
		ready = false
		reason := "journal is poisoned"
		if info.Persistence.JournalPoisonReason != "" {
			reason += ": " + info.Persistence.JournalPoisonReason
		}
		reasons = append(reasons, reason)
	}
	if r := info.Persistence.Recovery; r != nil && r.Status == "repaired" {
		reasons = append(reasons, "journal tail was repaired: "+pluralBytes(r.DiscardedBytes)+" discarded")
	}
	return ready, reasons
}

// handleReady answers whether the server can safely accept database
// operations: it is started, not shutting down, and persistence is healthy.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	info := s.ServerInfo()
	ready, reasons := Readiness(info, s.isClosing())

	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{
		"ready":    ready,
		"reasons":  reasons,
		"keys":     info.Store.Keys,
		"uptime_s": info.UptimeSeconds,
	})
}

// handleMetrics returns the measured counters as JSON.
//
// Only measurements appear here: no stored key or value is ever included, and
// the configured password is not represented in any field.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	info := s.ServerInfo()
	payload := map[string]any{
		"server": map[string]any{
			"version":        info.Version,
			"mode":           info.Mode,
			"go_version":     info.GoVersion,
			"os":             info.OS,
			"arch":           info.Arch,
			"process_id":     info.ProcessID,
			"bind":           info.Bind,
			"port":           info.Port,
			"uptime_seconds": info.UptimeSeconds,
			"auth_required":  s.cfg.RequirePass != "",
			"command_count":  s.reg.Len(),
		},
		"clients": map[string]any{
			"connected": info.Clients.ConnectedClients,
			"max":       info.Clients.MaxClients,
			"accepted":  info.Clients.TotalConnections,
			"rejected":  info.Clients.RejectedConnections,
		},
		"keyspace": map[string]any{
			"keys":                 info.Store.Keys,
			"expires":              info.Store.Expires,
			"payload_bytes_approx": info.Store.ApproxBytes,
		},
		"runtime": map[string]any{
			"heap_alloc_bytes": info.Store.HeapAllocBytes,
			"heap_sys_bytes":   info.Store.HeapSysBytes,
		},
		"stats": map[string]any{
			"commands_processed":           info.Exec.CommandsProcessed,
			"command_errors":               info.Exec.CommandErrors,
			"protocol_errors":              info.Exec.ProtocolErrors,
			"auth_failures":                info.Exec.AuthFailures,
			"keys_expired":                 info.Exec.KeysExpired,
			"avg_command_usec_lifetime":    info.Exec.AvgCommandMicros,
			"commands_per_second_lifetime": info.Exec.CommandsPerSecond,
		},
		"persistence": map[string]any{
			"enabled":             info.Persistence.Enabled,
			"journal_enabled":     info.Persistence.JournalEnabled,
			"journal_size_bytes":  info.Persistence.JournalSizeBytes,
			"journal_appends":     info.Persistence.JournalAppends,
			"journal_fsyncs":      info.Persistence.JournalFsyncs,
			"journal_sequence":    info.Persistence.JournalSeq,
			"journal_poisoned":    info.Persistence.JournalPoisoned,
			"appendfsync":         info.Persistence.FsyncMode,
			"snapshot_exists":     info.Persistence.SnapshotExists,
			"snapshot_count":      info.Persistence.SnapshotCount,
			"snapshot_last_bytes": info.Persistence.SnapshotLastBytes,
			"last_error":          info.Persistence.LastError,
		},
	}
	writeJSON(w, http.StatusOK, payload)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(body)
	_, _ = w.Write([]byte("\n"))
}

func pluralBytes(n int64) string {
	return itoa(n) + " bytes"
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	negative := n < 0
	if negative {
		n = -n
	}
	var buf [24]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
