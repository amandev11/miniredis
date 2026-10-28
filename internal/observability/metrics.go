// Package observability collects the runtime counters that INFO and the HTTP
// diagnostics endpoint report.
//
// Every value here is measured: counters are incremented where the event
// happens, and nothing is estimated. There are no fabricated throughput or
// memory figures anywhere in MiniRedis.
package observability

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics holds process-wide counters. All operations are safe for concurrent
// use; the hot paths use atomics only.
type Metrics struct {
	// StartTime is when the process began tracking metrics.
	StartTime time.Time

	CommandsProcessed atomic.Int64
	CommandErrors     atomic.Int64
	CommandMicros     atomic.Int64

	ConnectionsAccepted atomic.Int64
	ConnectionsRejected atomic.Int64
	ConnectionsCurrent  atomic.Int64
	ProtocolErrors      atomic.Int64
	AuthFailures        atomic.Int64
	KeysExpired         atomic.Int64

	mu         sync.RWMutex
	perCommand map[string]*CommandStat
}

// CommandStat accumulates per-command counters.
type CommandStat struct {
	Calls  atomic.Int64
	Errors atomic.Int64
	Micros atomic.Int64
}

// New returns initialized metrics.
func New() *Metrics {
	return &Metrics{
		StartTime:  time.Now(),
		perCommand: make(map[string]*CommandStat),
	}
}

// stat returns the counter set for a command, creating it on first use.
func (m *Metrics) stat(name string) *CommandStat {
	m.mu.RLock()
	st, ok := m.perCommand[name]
	m.mu.RUnlock()
	if ok {
		return st
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if st, ok := m.perCommand[name]; ok {
		return st
	}
	st = &CommandStat{}
	m.perCommand[name] = st
	return st
}

// RecordCommand records one dispatched command. name must be the upper-cased
// command name, d is how long the handler took, and failed reports whether the
// command returned an error.
func (m *Metrics) RecordCommand(name string, d time.Duration, failed bool) {
	m.CommandsProcessed.Add(1)
	m.CommandMicros.Add(d.Microseconds())

	st := m.stat(name)
	st.Calls.Add(1)
	st.Micros.Add(d.Microseconds())
	if failed {
		m.CommandErrors.Add(1)
		st.Errors.Add(1)
	}
}

// AddExpired records keys removed by the active expiration cycle.
func (m *Metrics) AddExpired(n int64) {
	if n > 0 {
		m.KeysExpired.Add(n)
	}
}

// CommandStatSnapshot is one row of the commandstats table.
type CommandStatSnapshot struct {
	Name   string
	Calls  int64
	Errors int64
	Micros int64
}

// CommandStats returns per-command counters sorted by name, so INFO output is
// stable between calls.
func (m *Metrics) CommandStats() []CommandStatSnapshot {
	m.mu.RLock()
	out := make([]CommandStatSnapshot, 0, len(m.perCommand))
	for name, st := range m.perCommand {
		out = append(out, CommandStatSnapshot{
			Name:   name,
			Calls:  st.Calls.Load(),
			Errors: st.Errors.Load(),
			Micros: st.Micros.Load(),
		})
	}
	m.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Snapshot is an immutable copy of the counters.
type Snapshot struct {
	UptimeSeconds       int64
	CommandsProcessed   int64
	CommandErrors       int64
	CommandMicros       int64
	ConnectionsAccepted int64
	ConnectionsRejected int64
	ConnectionsCurrent  int64
	ProtocolErrors      int64
	AuthFailures        int64
	KeysExpired         int64
}

// Snapshot copies the counters for reporting.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		UptimeSeconds:       int64(time.Since(m.StartTime).Seconds()),
		CommandsProcessed:   m.CommandsProcessed.Load(),
		CommandErrors:       m.CommandErrors.Load(),
		CommandMicros:       m.CommandMicros.Load(),
		ConnectionsAccepted: m.ConnectionsAccepted.Load(),
		ConnectionsRejected: m.ConnectionsRejected.Load(),
		ConnectionsCurrent:  m.ConnectionsCurrent.Load(),
		ProtocolErrors:      m.ProtocolErrors.Load(),
		AuthFailures:        m.AuthFailures.Load(),
		KeysExpired:         m.KeysExpired.Load(),
	}
}

// CommandsPerSecond estimates throughput over the process lifetime. It is a
// real measurement (processed commands divided by elapsed time) and is
// labelled as a lifetime average wherever it is reported.
func (s Snapshot) CommandsPerSecond() float64 {
	if s.UptimeSeconds <= 0 {
		return 0
	}
	return float64(s.CommandsProcessed) / float64(s.UptimeSeconds)
}

// AverageCommandMicros is the mean handler time across all commands.
func (s Snapshot) AverageCommandMicros() float64 {
	if s.CommandsProcessed == 0 {
		return 0
	}
	return float64(s.CommandMicros) / float64(s.CommandsProcessed)
}
