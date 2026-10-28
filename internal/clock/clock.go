// Package clock provides a minimal time abstraction.
//
// All time-dependent code in MiniRedis reads the current time through a Clock
// so that tests can drive expiration deterministically instead of sleeping.
// Times are always Unix epoch milliseconds, matching the unit used by the
// persistence formats and the wire protocol.
package clock

import (
	"sync"
	"time"
)

// Clock reports the current wall-clock time in Unix epoch milliseconds.
type Clock interface {
	// NowMS returns the number of milliseconds since the Unix epoch.
	NowMS() int64
}

// System is the real wall clock.
type System struct{}

// NowMS implements Clock.
func (System) NowMS() int64 { return time.Now().UnixMilli() }

// Manual is a Clock whose value only changes when a test changes it.
//
// Manual is safe for concurrent use so that it can be shared with background
// expiration goroutines inside tests.
type Manual struct {
	mu sync.Mutex
	ms int64
}

// NewManual returns a Manual clock started at startMS.
func NewManual(startMS int64) *Manual { return &Manual{ms: startMS} }

// NewManualAt returns a Manual clock started at the given time.
func NewManualAt(t time.Time) *Manual { return NewManual(t.UnixMilli()) }

// NowMS implements Clock.
func (m *Manual) NowMS() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ms
}

// Set moves the clock to an absolute millisecond timestamp.
func (m *Manual) Set(ms int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ms = ms
}

// Advance moves the clock forward by d. Negative durations are ignored.
func (m *Manual) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ms += d.Milliseconds()
}
