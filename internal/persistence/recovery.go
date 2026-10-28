package persistence

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amandev11/miniredis/internal/clock"
	"github.com/amandev11/miniredis/internal/store"
)

// Default file names inside the data directory.
const (
	// JournalFileName is the append-only journal.
	JournalFileName = "miniredis.mrj"
	// SnapshotFileName is the current snapshot.
	SnapshotFileName = "miniredis.mrs"
	// TempSuffix is appended to a snapshot while it is being written.
	TempSuffix = ".tmp"
)

// RepairPolicy selects what happens when the journal tail is damaged.
type RepairPolicy string

// Repair policies.
const (
	// RepairTruncate discards the damaged tail, reports loudly how many bytes
	// were dropped, and starts with the last consistent state. This is the
	// default because a torn tail is the normal result of a crash.
	RepairTruncate RepairPolicy = "truncate"
	// RepairFail refuses to start, leaving the file untouched for inspection.
	RepairFail RepairPolicy = "fail"
)

// ParseRepairPolicy validates a repair policy name.
func ParseRepairPolicy(s string) (RepairPolicy, error) {
	switch RepairPolicy(s) {
	case RepairTruncate, RepairFail:
		return RepairPolicy(s), nil
	default:
		return "", fmt.Errorf("unknown repair policy %q (want truncate or fail)", s)
	}
}

// Config configures the durability layer.
type Config struct {
	// Dir is the data directory holding the journal and snapshots.
	Dir string
	// EnableJournal turns the append-only journal on or off.
	EnableJournal bool
	// Fsync selects the durability mode.
	Fsync FsyncMode
	// Repair selects the damaged-tail policy.
	Repair RepairPolicy
	// MaxRecordBytes bounds one journal record and one snapshot field.
	MaxRecordBytes int
	// SnapshotInterval is the automatic snapshot period; zero disables it.
	SnapshotInterval time.Duration
	// SnapshotOnShutdown writes a snapshot during a clean shutdown.
	SnapshotOnShutdown bool
	// Clock is the time source. Nil means the system clock.
	Clock clock.Clock
	// Logger receives recovery and durability events. Nil means quiet
	// operation, which tests use.
	Logger *slog.Logger
}

// withDefaults fills unset fields and validates the configuration.
func (c Config) withDefaults() (Config, error) {
	if c.Fsync == "" {
		c.Fsync = FsyncEverySec
	}
	if _, err := ParseFsyncMode(string(c.Fsync)); err != nil {
		return c, err
	}
	if c.Repair == "" {
		c.Repair = RepairTruncate
	}
	if _, err := ParseRepairPolicy(string(c.Repair)); err != nil {
		return c, err
	}
	if c.MaxRecordBytes <= 0 {
		c.MaxRecordBytes = DefaultMaxRecordBytes
	}
	if c.SnapshotInterval < 0 {
		return c, fmt.Errorf("snapshot interval must not be negative")
	}
	if c.Clock == nil {
		c.Clock = clock.System{}
	}
	return c, nil
}

// RecoveryReport describes what startup recovery did. Every field is derived
// from files that were actually read; nothing is estimated.
type RecoveryReport struct {
	// Status is "empty", "ok" or "repaired".
	Status string
	// DurationMS is how long recovery took.
	DurationMS int64
	// SnapshotLoaded reports that a snapshot was read.
	SnapshotLoaded bool
	// SnapshotSeq is the sequence number stored in that snapshot.
	SnapshotSeq uint64
	// SnapshotKeys is the number of keys the snapshot contained.
	SnapshotKeys int
	// JournalRecords is the number of valid records found.
	JournalRecords int
	// JournalApplied counts records replayed into the store.
	JournalApplied int
	// JournalSkipped counts records already covered by the snapshot.
	JournalSkipped int
	// JournalOrphans counts records that referenced a missing key.
	JournalOrphans int
	// LastSeq is the highest sequence number that was restored.
	LastSeq uint64
	// DiscardedBytes counts damaged bytes removed from the journal tail.
	DiscardedBytes int64
	// TruncationRepaired reports that the tail was actually truncated.
	TruncationRepaired bool
	// Warnings lists every condition an operator should know about.
	Warnings []string
	// CompletedAtMS is when recovery finished.
	CompletedAtMS int64
}

// Status is the durability layer's diagnostic snapshot.
type Status struct {
	JournalEnabled    bool
	Journal           JournalStatus
	SnapshotPath      string
	SnapshotExists    bool
	LastSnapshotMS    int64
	LastSnapshotSeq   uint64
	LastSnapshotBytes int64
	SnapshotCount     uint64
	LastError         string
	Recovery          *RecoveryReport
}

// Manager owns the journal and snapshots and implements store.Recorder.
type Manager struct {
	cfg Config
	clk clock.Clock
	log *slog.Logger

	jrnl *Journal

	mu                sync.Mutex
	st                *store.Store
	memSeq            uint64
	lastSnapshotMS    int64
	lastSnapshotSeq   uint64
	lastSnapshotBytes int64
	snapshotCount     uint64
	lastErr           string
	started           bool
	closed            bool
	// recovered records that startup recovery completed. A manager whose
	// recovery failed must never write over the files an operator is
	// investigating, so the shutdown snapshot is skipped in that case.
	recovered bool
	stop      chan struct{}
	done      chan struct{}

	recovery atomic.Pointer[RecoveryReport]
}

// Open prepares the data directory and opens the journal.
//
// It does not read existing data: call Attach followed by Recover before the
// server starts accepting clients.
func Open(cfg Config) (*Manager, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if cfg.Dir == "" {
		return nil, errors.New("persistence requires a data directory")
	}
	if err := ensureDirectory(cfg.Dir); err != nil {
		return nil, err
	}

	m := &Manager{cfg: cfg, clk: cfg.Clock, log: cfg.Logger}

	// A snapshot temporary file is only ever a partial write from an
	// interrupted run. It is never a source of truth, so removing it is safe.
	tmp := m.SnapshotPath() + TempSuffix
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("remove interrupted snapshot %s: %w", tmp, err)
	}

	if cfg.EnableJournal {
		j, err := OpenJournal(m.JournalPath(), cfg.Fsync, cfg.MaxRecordBytes, cfg.Clock)
		if err != nil {
			return nil, err
		}
		m.jrnl = j
	}
	return m, nil
}

// JournalPath returns the journal's path.
func (m *Manager) JournalPath() string { return filepath.Join(m.cfg.Dir, JournalFileName) }

// SnapshotPath returns the snapshot's path.
func (m *Manager) SnapshotPath() string { return filepath.Join(m.cfg.Dir, SnapshotFileName) }

// Attach binds the store that this manager persists and recovers.
func (m *Manager) Attach(st *store.Store) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.st = st
}

// Record implements store.Recorder. It is called with the store's write lock
// held, from the goroutine that is executing the client's command.
func (m *Manager) Record(mut store.Mutation) (uint64, error) {
	if m.jrnl == nil {
		// Persistence without a journal: mutations are not durable until the
		// next snapshot. The counter still advances so snapshots carry a
		// meaningful sequence number.
		m.mu.Lock()
		m.memSeq++
		seq := m.memSeq
		m.mu.Unlock()
		return seq, nil
	}
	seq, err := m.jrnl.Append(mut)
	if err != nil {
		m.recordError(err)
	}
	return seq, err
}

// Seq returns the last committed sequence number.
func (m *Manager) Seq() uint64 {
	if m.jrnl == nil {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.memSeq
	}
	return m.jrnl.Seq()
}

// Fsync flushes the journal to stable storage.
func (m *Manager) Fsync() error {
	if m.jrnl == nil {
		return nil
	}
	err := m.jrnl.Fsync()
	if err != nil {
		m.recordError(err)
	}
	return err
}

// SaveSnapshot writes a consistent snapshot of the attached store.
//
// It captures the view first (holding the store lock only for the copy) and
// then writes to a temporary file that is flushed and renamed over the real
// snapshot, so the previous snapshot stays usable until the new one is
// complete on disk.
func (m *Manager) SaveSnapshot() error {
	m.mu.Lock()
	st := m.st
	m.mu.Unlock()
	if st == nil {
		return errors.New("persistence is not attached to a store")
	}

	view := st.Snapshot()
	path := m.SnapshotPath()
	size := int64(0)

	err := writeFileAtomic(path, func(f *os.File) error {
		if err := writeSnapshotFile(f, view); err != nil {
			return err
		}
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		size = fi.Size()
		return nil
	})
	if err != nil {
		m.recordError(err)
		return fmt.Errorf("write snapshot %s: %w", path, err)
	}

	m.mu.Lock()
	m.lastSnapshotMS = m.clk.NowMS()
	m.lastSnapshotSeq = view.Seq
	m.lastSnapshotBytes = size
	m.snapshotCount++
	m.mu.Unlock()

	if m.log != nil {
		m.log.Info("snapshot written",
			"path", path, "seq", view.Seq, "keys", len(view.Items), "bytes", size)
	}
	return nil
}

// Recover restores the attached store from the snapshot and journal.
//
// Order: the snapshot is applied first, then every journal record with a
// sequence number greater than the snapshot's. Records at or below the
// snapshot's sequence number are counted and skipped.
func (m *Manager) Recover() (*RecoveryReport, error) {
	start := time.Now()

	m.mu.Lock()
	st := m.st
	m.mu.Unlock()
	if st == nil {
		return nil, errors.New("persistence is not attached to a store")
	}

	report := &RecoveryReport{
		Status:        "empty",
		CompletedAtMS: start.UnixMilli(),
	}

	// 1. Snapshot.
	snapPath := m.SnapshotPath()
	if _, err := os.Stat(snapPath); err == nil {
		view, err := loadSnapshotFile(snapPath, m.cfg.MaxRecordBytes)
		if err != nil {
			// A corrupt snapshot is fatal: starting with a silently empty
			// database would be far worse than refusing to start.
			return nil, fmt.Errorf("cannot use snapshot %s: %w", snapPath, err)
		}
		if err := st.ApplySnapshot(view); err != nil {
			return nil, fmt.Errorf("cannot apply snapshot %s: %w", snapPath, err)
		}
		report.SnapshotLoaded = true
		report.SnapshotSeq = view.Seq
		report.SnapshotKeys = len(view.Items)
		report.LastSeq = view.Seq
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat snapshot %s: %w", snapPath, err)
	}

	// 2. Journal replay.
	if m.jrnl != nil {
		scan, err := ScanJournal(m.JournalPath(), m.cfg.MaxRecordBytes, func(seq uint64, mut store.Mutation) error {
			if seq <= report.SnapshotSeq {
				report.JournalSkipped++
				return nil
			}
			if err := st.ValidateReplay(mut); err != nil {
				return err
			}
			if err := st.Replay(mut); err != nil {
				if errors.Is(err, store.ErrReplayOrphan) {
					report.JournalOrphans++
					report.Warnings = append(report.Warnings,
						fmt.Sprintf("journal record %d references a key that is not present and was skipped", seq))
					return nil
				}
				return err
			}
			report.JournalApplied++
			return nil
		})
		if err != nil {
			return nil, err
		}

		report.JournalRecords = scan.Records
		report.DiscardedBytes = scan.DiscardedBytes
		if scan.LastSeq > report.LastSeq {
			report.LastSeq = scan.LastSeq
		}

		if scan.DiscardedBytes > 0 {
			detail := scan.Corruption
			if detail == nil && scan.Truncated {
				detail = ErrTruncatedRecord
			}
			message := fmt.Sprintf("journal tail is damaged: %d bytes after offset %d were discarded (%v)",
				scan.DiscardedBytes, scan.ValidBytes, detail)
			if m.cfg.Repair == RepairFail {
				return nil, fmt.Errorf("%s: refusing to start (repair policy is %q); move or repair the file to continue",
					message, m.cfg.Repair)
			}
			dropped, err := TruncateJournal(m.JournalPath(), scan.ValidBytes)
			if err != nil {
				return nil, fmt.Errorf("%s, and truncation failed: %w", message, err)
			}
			report.TruncationRepaired = true
			report.DiscardedBytes = dropped
			report.Warnings = append(report.Warnings, message)
		}

		// Point the append offset at the end of the last valid record so that
		// no hole can be introduced after a truncation.
		if err := m.jrnl.Reposition(scan.ValidBytes, report.LastSeq); err != nil {
			return nil, err
		}
	} else {
		m.mu.Lock()
		if report.LastSeq < m.memSeq {
			report.LastSeq = m.memSeq
		}
		m.mu.Unlock()
	}

	switch {
	case report.TruncationRepaired:
		report.Status = "repaired"
	case report.SnapshotLoaded || report.JournalRecords > 0:
		report.Status = "ok"
	default:
		report.Status = "empty"
	}
	report.DurationMS = time.Since(start).Milliseconds()
	report.CompletedAtMS = time.Now().UnixMilli()

	m.mu.Lock()
	m.recovered = true
	m.mu.Unlock()

	m.recovery.Store(report)
	if m.log != nil {
		m.log.Info("recovery finished",
			"status", report.Status,
			"snapshot_loaded", report.SnapshotLoaded,
			"snapshot_keys", report.SnapshotKeys,
			"journal_records", report.JournalRecords,
			"journal_applied", report.JournalApplied,
			"journal_skipped", report.JournalSkipped,
			"discarded_bytes", report.DiscardedBytes,
			"last_seq", report.LastSeq)
		for _, w := range report.Warnings {
			m.log.Warn("recovery warning", "detail", w)
		}
	}
	return report, nil
}

// Start launches background durability work: the periodic fsync and the
// periodic snapshot.
func (m *Manager) Start() {
	m.mu.Lock()
	if m.started || m.closed {
		m.mu.Unlock()
		return
	}
	m.started = true
	m.stop = make(chan struct{})
	m.done = make(chan struct{})
	stop, done := m.stop, m.done
	interval := m.cfg.SnapshotInterval
	mode := m.cfg.Fsync
	m.mu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var lastSnapshot time.Time
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if mode == FsyncEverySec && m.jrnl != nil {
					if err := m.Fsync(); err != nil && m.log != nil {
						m.log.Error("periodic fsync failed", "err", err)
					}
				}
				if interval > 0 && time.Since(lastSnapshot) >= interval {
					lastSnapshot = time.Now()
					if err := m.SaveSnapshot(); err != nil && m.log != nil {
						m.log.Error("periodic snapshot failed", "err", err)
					}
				}
			}
		}
	}()
}

// Close stops background work, optionally writes a final snapshot, and closes
// the journal. It is safe to call more than once.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	started := m.started
	recovered := m.recovered
	stop, done := m.stop, m.done
	onShutdown := m.cfg.SnapshotOnShutdown
	m.mu.Unlock()

	if started {
		close(stop)
		<-done
	}

	var errs []error
	if onShutdown && recovered {
		if err := m.SaveSnapshot(); err != nil {
			errs = append(errs, err)
		}
	} else if onShutdown && m.log != nil {
		// Not an error: refusing to write is the correct behaviour, because
		// the existing files are evidence an operator has to inspect.
		m.log.Warn("skipping the shutdown snapshot: startup recovery never completed")
	}
	if m.jrnl != nil {
		if err := m.jrnl.Close(); err != nil {
			errs = append(errs, err)
			m.recordError(err)
		}
	}
	return errors.Join(errs...)
}

// Status returns a diagnostics snapshot.
func (m *Manager) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()

	st := Status{
		JournalEnabled:    m.jrnl != nil,
		SnapshotPath:      m.SnapshotPath(),
		LastSnapshotMS:    m.lastSnapshotMS,
		LastSnapshotSeq:   m.lastSnapshotSeq,
		LastSnapshotBytes: m.lastSnapshotBytes,
		SnapshotCount:     m.snapshotCount,
		LastError:         m.lastErr,
		Recovery:          m.recovery.Load(),
	}
	if m.jrnl != nil {
		st.Journal = m.jrnl.Status()
	}
	if _, err := os.Stat(st.SnapshotPath); err == nil {
		st.SnapshotExists = true
	}
	return st
}

// recordError remembers the most recent durability failure so that it shows up
// in diagnostics instead of only in the log.
func (m *Manager) recordError(err error) {
	if err == nil {
		return
	}
	m.mu.Lock()
	m.lastErr = strings.TrimSpace(err.Error())
	m.mu.Unlock()
}
