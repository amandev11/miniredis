package persistence

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/amandev11/miniredis/internal/clock"
	"github.com/amandev11/miniredis/internal/store"
)

// FsyncMode selects how aggressively the journal is flushed to stable storage.
type FsyncMode string

// Durability modes. The names match Redis's appendfsync values.
const (
	// FsyncAlways calls fsync before a write is acknowledged. A crash can lose
	// nothing that was acknowledged, at the cost of one fsync per write.
	FsyncAlways FsyncMode = "always"
	// FsyncEverySec calls fsync once per second from a background goroutine. A
	// process crash loses nothing (the data is in the page cache); a machine
	// crash or power loss can lose up to about one second of acknowledged
	// writes.
	FsyncEverySec FsyncMode = "everysec"
	// FsyncNo never calls fsync explicitly. Writes reach the page cache and are
	// flushed by the operating system, so a machine crash can lose an
	// unbounded amount of recent data.
	FsyncNo FsyncMode = "no"
)

// ParseFsyncMode validates a durability mode name.
func ParseFsyncMode(s string) (FsyncMode, error) {
	switch FsyncMode(s) {
	case FsyncAlways, FsyncEverySec, FsyncNo:
		return FsyncMode(s), nil
	default:
		return "", fmt.Errorf("unknown fsync mode %q (want always, everysec or no)", s)
	}
}

// JournalStatus describes the journal's health for diagnostics.
type JournalStatus struct {
	Path           string
	Enabled        bool
	FsyncMode      FsyncMode
	SizeBytes      int64
	Seq            uint64
	Appends        uint64
	BytesWritten   uint64
	Fsyncs         uint64
	LastFsyncMS    int64
	WriteFailures  uint64
	Poisoned       bool
	PoisonedReason string
}

// Journal is an append-only file of length-delimited mutation records.
//
// The journal is safe for concurrent use; Appends are serialised by mu, and
// the store already serialises them anyway by holding its write lock.
type Journal struct {
	mu   sync.Mutex
	f    *os.File
	path string

	mode      FsyncMode
	maxRecord int
	clk       clock.Clock

	seq  uint64
	size int64

	appends      uint64
	bytesWritten uint64
	syncs        uint64
	lastSyncMS   int64
	writeErrors  uint64

	// poison holds the first unrecoverable write failure. Once set, the
	// journal refuses further appends: continuing to write after a partial
	// write could interleave records and corrupt the middle of the file, and
	// there is no safe way to continue without operator intervention.
	poison error
}

// OpenJournal opens or creates the journal at path.
//
// A newly created file gets the fixed header. An existing file is validated
// before it is used, and the file offset is left at the end, ready to append.
// The caller is responsible for running recovery (Manager.Recover) before
// appending to a journal that already contained records.
func OpenJournal(path string, mode FsyncMode, maxRecord int, clk clock.Clock) (*Journal, error) {
	if clk == nil {
		clk = clock.System{}
	}
	if maxRecord <= 0 {
		maxRecord = DefaultMaxRecordBytes
	}

	if err := ensureDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	existed, err := ensureRegularFile(path)
	if err != nil {
		return nil, err
	}

	// The file is created with owner-only permissions; an existing file keeps
	// its permissions, which keeps recovery from silently loosening them.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open journal %s: %w", path, err)
	}

	j := &Journal{f: f, path: path, mode: mode, maxRecord: maxRecord, clk: clk}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat journal %s: %w", path, err)
	}
	if !fi.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("%s is not a regular file", path)
	}

	if !existed || fi.Size() == 0 {
		if _, err := f.WriteAt(journalHeader(), 0); err != nil {
			f.Close()
			return nil, fmt.Errorf("write journal header %s: %w", path, err)
		}
		if err := f.Truncate(journalHeaderSize); err != nil {
			f.Close()
			return nil, fmt.Errorf("size journal %s: %w", path, err)
		}
	} else {
		header := make([]byte, journalHeaderSize)
		if _, err := io.ReadFull(io.NewSectionReader(f, 0, journalHeaderSize), header); err != nil {
			f.Close()
			return nil, fmt.Errorf("read journal header %s: %w", path, err)
		}
		if err := validateJournalHeader(header); err != nil {
			f.Close()
			return nil, fmt.Errorf("%s: %w", path, err)
		}
	}

	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("seek journal %s: %w", path, err)
	}
	if size < journalHeaderSize {
		f.Close()
		return nil, fmt.Errorf("%s: journal is %d bytes, shorter than its %d byte header", path, size, journalHeaderSize)
	}
	j.size = size
	return j, nil
}

// Append writes one record and returns its sequence number.
//
// Failure policy:
//
//   - A write that fails before the whole frame reaches the file returns a
//     plain error. The record is definitely incomplete, so the caller must not
//     apply the mutation.
//   - A failure that leaves the complete frame in the file, or an fsync
//     failure, returns *store.AmbiguousAppendError: the record may be replayed
//     after a restart, so the caller should apply the mutation and report the
//     failure.
//
// In every failure case the journal is poisoned and refuses further appends.
func (j *Journal) Append(m store.Mutation) (uint64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.f == nil {
		return 0, fmt.Errorf("journal %s is closed", j.path)
	}
	if j.poison != nil {
		return 0, fmt.Errorf("%w: %v", ErrJournalUnusable, j.poison)
	}

	seq := j.seq + 1
	payload, err := marshalRecord(seq, m)
	if err != nil {
		return 0, err
	}
	if len(payload) > j.maxRecord {
		return 0, fmt.Errorf("%w: %d bytes (maximum %d)", ErrRecordTooLarge, len(payload), j.maxRecord)
	}

	frame := make([]byte, frameHeaderSize+len(payload))
	putFrameHeader(frame[:frameHeaderSize], payload)
	copy(frame[frameHeaderSize:], payload)

	n, werr := j.f.Write(frame)
	j.size += int64(n)
	j.bytesWritten += uint64(n)
	if werr != nil {
		j.writeErrors++
		j.poison = werr
		if n == len(frame) {
			// The whole frame is in the file even though the call failed;
			// recovery can replay it.
			j.seq = seq
			return seq, &store.AmbiguousAppendError{Err: werr}
		}
		return 0, fmt.Errorf("journal append failed after %d of %d bytes: %w", n, len(frame), werr)
	}

	if j.mode == FsyncAlways {
		if err := j.syncLocked(); err != nil {
			j.writeErrors++
			j.poison = err
			j.seq = seq
			return seq, &store.AmbiguousAppendError{Err: err}
		}
	}

	j.seq = seq
	j.appends++
	return seq, nil
}

// Fsync flushes buffered writes to stable storage.
func (j *Journal) Fsync() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.syncLocked()
}

func (j *Journal) syncLocked() error {
	if j.f == nil {
		return fmt.Errorf("journal %s is closed", j.path)
	}
	if err := j.f.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", j.path, err)
	}
	j.syncs++
	j.lastSyncMS = j.clk.NowMS()
	return nil
}

// Seq returns the sequence number of the last appended record.
func (j *Journal) Seq() uint64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.seq
}

// Reposition points the append offset at offset. Recovery uses it after a
// truncation so that later appends continue directly after the last valid
// record instead of leaving a hole in the file.
func (j *Journal) Reposition(offset int64, seq uint64) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.f == nil {
		return fmt.Errorf("journal %s is closed", j.path)
	}
	if offset < journalHeaderSize {
		offset = journalHeaderSize
	}
	if _, err := j.f.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek journal %s to %d: %w", j.path, offset, err)
	}
	j.size = offset
	if seq > j.seq {
		j.seq = seq
	}
	return nil
}

// SetSeq forces the sequence counter, which recovery uses to continue after
// the highest sequence number it found.
func (j *Journal) SetSeq(seq uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if seq > j.seq {
		j.seq = seq
	}
}

// Path returns the journal's filesystem path.
func (j *Journal) Path() string { return j.path }

// Size returns the journal's current size in bytes.
func (j *Journal) Size() int64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.size
}

// Mode returns the configured durability mode.
func (j *Journal) Mode() FsyncMode { return j.mode }

// Err returns the failure that poisoned the journal, if any.
func (j *Journal) Err() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.poison
}

// Sync flushes and closes the journal file.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.f == nil {
		return nil
	}
	var errs []error
	// Only attempt a final flush when the journal is still healthy; a poisoned
	// journal has already reported its failure to the caller.
	if j.poison == nil {
		if err := j.f.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("final fsync %s: %w", j.path, err))
		}
	}
	if err := j.f.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close %s: %w", j.path, err))
	}
	j.f = nil
	return errors.Join(errs...)
}

// Status returns a snapshot of the journal's counters.
func (j *Journal) Status() JournalStatus {
	j.mu.Lock()
	defer j.mu.Unlock()
	st := JournalStatus{
		Path:          j.path,
		Enabled:       true,
		FsyncMode:     j.mode,
		SizeBytes:     j.size,
		Seq:           j.seq,
		Appends:       j.appends,
		BytesWritten:  j.bytesWritten,
		Fsyncs:        j.syncs,
		LastFsyncMS:   j.lastSyncMS,
		WriteFailures: j.writeErrors,
	}
	if j.poison != nil {
		st.Poisoned = true
		st.PoisonedReason = j.poison.Error()
	}
	return st
}

func putFrameHeader(dst []byte, payload []byte) {
	putUint32(dst[0:4], uint32(len(payload)))
	putUint32(dst[4:8], crc32Checksum(payload))
}

// ---------------------------------------------------------------------------
// Journal scanning and repair
// ---------------------------------------------------------------------------

// ScanReport describes what a journal scan found.
type ScanReport struct {
	// Records is the number of complete, valid records read.
	Records int
	// LastSeq is the highest sequence number seen.
	LastSeq uint64
	// ValidBytes is the offset just after the last valid record.
	ValidBytes int64
	// DiscardedBytes counts the trailing bytes that were not part of a valid
	// record: a truncated record, a failed checksum, or an unknown operation.
	DiscardedBytes int64
	// Truncated reports that the file ended mid-record, which is the expected
	// outcome of a crash during an append.
	Truncated bool
	// Corruption describes the first invalid record that was not a simple
	// truncation, if any.
	Corruption error
}

// TruncationFailed reports whether the scan stopped before the end of the file
// because the tail was torn or damaged.
func (r *ScanReport) TruncationFailed() bool { return r.Truncated || r.Corruption != nil }

// Scan reads every complete record in the journal in order and calls apply for
// each one.
//
// The scan stops at the first record that is not fully readable and reports it
// instead of guessing: a record that fails validation makes everything after
// it untrustworthy, because there is no way to tell a torn write from edited
// or reordered bytes. No valid record before that point is skipped.
func ScanJournal(path string, maxRecord int, apply func(seq uint64, m store.Mutation) error) (*ScanReport, error) {
	if maxRecord <= 0 {
		maxRecord = DefaultMaxRecordBytes
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	report := &ScanReport{ValidBytes: journalHeaderSize}
	size := fi.Size()

	header := make([]byte, journalHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, fmt.Errorf("read journal header %s: %w", path, err)
	}
	if err := validateJournalHeader(header); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	var offset int64 = journalHeaderSize
	for offset < size {
		remaining := size - offset
		if remaining < frameHeaderSize {
			report.DiscardedBytes = remaining
			report.Truncated = true
			break
		}
		frameHeader := make([]byte, frameHeaderSize)
		if _, err := io.ReadFull(f, frameHeader); err != nil {
			if isShortRead(err) {
				report.DiscardedBytes = remaining
				report.Truncated = true
				break
			}
			return nil, fmt.Errorf("read journal %s at offset %d: %w", path, offset, err)
		}
		length := int64(getUint32(frameHeader[0:4]))
		wantCRC := getUint32(frameHeader[4:8])

		if length < minPayloadSize || length > int64(maxRecord) {
			report.DiscardedBytes = remaining
			report.Corruption = fmt.Errorf("%w: record at offset %d declares an implausible length of %d bytes",
				ErrCorruptRecord, offset, length)
			break
		}
		if remaining-frameHeaderSize < length {
			report.DiscardedBytes = remaining
			report.Truncated = true
			break
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			if isShortRead(err) {
				report.DiscardedBytes = remaining
				report.Truncated = true
				break
			}
			return nil, fmt.Errorf("read journal %s at offset %d: %w", path, offset, err)
		}
		if got := crc32Checksum(payload); got != wantCRC {
			report.DiscardedBytes = remaining
			report.Corruption = fmt.Errorf("%w: checksum mismatch at offset %d (stored 0x%08x, computed 0x%08x)",
				ErrCorruptRecord, offset, wantCRC, got)
			break
		}
		seq, m, err := unmarshalRecord(payload)
		if err != nil {
			report.DiscardedBytes = remaining
			report.Corruption = fmt.Errorf("journal record at offset %d: %w", offset, err)
			break
		}
		if err := apply(seq, m); err != nil {
			return report, fmt.Errorf("applying journal record %d at offset %d: %w", seq, offset, err)
		}

		report.Records++
		if seq > report.LastSeq {
			report.LastSeq = seq
		}
		offset += frameHeaderSize + length
		report.ValidBytes = offset
	}
	return report, nil
}

// TruncateJournal cuts the journal down to the last valid record.
//
// Truncation is only ever used for a damaged tail. Every discarded byte is
// reported to the caller and surfaced through INFO, so an operator always sees
// that data was dropped instead of a server that looks healthy.
func TruncateJournal(path string, offset int64) (int64, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if offset > fi.Size() {
		return 0, fmt.Errorf("cannot grow journal %s from %d to %d bytes", path, fi.Size(), offset)
	}
	if err := f.Truncate(offset); err != nil {
		return 0, fmt.Errorf("truncate journal %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("fsync truncated journal %s: %w", path, err)
	}
	return fi.Size() - offset, nil
}

func isShortRead(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
