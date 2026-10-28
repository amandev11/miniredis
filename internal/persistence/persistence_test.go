package persistence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amandev11/miniredis/internal/clock"
	"github.com/amandev11/miniredis/internal/store"
)

const startMS = int64(1_700_000_000_000)

// testLimits bounds record and field sizes in tests so that oversized cases are
// cheap to construct.
const testMaxRecord = 1 << 16

func fv(name, value string) store.FieldValue {
	return store.FieldValue{Name: name, Value: []byte(value)}
}

// openManager prepares a manager over a temporary directory.
func openManager(t *testing.T, cfg Config) *Manager {
	t.Helper()
	if cfg.Dir == "" {
		cfg.Dir = t.TempDir()
	}
	if cfg.MaxRecordBytes == 0 {
		cfg.MaxRecordBytes = testMaxRecord
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.NewManual(startMS)
	}
	m, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// newEngine wires a store to a manager the way the server does. A clock passed
// in cfg is shared with the caller, so a test can move time forward while the
// server is "down".
func newEngine(t *testing.T, cfg Config) (*Manager, *store.Store, *clock.Manual) {
	t.Helper()
	clk, _ := cfg.Clock.(*clock.Manual)
	if clk == nil {
		clk = clock.NewManual(startMS)
	}
	cfg.Clock = clk
	m := openManager(t, cfg)
	st := store.New(clk, store.Options{Recorder: m, MaxValueBytes: 1 << 20, MaxHashFields: 1 << 16})
	m.Attach(st)
	return m, st, clk
}

func mustSet(t *testing.T, s *store.Store, key, value string) {
	t.Helper()
	applied, err := s.Set(key, []byte(value), store.SetOptions{})
	if err != nil {
		t.Fatalf("Set(%q): %v", key, err)
	}
	if !applied {
		t.Fatalf("Set(%q) was not applied", key)
	}
}

// ---------------------------------------------------------------------------
// Record encoding
// ---------------------------------------------------------------------------

func TestRecordRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		op   store.Mutation
		seq  uint64
	}{
		{
			name: "set with a binary value and deadline",
			op: store.Mutation{
				Op: store.OpSet, Key: "key\x00\r\n", ExpireAt: startMS + 5000,
				Value: []byte{0x00, 0xff, '\r', '\n'},
			},
			seq: 1,
		},
		{
			name: "set with no expiry",
			op:   store.Mutation{Op: store.OpSet, Key: "k", Value: []byte("v")},
			seq:  42,
		},
		{
			name: "mset",
			op: store.Mutation{Op: store.OpMSet, Pairs: []store.FieldValue{
				fv("a", "1"), fv("b", "2"), fv("binary\x00", "x\r\n"),
			}},
			seq: 7,
		},
		{
			name: "del",
			op:   store.Mutation{Op: store.OpDel, Keys: []string{"a", "b", "c"}},
			seq:  9,
		},
		{
			name: "append",
			op:   store.Mutation{Op: store.OpAppend, Key: "k", ExpireAt: startMS + 1, Value: []byte("more")},
			seq:  11,
		},
		{
			name: "hash set",
			op: store.Mutation{Op: store.OpHashSet, Key: "h", ExpireAt: startMS + 99,
				Pairs: []store.FieldValue{fv("f1", "v1"), fv("f2", "v2")}},
			seq: 13,
		},
		{
			name: "hash del",
			op:   store.Mutation{Op: store.OpHashDel, Key: "h", Fields: []string{"f1", "f2"}},
			seq:  15,
		},
		{
			name: "expire",
			op:   store.Mutation{Op: store.OpExpire, Key: "k", ExpireAt: startMS + 10},
			seq:  17,
		},
		{
			name: "persist clears the deadline",
			op:   store.Mutation{Op: store.OpExpire, Key: "k", ExpireAt: 0},
			seq:  19,
		},
		{
			name: "flush",
			op:   store.Mutation{Op: store.OpFlush},
			seq:  21,
		},
		{
			name: "empty value and empty key parts",
			op:   store.Mutation{Op: store.OpSet, Key: "", Value: nil},
			seq:  23,
		},
		{
			name: "max uint64 sequence",
			op:   store.Mutation{Op: store.OpSet, Key: "k", Value: []byte("v")},
			seq:  ^uint64(0),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := marshalRecord(tc.seq, tc.op)
			if err != nil {
				t.Fatalf("marshalRecord: %v", err)
			}
			seq, got, err := unmarshalRecord(payload)
			if err != nil {
				t.Fatalf("unmarshalRecord: %v", err)
			}
			if seq != tc.seq {
				t.Fatalf("seq = %d, want %d", seq, tc.seq)
			}
			if got.Op != tc.op.Op || got.Key != tc.op.Key || got.ExpireAt != tc.op.ExpireAt {
				t.Fatalf("got %+v, want %+v", got, tc.op)
			}
			if !bytes.Equal(got.Value, tc.op.Value) {
				t.Fatalf("value = %q, want %q", got.Value, tc.op.Value)
			}
			if len(got.Keys) != len(tc.op.Keys) {
				t.Fatalf("keys = %v, want %v", got.Keys, tc.op.Keys)
			}
			if len(got.Fields) != len(tc.op.Fields) {
				t.Fatalf("fields = %v, want %v", got.Fields, tc.op.Fields)
			}
			if len(got.Pairs) != len(tc.op.Pairs) {
				t.Fatalf("pairs = %v, want %v", got.Pairs, tc.op.Pairs)
			}
			for i := range tc.op.Pairs {
				if got.Pairs[i].Name != tc.op.Pairs[i].Name || !bytes.Equal(got.Pairs[i].Value, tc.op.Pairs[i].Value) {
					t.Fatalf("pair %d = %v, want %v", i, got.Pairs[i], tc.op.Pairs[i])
				}
			}
		})
	}
}

func TestUnmarshalRejectsDamagedRecords(t *testing.T) {
	valid, err := marshalRecord(5, store.Mutation{Op: store.OpSet, Key: "key", Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string][]byte{
		"empty":                 {},
		"truncated before op":   valid[:8],
		"truncated inside body": valid[:len(valid)-2],
		"trailing bytes":        append(append([]byte(nil), valid...), 0xff),
		"unknown operation":     {5, 0, 0, 0, 0, 0, 0, 0, 200, 0, 0, 0, 0},
	}

	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := unmarshalRecord(payload); err == nil {
				t.Fatalf("payload %x was accepted", payload)
			} else if !errors.Is(err, ErrCorruptRecord) && !strings.Contains(err.Error(), "record") {
				t.Fatalf("error %v should identify a corrupt record", err)
			}
		})
	}
}

func TestUnmarshalRejectsImpossibleCounts(t *testing.T) {
	// A DEL record that claims 4 billion keys but carries almost no bytes.
	payload := make([]byte, 0, 16)
	payload = binary.LittleEndian.AppendUint64(payload, 1)
	payload = append(payload, byte(store.OpDel))
	payload = binary.LittleEndian.AppendUint32(payload, 0xffffffff)
	if _, _, err := unmarshalRecord(payload); err == nil {
		t.Fatal("an impossible key count was accepted")
	}
}

func TestMarshalRejectsUnknownOperation(t *testing.T) {
	if _, err := marshalRecord(1, store.Mutation{Op: store.Op(99)}); err == nil {
		t.Fatal("unknown operation was marshalled")
	}
}

// ---------------------------------------------------------------------------
// Journal file handling
// ---------------------------------------------------------------------------

func TestJournalHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.mrj")

	j, err := OpenJournal(path, FsyncNo, testMaxRecord, clock.NewManual(startMS))
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != journalHeaderSize {
		t.Fatalf("new journal is %d bytes, want a %d byte header", len(raw), journalHeaderSize)
	}
	if err := validateJournalHeader(raw); err != nil {
		t.Fatalf("header does not validate: %v", err)
	}
	if string(raw[0:8]) != journalMagic {
		t.Fatalf("magic = %q", raw[0:8])
	}

	// Permissions must not expose stored data to other accounts.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perms := fi.Mode().Perm(); perms&0o077 != 0 {
		t.Fatalf("journal permissions are %o, want no group or other access", perms)
	}
}

func TestOpenJournalRejectsBadHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.mrj")
	if err := os.WriteFile(path, []byte("NOTAJOURNALFILE..."), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJournal(path, FsyncNo, testMaxRecord, nil); !errors.Is(err, ErrBadJournalHeader) {
		t.Fatalf("got %v, want ErrBadJournalHeader", err)
	}
}

func TestOpenJournalRejectsUnsupportedVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "v9.mrj")
	header := journalHeader()
	binary.LittleEndian.PutUint16(header[8:10], 9)
	if err := os.WriteFile(path, header, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJournal(path, FsyncNo, testMaxRecord, nil); !errors.Is(err, ErrBadJournalHeader) {
		t.Fatalf("got %v, want ErrBadJournalHeader", err)
	}
}

func TestOpenRefusesSymlinkedDataFiles(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.mrj")
	if err := os.WriteFile(target, journalHeader(), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.mrj")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	_, err := OpenJournal(link, FsyncNo, testMaxRecord, nil)
	if err == nil {
		t.Fatal("a symlinked journal was accepted")
	}
	if !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("error %v should explain the symlink refusal", err)
	}
}

func TestAppendAndScan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.mrj")
	j, err := OpenJournal(path, FsyncNo, testMaxRecord, clock.NewManual(startMS))
	if err != nil {
		t.Fatal(err)
	}

	mutations := []store.Mutation{
		{Op: store.OpSet, Key: "a", Value: []byte("1")},
		{Op: store.OpHashSet, Key: "h", Pairs: []store.FieldValue{fv("f", "v")}},
		{Op: store.OpDel, Keys: []string{"a"}},
		{Op: store.OpFlush},
	}
	var wantSeqs []uint64
	for i, m := range mutations {
		seq, err := j.Append(m)
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if seq != uint64(i+1) {
			t.Fatalf("Append %d returned seq %d, want %d", i, seq, i+1)
		}
		wantSeqs = append(wantSeqs, seq)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	var got []store.Mutation
	var gotSeqs []uint64
	report, err := ScanJournal(path, testMaxRecord, func(seq uint64, m store.Mutation) error {
		gotSeqs = append(gotSeqs, seq)
		got = append(got, m)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanJournal: %v", err)
	}
	if report.Records != len(mutations) {
		t.Fatalf("read %d records, want %d", report.Records, len(mutations))
	}
	if report.DiscardedBytes != 0 || report.TruncationFailed() {
		t.Fatalf("clean journal reported damage: %+v", report)
	}
	if report.LastSeq != uint64(len(mutations)) {
		t.Fatalf("LastSeq = %d, want %d", report.LastSeq, len(mutations))
	}
	if fmt.Sprint(gotSeqs) != fmt.Sprint(wantSeqs) {
		t.Fatalf("sequence numbers = %v, want %v", gotSeqs, wantSeqs)
	}
	for i := range mutations {
		if got[i].Op != mutations[i].Op {
			t.Errorf("record %d op = %v, want %v", i, got[i].Op, mutations[i].Op)
		}
	}
}

func TestAppendAfterCloseFails(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenJournal(filepath.Join(dir, "test.mrj"), FsyncNo, testMaxRecord, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Append(store.Mutation{Op: store.OpFlush}); err == nil {
		t.Fatal("Append after Close succeeded")
	}
	if err := j.Fsync(); err == nil {
		t.Fatal("Fsync after Close succeeded")
	}
}

func TestAppendRejectsOversizedRecord(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenJournal(filepath.Join(dir, "test.mrj"), FsyncNo, 128, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	_, err = j.Append(store.Mutation{Op: store.OpSet, Key: "k", Value: bytes.Repeat([]byte("x"), 256)})
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("got %v, want ErrRecordTooLarge", err)
	}
	// A rejected record must not consume a sequence number.
	if seq := j.Seq(); seq != 0 {
		t.Fatalf("Seq() = %d after a rejected append, want 0", seq)
	}
}

func TestPoisonedJournalRefusesAppends(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenJournal(filepath.Join(dir, "test.mrj"), FsyncAlways, testMaxRecord, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	// Simulate the state after an unrecoverable write failure.
	j.mu.Lock()
	j.poison = errors.New("injected fsync failure")
	j.mu.Unlock()

	_, err = j.Append(store.Mutation{Op: store.OpFlush})
	if !errors.Is(err, ErrJournalUnusable) {
		t.Fatalf("got %v, want ErrJournalUnusable", err)
	}
	if st := j.Status(); !st.Poisoned || st.WriteFailures != 0 {
		t.Fatalf("status does not report poisoning: %+v", st)
	}
}

func TestJournalFsyncCounters(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenJournal(filepath.Join(dir, "test.mrj"), FsyncAlways, testMaxRecord, clock.NewManual(startMS))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	for i := 0; i < 3; i++ {
		if _, err := j.Append(store.Mutation{Op: store.OpSet, Key: "k", Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	st := j.Status()
	if st.Appends != 3 {
		t.Fatalf("Appends = %d, want 3", st.Appends)
	}
	if st.Fsyncs != 3 {
		t.Fatalf("Fsyncs = %d, want 3 in always mode", st.Fsyncs)
	}
	if st.LastFsyncMS != startMS {
		t.Fatalf("LastFsyncMS = %d, want %d", st.LastFsyncMS, startMS)
	}
	if st.SizeBytes <= journalHeaderSize {
		t.Fatalf("SizeBytes = %d, want more than the header", st.SizeBytes)
	}
}

func TestFsyncNoModeDoesNotSyncPerWrite(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenJournal(filepath.Join(dir, "test.mrj"), FsyncNo, testMaxRecord, clock.NewManual(startMS))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	if _, err := j.Append(store.Mutation{Op: store.OpFlush}); err != nil {
		t.Fatal(err)
	}
	if st := j.Status(); st.Fsyncs != 0 {
		t.Fatalf("Fsyncs = %d, want 0 in no mode", st.Fsyncs)
	}
	// Data must still reach the file: restart recovery reads the page cache.
	if st := j.Status(); st.BytesWritten != uint64(st.SizeBytes-journalHeaderSize) {
		t.Fatalf("BytesWritten = %d, size = %d, want them to agree", st.BytesWritten, st.SizeBytes)
	}
}

// ---------------------------------------------------------------------------
// Damage handling
// ---------------------------------------------------------------------------

func TestScanDetectsChecksumCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.mrj")
	j, err := OpenJournal(path, FsyncNo, testMaxRecord, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Append(store.Mutation{Op: store.OpSet, Key: "good", Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Append(store.Mutation{Op: store.OpSet, Key: "bad", Value: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip one bit inside the second record's payload.
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	seen := 0
	report, err := ScanJournal(path, testMaxRecord, func(uint64, store.Mutation) error {
		seen++
		return nil
	})
	if err != nil {
		t.Fatalf("ScanJournal: %v", err)
	}
	if seen != 1 {
		t.Fatalf("scanned %d records, want 1: the damaged record must not be applied", seen)
	}
	if report.Corruption == nil {
		t.Fatal("checksum corruption was not reported")
	}
	if !errors.Is(report.Corruption, ErrCorruptRecord) {
		t.Fatalf("error %v should wrap ErrCorruptRecord", report.Corruption)
	}
	if report.DiscardedBytes <= 0 {
		t.Fatalf("DiscardedBytes = %d, want the damaged bytes to be counted", report.DiscardedBytes)
	}
}

func TestScanDetectsTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.mrj")
	j, err := OpenJournal(path, FsyncNo, testMaxRecord, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := j.Append(store.Mutation{Op: store.OpSet, Key: fmt.Sprintf("k%d", i), Value: []byte("value")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// All three records have the same shape, so the record length follows from
	// the file size.
	recordLen := (len(raw) - journalHeaderSize) / 3
	if recordLen <= 12 {
		t.Fatalf("unexpected record length %d", recordLen)
	}
	wantValid := int64(journalHeaderSize + 2*recordLen)

	for _, cut := range []int{1, 4, 8, 9, 12} {
		t.Run(fmt.Sprintf("cut %d bytes", cut), func(t *testing.T) {
			trimmed := raw[:len(raw)-cut]
			if err := os.WriteFile(path, trimmed, 0o600); err != nil {
				t.Fatal(err)
			}
			seen := 0
			report, err := ScanJournal(path, testMaxRecord, func(uint64, store.Mutation) error {
				seen++
				return nil
			})
			if err != nil {
				t.Fatalf("ScanJournal: %v", err)
			}
			if seen != 2 {
				t.Fatalf("applied %d records, want the 2 intact ones", seen)
			}
			if report.ValidBytes != wantValid {
				t.Fatalf("ValidBytes = %d, want %d", report.ValidBytes, wantValid)
			}
			wantDiscarded := int64(len(trimmed)) - wantValid
			if report.DiscardedBytes != wantDiscarded {
				t.Fatalf("DiscardedBytes = %d, want %d", report.DiscardedBytes, wantDiscarded)
			}
			if report.Corruption != nil {
				t.Fatalf("a torn tail was reported as corruption: %v", report.Corruption)
			}
		})
	}
}

func TestScanRejectsImplausibleRecordLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.mrj")
	j, err := OpenJournal(path, FsyncNo, testMaxRecord, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a frame header that claims a 3 GiB record.
	corrupt := journalHeader()
	corrupt = binary.LittleEndian.AppendUint32(corrupt, 3<<30)
	corrupt = binary.LittleEndian.AppendUint32(corrupt, 0)
	if err := os.WriteFile(path, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := ScanJournal(path, testMaxRecord, func(uint64, store.Mutation) error { return nil })
	if err != nil {
		t.Fatalf("ScanJournal: %v", err)
	}
	if report.Corruption == nil {
		t.Fatal("an implausible record length was not reported")
	}
	if report.Records != 0 {
		t.Fatalf("Records = %d, want 0", report.Records)
	}
}

func TestTruncateJournal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.mrj")
	j, err := OpenJournal(path, FsyncNo, testMaxRecord, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Append(store.Mutation{Op: store.OpSet, Key: "a", Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	offset := j.Size()
	if _, err := j.Append(store.Mutation{Op: store.OpSet, Key: "b", Value: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	dropped, err := TruncateJournal(path, offset)
	if err != nil {
		t.Fatal(err)
	}
	if dropped <= 0 {
		t.Fatalf("dropped %d bytes, want a positive count", dropped)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != offset {
		t.Fatalf("size = %d, want %d", fi.Size(), offset)
	}

	report, err := ScanJournal(path, testMaxRecord, func(uint64, store.Mutation) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if report.Records != 1 || report.DiscardedBytes != 0 {
		t.Fatalf("report = %+v, want exactly one clean record", report)
	}
}

func TestTruncateJournalRefusesToGrow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.mrj")
	if err := os.WriteFile(path, journalHeader(), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := TruncateJournal(path, 1<<20); err == nil {
		t.Fatal("truncation was allowed to grow the file")
	}
}

// ---------------------------------------------------------------------------
// Recovery through the manager
// ---------------------------------------------------------------------------

func TestRecoverEmptyDirectory(t *testing.T) {
	m, st, _ := newEngine(t, Config{EnableJournal: true})
	report, err := m.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "empty" {
		t.Fatalf("status = %q, want empty", report.Status)
	}
	if st.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", st.Len())
	}
}

func TestJournalSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	// First "process": write data and shut down.
	m1, st1, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, Fsync: FsyncAlways})
	if _, err := m1.Recover(); err != nil {
		t.Fatal(err)
	}
	mustSet(t, st1, "str", "value")
	if _, err := st1.HashSet("hash", []store.FieldValue{fv("field", "value")}); err != nil {
		t.Fatal(err)
	}
	if _, err := st1.Set("volatile", []byte("v"), store.SetOptions{ExpireAt: startMS + 60_000}); err != nil {
		t.Fatal(err)
	}
	if _, err := st1.Incr("counter", 7); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	// Second "process": recover from the same directory.
	m2, st2, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, Fsync: FsyncAlways})
	report, err := m2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "ok" {
		t.Fatalf("status = %q, want ok (%v)", report.Status, report.Warnings)
	}
	if report.JournalApplied == 0 {
		t.Fatal("no journal records were applied")
	}

	got, ok, err := st2.Get("str")
	if err != nil || !ok || string(got) != "value" {
		t.Fatalf("string was not recovered: (%q, %v, %v)", got, ok, err)
	}
	if v, ok, _ := st2.HashGet("hash", "field"); !ok || string(v) != "value" {
		t.Fatalf("hash field was not recovered: (%q, %v)", v, ok)
	}
	if v, ok, _ := st2.Get("counter"); !ok || string(v) != "7" {
		t.Fatalf("counter was not recovered: (%q, %v)", v, ok)
	}
	if remaining, state := st2.TTL("volatile"); state != store.TTLRemaining || remaining != 60_000 {
		t.Fatalf("deadline was not restored: (%d, %v)", remaining, state)
	}
}

func TestRecoveryAppliesDeletionsAndFlush(t *testing.T) {
	dir := t.TempDir()
	m1, st1, _ := newEngine(t, Config{Dir: dir, EnableJournal: true})
	mustSet(t, st1, "kept", "1")
	mustSet(t, st1, "deleted", "2")
	mustSet(t, st1, "flushed", "3")
	if _, err := st1.Delete("deleted"); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	m2, st2, _ := newEngine(t, Config{Dir: dir, EnableJournal: true})
	if _, err := m2.Recover(); err != nil {
		t.Fatal(err)
	}
	if st2.Exists("kept") != 1 {
		t.Fatal("kept key is missing")
	}
	if st2.Exists("deleted") != 0 {
		t.Fatal("deleted key came back")
	}

	// Flush the recovered store, then recover again: the FLUSH record must
	// clear the keyspace brought back by the earlier records.
	if err := st2.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := m2.Close(); err != nil {
		t.Fatal(err)
	}

	m3, st3, _ := newEngine(t, Config{Dir: dir, EnableJournal: true})
	report, err := m3.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if st3.Len() != 0 {
		t.Fatalf("Len() = %d after replaying FLUSH, want 0 (report %+v)", st3.Len(), report)
	}
}

func TestRecoverBinaryData(t *testing.T) {
	dir := t.TempDir()
	key := "key\x00\xff\r\nwith binary"
	value := []byte{0x00, 0x01, 0xff, '\r', '\n', 0x7f, 0x80}

	m1, st1, _ := newEngine(t, Config{Dir: dir, EnableJournal: true})
	if _, err := st1.Set(key, value, store.SetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := st1.HashSet("h", []store.FieldValue{{Name: "f\x00", Value: value}}); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	m2, st2, _ := newEngine(t, Config{Dir: dir, EnableJournal: true})
	if _, err := m2.Recover(); err != nil {
		t.Fatal(err)
	}
	got, ok, err := st2.Get(key)
	if err != nil || !ok {
		t.Fatalf("binary key was not recovered: (%v, %v)", ok, err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value = %x, want %x", got, value)
	}
	hv, ok, _ := st2.HashGet("h", "f\x00")
	if !ok || !bytes.Equal(hv, value) {
		t.Fatalf("binary hash field = %x, want %x", hv, value)
	}
}

func TestRecoveryDropsKeysExpiredWhileDown(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewManual(startMS)

	m1, st1, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, Clock: clk})
	if _, err := st1.Set("short", []byte("v"), store.SetOptions{ExpireAt: startMS + 1_000}); err != nil {
		t.Fatal(err)
	}
	if _, err := st1.Set("long", []byte("v"), store.SetOptions{ExpireAt: startMS + 1_000_000}); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	// The server is down for a while.
	clk.Advance(10 * time.Second)

	m2, st2, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, Clock: clk})
	if _, err := m2.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st2.Get("short"); ok {
		t.Fatal("a key whose deadline passed while the server was down came back")
	}
	if _, ok, _ := st2.Get("long"); !ok {
		t.Fatal("a key with time left was lost")
	}
	if st2.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", st2.Len())
	}
}

func TestRecoveryReportsTruncatedTailAndRepairs(t *testing.T) {
	dir := t.TempDir()
	m1, st1, _ := newEngine(t, Config{Dir: dir, EnableJournal: true})
	mustSet(t, st1, "a", "1")
	mustSet(t, st1, "b", "2")
	mustSet(t, st1, "c", "3")
	journalPath := m1.JournalPath()
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash during the last append.
	raw, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	recordLen := int64(len(raw)-journalHeaderSize) / 3
	if err := os.WriteFile(journalPath, raw[:len(raw)-3], 0o600); err != nil {
		t.Fatal(err)
	}

	m2, st2, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, Repair: RepairTruncate})
	report, err := m2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "repaired" {
		t.Fatalf("status = %q, want repaired", report.Status)
	}
	if want := recordLen - 3; report.DiscardedBytes != want {
		t.Fatalf("DiscardedBytes = %d, want %d", report.DiscardedBytes, want)
	}
	if len(report.Warnings) == 0 {
		t.Fatal("repair was not reported as a warning")
	}
	if st2.Exists("a", "b", "c") != 2 {
		t.Fatalf("recovered %d of the 3 keys, want the 2 complete ones", st2.Exists("a", "b", "c"))
	}

	// Appending after the repair must not leave a hole: the new record must be
	// readable straight after the last valid one.
	if _, err := st2.Set("after", []byte("repair"), store.SetOptions{}); err != nil {
		t.Fatalf("write after repair: %v", err)
	}
	if err := m2.Close(); err != nil {
		t.Fatal(err)
	}

	m3, st3, _ := newEngine(t, Config{Dir: dir, EnableJournal: true})
	report3, err := m3.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if report3.DiscardedBytes != 0 {
		t.Fatalf("the repaired journal reported damage again: %d bytes", report3.DiscardedBytes)
	}
	if v, ok, _ := st3.Get("after"); !ok || string(v) != "repair" {
		t.Fatalf("the post-repair write was lost: (%q, %v)", v, ok)
	}
}

func TestRecoveryRefusesToRepairWhenConfiguredToFail(t *testing.T) {
	dir := t.TempDir()
	m1, st1, _ := newEngine(t, Config{Dir: dir, EnableJournal: true})
	mustSet(t, st1, "a", "1")
	journalPath := m1.JournalPath()
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(journalPath, raw[:len(raw)-3], 0o600); err != nil {
		t.Fatal(err)
	}

	m2, _, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, Repair: RepairFail})
	if _, err := m2.Recover(); err == nil {
		t.Fatal("Recover succeeded although the repair policy is fail")
	} else if !strings.Contains(err.Error(), "refusing to start") {
		t.Fatalf("error %v should explain the refusal", err)
	}

	// The file must be untouched.
	after, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(raw)-3 {
		t.Fatalf("the journal was modified despite the fail policy: %d bytes, want %d", len(after), len(raw)-3)
	}
}

func TestRecoveryCountsOrphanRecords(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewManual(startMS)

	m1, st1, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, Clock: clk})
	mustSet(t, st1, "target", "1")
	if _, err := st1.SetExpiry("target", startMS+1000); err != nil {
		t.Fatal(err)
	}
	mustSet(t, st1, "victim", "1")
	// Delete the key, then expire it: the second record is an orphan for the
	// state being replayed.
	if _, err := st1.Delete("victim"); err != nil {
		t.Fatal(err)
	}
	m := store.Mutation{Op: store.OpExpire, Key: "victim", ExpireAt: startMS + 5000}
	if _, err := m1.Record(m); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	m2, st2, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, Clock: clk})
	report, err := m2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if report.JournalOrphans != 1 {
		t.Fatalf("JournalOrphans = %d, want 1", report.JournalOrphans)
	}
	if len(report.Warnings) == 0 {
		t.Fatal("an orphan record was not reported")
	}
	if remaining, state := st2.TTL("target"); state != store.TTLRemaining || remaining != 1000 {
		t.Fatalf("target deadline = (%d, %v), want (1000, remaining)", remaining, state)
	}
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

func TestSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	m1, st1, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, SnapshotOnShutdown: false})

	mustSet(t, st1, "string", "value")
	if _, err := st1.HashSet("hash", []store.FieldValue{fv("b", "2"), fv("a", "1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := st1.Set("volatile", []byte("v"), store.SetOptions{ExpireAt: startMS + 30_000}); err != nil {
		t.Fatal(err)
	}
	rawValue := []byte{0x00, 0xff, '\r', '\n'}
	if _, err := st1.Set("binary", rawValue, store.SetOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := m1.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}

	view, err := loadSnapshotFile(m1.SnapshotPath(), testMaxRecord)
	if err != nil {
		t.Fatalf("loadSnapshotFile: %v", err)
	}
	if len(view.Items) != 4 {
		t.Fatalf("snapshot has %d items, want 4", len(view.Items))
	}
	if view.Seq == 0 {
		t.Fatal("snapshot does not carry a sequence number")
	}

	// Restore into a brand new store and compare.
	fresh := store.New(clock.NewManual(startMS), store.Options{MaxValueBytes: 1 << 20})
	if err := fresh.ApplySnapshot(view); err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := fresh.Get("string"); !ok || string(got) != "value" {
		t.Fatalf("string = (%q, %v)", got, ok)
	}
	if got, ok, _ := fresh.Get("binary"); !ok || !bytes.Equal(got, rawValue) {
		t.Fatalf("binary = (%x, %v)", got, ok)
	}
	pairs, ok, err := fresh.HashGetAll("hash")
	if err != nil || !ok || len(pairs) != 2 || pairs[0].Name != "a" || pairs[1].Name != "b" {
		t.Fatalf("hash = (%v, %v, %v)", pairs, ok, err)
	}
	if remaining, state := fresh.TTL("volatile"); state != store.TTLRemaining || remaining != 30_000 {
		t.Fatalf("deadline = (%d, %v), want (30000, remaining)", remaining, state)
	}
}

func TestSnapshotIsReplacedAtomically(t *testing.T) {
	dir := t.TempDir()
	m, st, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, SnapshotOnShutdown: false})

	if err := m.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(m.SnapshotPath())
	if err != nil {
		t.Fatal(err)
	}

	mustSet(t, st, "k", "v")
	if err := m.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(m.SnapshotPath())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, second) {
		t.Fatal("the second snapshot did not replace the first")
	}

	// No temporary file may be left behind.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), TempSuffix) {
			t.Fatalf("temporary file %s was left behind", e.Name())
		}
	}
}

func TestOpenRemovesInterruptedSnapshotTemp(t *testing.T) {
	dir := t.TempDir()
	tmp := filepath.Join(dir, SnapshotFileName+TempSuffix)
	if err := os.WriteFile(tmp, []byte("partial snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	m := openManager(t, Config{Dir: dir, EnableJournal: false})
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("interrupted snapshot temp file survived: %v", err)
	}
}

func TestCorruptSnapshotIsRefused(t *testing.T) {
	tests := map[string]func([]byte) []byte{
		"bad magic":       func(b []byte) []byte { copy(b[0:8], "BROKEN!!"); return b },
		"bad version":     func(b []byte) []byte { binary.LittleEndian.PutUint16(b[8:10], 99); return b },
		"bad checksum":    func(b []byte) []byte { b[len(b)-1] ^= 0xff; return b },
		"truncated":       func(b []byte) []byte { return b[:len(b)-2] },
		"too small":       func(b []byte) []byte { return b[:4] },
		"absurd keycount": func(b []byte) []byte { binary.LittleEndian.PutUint64(b[28:36], 1<<40); return b },
	}

	for name, damage := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			m1, st1, _ := newEngine(t, Config{Dir: dir, EnableJournal: false, SnapshotOnShutdown: false})
			mustSet(t, st1, "k", "v")
			if err := m1.SaveSnapshot(); err != nil {
				t.Fatal(err)
			}
			if err := m1.Close(); err != nil {
				t.Fatal(err)
			}

			raw, err := os.ReadFile(m1.SnapshotPath())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(m1.SnapshotPath(), damage(raw), 0o600); err != nil {
				t.Fatal(err)
			}

			m2, st2, _ := newEngine(t, Config{Dir: dir, EnableJournal: false, SnapshotOnShutdown: false})
			_, err = m2.Recover()
			if err == nil {
				t.Fatal("a corrupt snapshot was accepted")
			}
			if !errors.Is(err, ErrCorruptSnapshot) {
				t.Fatalf("error %v should wrap ErrCorruptSnapshot", err)
			}
			// Critically: the store must not silently become empty-and-ok.
			if st2.Len() != 0 {
				t.Fatalf("the store was populated from a rejected snapshot: %d keys", st2.Len())
			}
		})
	}
}

func TestSnapshotThenJournalReplay(t *testing.T) {
	dir := t.TempDir()
	m1, st1, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, SnapshotOnShutdown: false})

	mustSet(t, st1, "before-snapshot", "1")
	if err := m1.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	snapshotSeq := m1.Seq()

	// Writes after the snapshot must survive through the journal.
	mustSet(t, st1, "after-snapshot", "2")
	if _, err := st1.Delete("before-snapshot"); err != nil {
		t.Fatal(err)
	}
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	m2, st2, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, SnapshotOnShutdown: false})
	report, err := m2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if !report.SnapshotLoaded {
		t.Fatal("the snapshot was not loaded")
	}
	if report.SnapshotSeq != snapshotSeq {
		t.Fatalf("SnapshotSeq = %d, want %d", report.SnapshotSeq, snapshotSeq)
	}
	if report.JournalSkipped == 0 {
		t.Fatal("no records were skipped as already covered by the snapshot")
	}
	if report.JournalApplied == 0 {
		t.Fatal("no post-snapshot records were applied")
	}
	if st2.Exists("before-snapshot") != 0 {
		t.Fatal("the deletion after the snapshot was lost")
	}
	if v, ok, _ := st2.Get("after-snapshot"); !ok || string(v) != "2" {
		t.Fatalf("post-snapshot write was lost: (%q, %v)", v, ok)
	}
	if st2.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", st2.Len())
	}
}

func TestSnapshotSequenceMatchesJournal(t *testing.T) {
	dir := t.TempDir()
	m, st, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, SnapshotOnShutdown: false})

	mustSet(t, st, "a", "1")
	mustSet(t, st, "b", "2")
	if err := m.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}

	view, err := loadSnapshotFile(m.SnapshotPath(), testMaxRecord)
	if err != nil {
		t.Fatal(err)
	}
	if view.Seq != m.Seq() {
		t.Fatalf("snapshot seq = %d, journal seq = %d; a snapshot must not claim state it does not have",
			view.Seq, m.Seq())
	}
}

func TestSnapshotWithJournalDisabled(t *testing.T) {
	dir := t.TempDir()
	m1, st1, _ := newEngine(t, Config{Dir: dir, EnableJournal: false, SnapshotOnShutdown: true})
	// Mirror the server's startup order: recovery runs before the store is
	// used, and a manager only writes a shutdown snapshot after that.
	if _, err := m1.Recover(); err != nil {
		t.Fatal(err)
	}
	mustSet(t, st1, "k", "v")
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	// Without a journal, durability comes from the shutdown snapshot.
	m2, st2, _ := newEngine(t, Config{Dir: dir, EnableJournal: false, SnapshotOnShutdown: false})
	report, err := m2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if !report.SnapshotLoaded {
		t.Fatalf("snapshot was not loaded: %+v", report)
	}
	if v, ok, _ := st2.Get("k"); !ok || string(v) != "v" {
		t.Fatalf("value = (%q, %v), want (\"v\", true)", v, ok)
	}
}

func TestSnapshotDuringConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	m, st, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, Fsync: FsyncNo, SnapshotOnShutdown: false})

	const writers = 4
	const perWriter = 200
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				if _, err := st.Set(fmt.Sprintf("w%d-k%04d", id, j), []byte("v"), store.SetOptions{}); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
			}
		}(i)
	}

	// Snapshot repeatedly while the writers run.
	for i := 0; i < 5; i++ {
		if err := m.SaveSnapshot(); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
	}
	wg.Wait()

	// Final state: snapshot the whole thing and make sure everything is there.
	if err := m.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	view, err := loadSnapshotFile(m.SnapshotPath(), testMaxRecord)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Items) != writers*perWriter {
		t.Fatalf("snapshot holds %d keys, want %d", len(view.Items), writers*perWriter)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	m2, st2, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, SnapshotOnShutdown: false})
	report, err := m2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if st2.Len() != writers*perWriter {
		t.Fatalf("recovered %d keys, want %d (report %+v)", st2.Len(), writers*perWriter, report)
	}
	// Every key written while a snapshot was in flight must be present: the
	// post-snapshot journal records cover them.
	for i := 0; i < writers; i++ {
		for j := 0; j < perWriter; j++ {
			key := fmt.Sprintf("w%d-k%04d", i, j)
			if _, ok, _ := st2.Get(key); !ok {
				t.Fatalf("key %q was lost across snapshot and recovery", key)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Manager behaviour
// ---------------------------------------------------------------------------

func TestManagerSequenceAdvances(t *testing.T) {
	dir := t.TempDir()
	m, st, _ := newEngine(t, Config{Dir: dir, EnableJournal: true})
	if m.Seq() != 0 {
		t.Fatalf("initial Seq() = %d, want 0", m.Seq())
	}
	mustSet(t, st, "a", "1")
	if m.Seq() != 1 {
		t.Fatalf("Seq() = %d, want 1", m.Seq())
	}
	mustSet(t, st, "b", "2")
	if m.Seq() != 2 {
		t.Fatalf("Seq() = %d, want 2", m.Seq())
	}
}

func TestManagerWithoutJournalStillCounts(t *testing.T) {
	dir := t.TempDir()
	m, st, _ := newEngine(t, Config{Dir: dir, EnableJournal: false})
	mustSet(t, st, "a", "1")
	mustSet(t, st, "b", "2")
	if m.Seq() != 2 {
		t.Fatalf("Seq() = %d, want 2", m.Seq())
	}
	if st := m.Status(); st.JournalEnabled {
		t.Fatal("status reports a journal that was disabled")
	}
}

func TestManagerStartStopsCleanly(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewManual(startMS)
	m := openManager(t, Config{
		Dir:              dir,
		EnableJournal:    true,
		Fsync:            FsyncEverySec,
		SnapshotInterval: time.Millisecond,
		Clock:            clk,
	})
	st := store.New(clk, store.Options{Recorder: m, MaxValueBytes: 1 << 20})
	m.Attach(st)

	m.Start()
	mustSet(t, st, "k", "v")
	if err := m.Fsync(); err != nil {
		t.Fatalf("Fsync: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Closing twice must be safe.
	if err := m.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestManagerStatusReportsRecovery(t *testing.T) {
	dir := t.TempDir()
	m, st, _ := newEngine(t, Config{Dir: dir, EnableJournal: true, SnapshotOnShutdown: false})
	mustSet(t, st, "k", "v")
	if err := m.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Recover(); err != nil {
		t.Fatal(err)
	}

	stt := m.Status()
	if !stt.JournalEnabled {
		t.Fatal("status says the journal is disabled")
	}
	if !stt.SnapshotExists || stt.SnapshotCount == 0 {
		t.Fatalf("status does not report the snapshot: %+v", stt)
	}
	if stt.Recovery == nil || stt.Recovery.Status != "ok" {
		t.Fatalf("status does not report recovery: %+v", stt.Recovery)
	}
	if stt.Journal.SizeBytes <= journalHeaderSize {
		t.Fatalf("journal size = %d, want more than the header", stt.Journal.SizeBytes)
	}
	if stt.LastSnapshotSeq != m.Seq() {
		t.Fatalf("LastSnapshotSeq = %d, want %d", stt.LastSnapshotSeq, m.Seq())
	}
}

func TestConfigValidation(t *testing.T) {
	if _, err := Open(Config{Dir: ""}); err == nil {
		t.Fatal("an empty data directory was accepted")
	}
	if _, err := Open(Config{Dir: t.TempDir(), Fsync: "sometimes"}); err == nil {
		t.Fatal("an unknown fsync mode was accepted")
	}
	if _, err := Open(Config{Dir: t.TempDir(), Repair: "maybe"}); err == nil {
		t.Fatal("an unknown repair policy was accepted")
	}
	if _, err := Open(Config{Dir: t.TempDir(), SnapshotInterval: -time.Second}); err == nil {
		t.Fatal("a negative snapshot interval was accepted")
	}
}

func TestParseHelpers(t *testing.T) {
	for _, ok := range []string{"always", "everysec", "no"} {
		if _, err := ParseFsyncMode(ok); err != nil {
			t.Errorf("ParseFsyncMode(%q): %v", ok, err)
		}
	}
	if _, err := ParseFsyncMode("always!"); err == nil {
		t.Error("ParseFsyncMode accepted nonsense")
	}
	for _, ok := range []string{"truncate", "fail"} {
		if _, err := ParseRepairPolicy(ok); err != nil {
			t.Errorf("ParseRepairPolicy(%q): %v", ok, err)
		}
	}
	if _, err := ParseRepairPolicy("ignore"); err == nil {
		t.Error("ParseRepairPolicy accepted nonsense")
	}
}

// TestScanReportTruncationFailed documents the helper used by tests above.
func TestScanReportTruncationFailed(t *testing.T) {
	r := &ScanReport{}
	if r.TruncationFailed() {
		t.Fatal("a clean report claims truncation")
	}
	r.Truncated = true
	if !r.TruncationFailed() {
		t.Fatal("a truncated report does not report truncation")
	}
}
