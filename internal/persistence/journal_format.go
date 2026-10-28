// Package persistence implements MiniRedis's durability layer: an append-only
// journal, versioned snapshots, and startup recovery.
//
// Everything in this package is written from scratch. No external database,
// key-value store or serialisation framework is involved.
//
// # Ordering
//
// The store calls Recorder.Record with its write lock held, immediately before
// it applies the mutation in memory. That gives two properties:
//
//   - the journal order is exactly the commit order, so replaying the journal
//     reproduces the keyspace;
//   - a mutation that is visible to a client has already been written to the
//     journal file (its durability then depends on the fsync mode).
//
// # Lock ordering
//
// The only nesting is store.mu (held by the store) → journal.mu. Nothing in
// this package acquires another MiniRedis lock while holding journal.mu, so the
// order cannot be reversed and no deadlock is possible.
package persistence

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/amandev11/miniredis/internal/store"
)

// Journal and snapshot format constants.
const (
	// journalMagic identifies a MiniRedis journal file.
	journalMagic = "MRJRNL01"
	// journalVersion is the current record format version.
	journalVersion = uint16(1)
	// journalHeaderSize is the size of the fixed file header.
	journalHeaderSize = 16
	// frameHeaderSize is the size of the [length][crc32] prefix of a record.
	frameHeaderSize = 8
	// minPayloadSize is seq (8) plus op (1).
	minPayloadSize = 9

	// DefaultMaxRecordBytes bounds a single journal record. It also bounds how
	// much a corrupt record header can make the reader allocate.
	DefaultMaxRecordBytes = 64 << 20
)

// crcTable is the Castagnoli polynomial, which has hardware support on amd64
// and arm64 and is therefore cheaper than the IEEE polynomial.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Format errors. They are deliberately distinct so that operators can tell a
// truncated tail (normal after a crash) from real corruption.
var (
	// ErrBadJournalHeader means the file is not a MiniRedis journal.
	ErrBadJournalHeader = fmt.Errorf("unrecognised journal header")
	// ErrCorruptRecord means a record failed validation.
	ErrCorruptRecord = fmt.Errorf("journal record is corrupt")
	// ErrTruncatedRecord means the file ends in the middle of a record, which
	// is the expected outcome of a crash during an append.
	ErrTruncatedRecord = fmt.Errorf("journal record is truncated")
	// ErrRecordTooLarge means a record exceeds the configured maximum.
	ErrRecordTooLarge = fmt.Errorf("journal record exceeds the maximum size")
	// ErrJournalUnusable means an earlier write failure poisoned the journal.
	ErrJournalUnusable = fmt.Errorf("journal is unusable after an earlier write failure")
)

// ---------------------------------------------------------------------------
// Binary primitives
// ---------------------------------------------------------------------------

// encoder appends little-endian fields to a growable buffer.
type encoder struct{ buf []byte }

func (e *encoder) u8(v byte) { e.buf = append(e.buf, v) }
func (e *encoder) u16(v uint16) {
	e.buf = binary.LittleEndian.AppendUint16(e.buf, v)
}
func (e *encoder) u32(v uint32) {
	e.buf = binary.LittleEndian.AppendUint32(e.buf, v)
}
func (e *encoder) u64(v uint64) {
	e.buf = binary.LittleEndian.AppendUint64(e.buf, v)
}
func (e *encoder) i64(v int64) { e.u64(uint64(v)) }

// bytes writes a length-delimited byte string.
func (e *encoder) bytes(b []byte) {
	e.u32(uint32(len(b)))
	e.buf = append(e.buf, b...)
}

func (e *encoder) str(s string) {
	e.u32(uint32(len(s)))
	e.buf = append(e.buf, s...)
}

// decoder reads fields back. The first failure is latched and every later read
// returns a zero value, so callers can decode a whole record and check once.
type decoder struct {
	buf []byte
	err error
}

func (d *decoder) fail(format string, args ...any) {
	if d.err == nil {
		d.err = fmt.Errorf(format, args...)
	}
}

func (d *decoder) need(n int) bool {
	if d.err != nil {
		return false
	}
	if len(d.buf) < n {
		d.fail("record is short: %d bytes remain, %d needed", len(d.buf), n)
		return false
	}
	return true
}

func (d *decoder) u8() byte {
	if !d.need(1) {
		return 0
	}
	v := d.buf[0]
	d.buf = d.buf[1:]
	return v
}

func (d *decoder) u16() uint16 {
	if !d.need(2) {
		return 0
	}
	v := binary.LittleEndian.Uint16(d.buf)
	d.buf = d.buf[2:]
	return v
}

func (d *decoder) u32() uint32 {
	if !d.need(4) {
		return 0
	}
	v := binary.LittleEndian.Uint32(d.buf)
	d.buf = d.buf[4:]
	return v
}

func (d *decoder) u64() uint64 {
	if !d.need(8) {
		return 0
	}
	v := binary.LittleEndian.Uint64(d.buf)
	d.buf = d.buf[8:]
	return v
}

func (d *decoder) i64() int64 { return int64(d.u64()) }

// bytes returns a sub-slice of the payload. The payload is freshly allocated
// for every record and never reused, so the result is safe to retain; callers
// that store it copy it anyway.
func (d *decoder) bytes() []byte {
	n := d.u32()
	if d.err != nil {
		return nil
	}
	if int64(n) > int64(len(d.buf)) {
		d.fail("length-delimited field of %d bytes exceeds the %d bytes left", n, len(d.buf))
		return nil
	}
	v := d.buf[:n]
	d.buf = d.buf[n:]
	return v
}

func (d *decoder) str() string { return string(d.bytes()) }

// finish reports the first error, or an error when trailing bytes remain.
func (d *decoder) finish() error {
	if d.err != nil {
		return d.err
	}
	if len(d.buf) != 0 {
		return fmt.Errorf("record has %d trailing bytes", len(d.buf))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Record encoding
// ---------------------------------------------------------------------------

// marshalRecord serialises one mutation together with its sequence number.
//
// Layout (all integers little-endian):
//
//	u64 seq
//	u8  op
//	op-specific body, described per operation in docs/PERSISTENCE.md
//
// Every byte string is stored as a u32 length followed by the raw bytes, which
// makes keys, hash fields and values binary safe.
func marshalRecord(seq uint64, m store.Mutation) ([]byte, error) {
	e := &encoder{buf: make([]byte, 0, 64)}
	e.u64(seq)
	e.u8(byte(m.Op))

	switch m.Op {
	case store.OpSet:
		e.str(m.Key)
		e.i64(m.ExpireAt)
		e.bytes(m.Value)

	case store.OpMSet:
		if len(m.Pairs) > 0xffff {
			return nil, fmt.Errorf("%w: MSET with %d pairs", ErrRecordTooLarge, len(m.Pairs))
		}
		e.u32(uint32(len(m.Pairs)))
		for _, p := range m.Pairs {
			e.str(p.Name)
			e.i64(0) // MSET always clears deadlines
			e.bytes(p.Value)
		}

	case store.OpDel:
		e.u32(uint32(len(m.Keys)))
		for _, k := range m.Keys {
			e.str(k)
		}

	case store.OpAppend:
		e.str(m.Key)
		e.i64(m.ExpireAt)
		e.bytes(m.Value)

	case store.OpHashSet:
		e.str(m.Key)
		e.i64(m.ExpireAt)
		e.u32(uint32(len(m.Pairs)))
		for _, p := range m.Pairs {
			e.str(p.Name)
			e.bytes(p.Value)
		}

	case store.OpHashDel:
		e.str(m.Key)
		e.u32(uint32(len(m.Fields)))
		for _, f := range m.Fields {
			e.str(f)
		}

	case store.OpExpire:
		e.str(m.Key)
		e.i64(m.ExpireAt)

	case store.OpFlush:
		// No body.

	default:
		return nil, fmt.Errorf("cannot journal unknown operation %d", uint8(m.Op))
	}
	return e.buf, nil
}

// unmarshalRecord reverses marshalRecord. It is strict: any length that does
// not fit, and any trailing byte, is an error rather than a silent skip.
func unmarshalRecord(payload []byte) (uint64, store.Mutation, error) {
	var m store.Mutation
	d := &decoder{buf: payload}

	seq := d.u64()
	op := store.Op(d.u8())
	m.Op = op

	switch op {
	case store.OpSet, store.OpAppend:
		m.Key = d.str()
		m.ExpireAt = d.i64()
		m.Value = d.bytes()

	case store.OpMSet:
		n := d.u32()
		if d.err == nil {
			if int64(n) > int64(len(d.buf))/4 {
				d.fail("MSET declares %d pairs but only %d bytes remain", n, len(d.buf))
				break
			}
			pairs := make([]store.FieldValue, 0, n)
			for i := uint32(0); i < n; i++ {
				name := d.str()
				_ = d.i64() // reserved: MSET deadlines are always cleared
				value := d.bytes()
				pairs = append(pairs, store.FieldValue{Name: name, Value: value})
			}
			m.Pairs = pairs
		}

	case store.OpDel:
		n := d.u32()
		if d.err == nil {
			if int64(n) > int64(len(d.buf))/4 {
				d.fail("DEL declares %d keys but only %d bytes remain", n, len(d.buf))
				break
			}
			keys := make([]string, 0, n)
			for i := uint32(0); i < n; i++ {
				keys = append(keys, d.str())
			}
			m.Keys = keys
		}

	case store.OpHashSet:
		m.Key = d.str()
		m.ExpireAt = d.i64()
		n := d.u32()
		if d.err == nil {
			if int64(n) > int64(len(d.buf))/4 {
				d.fail("HSET declares %d pairs but only %d bytes remain", n, len(d.buf))
				break
			}
			pairs := make([]store.FieldValue, 0, n)
			for i := uint32(0); i < n; i++ {
				name := d.str()
				value := d.bytes()
				pairs = append(pairs, store.FieldValue{Name: name, Value: value})
			}
			m.Pairs = pairs
		}

	case store.OpHashDel:
		m.Key = d.str()
		n := d.u32()
		if d.err == nil {
			if int64(n) > int64(len(d.buf))/4 {
				d.fail("HDEL declares %d fields but only %d bytes remain", n, len(d.buf))
				break
			}
			fields := make([]string, 0, n)
			for i := uint32(0); i < n; i++ {
				fields = append(fields, d.str())
			}
			m.Fields = fields
		}

	case store.OpExpire:
		m.Key = d.str()
		m.ExpireAt = d.i64()

	case store.OpFlush:
		// No body.

	default:
		return 0, m, fmt.Errorf("%w: unknown operation %d", ErrCorruptRecord, uint8(op))
	}

	if err := d.finish(); err != nil {
		return 0, m, fmt.Errorf("%w: %v", ErrCorruptRecord, err)
	}
	return seq, m, nil
}

// journalHeader returns the fixed 16-byte file header.
func journalHeader() []byte {
	h := make([]byte, journalHeaderSize)
	copy(h[0:8], journalMagic)
	binary.LittleEndian.PutUint16(h[8:10], journalVersion)
	binary.LittleEndian.PutUint16(h[10:12], 0) // flags, reserved
	binary.LittleEndian.PutUint32(h[12:16], 0) // reserved
	return h
}

// validateJournalHeader checks magic, version and the reserved fields.
func validateJournalHeader(h []byte) error {
	if len(h) != journalHeaderSize {
		return fmt.Errorf("%w: header is %d bytes, want %d", ErrBadJournalHeader, len(h), journalHeaderSize)
	}
	if string(h[0:8]) != journalMagic {
		return fmt.Errorf("%w: bad magic %q", ErrBadJournalHeader, sanitiseMagic(h[0:8]))
	}
	if v := binary.LittleEndian.Uint16(h[8:10]); v != journalVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrBadJournalHeader, v)
	}
	if flags := binary.LittleEndian.Uint16(h[10:12]); flags != 0 {
		return fmt.Errorf("%w: unsupported flags 0x%04x", ErrBadJournalHeader, flags)
	}
	return nil
}

// sanitiseMagic renders a magic field for an error message without echoing
// arbitrary binary content for longer than a few bytes.
func sanitiseMagic(b []byte) string {
	var sb strings.Builder
	for _, c := range b {
		if c >= 0x20 && c < 0x7f {
			sb.WriteByte(c)
		} else {
			fmt.Fprintf(&sb, "\\x%02x", c)
		}
	}
	return sb.String()
}
