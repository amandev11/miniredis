package persistence

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/amandev11/miniredis/internal/store"
)

// Snapshot file constants.
const (
	// snapshotMagic identifies a MiniRedis snapshot.
	snapshotMagic = "MRSNAP01"
	// snapshotVersion is the current snapshot format version.
	snapshotVersion = uint16(1)
	// snapshotHeaderSize is the size of the fixed header.
	snapshotHeaderSize = 36
	// snapshotFooterSize is the trailing checksum.
	snapshotFooterSize = 4
	// snapshotMinSize is the smallest possible valid file.
	snapshotMinSize = snapshotHeaderSize + snapshotFooterSize
	// maxSnapshotField bounds any single length inside a snapshot file, which
	// also bounds how much a corrupt length can make the loader allocate.
	defaultMaxSnapshotField = DefaultMaxRecordBytes
)

// ErrCorruptSnapshot reports a snapshot file that failed validation. A corrupt
// snapshot is never treated as an empty database: the server refuses to start
// until the file is repaired or moved aside.
var ErrCorruptSnapshot = errors.New("snapshot file is corrupt")

// writeSnapshotFile serialises view into an already-open file.
//
// The writer streams straight to the file and computes the checksum as it
// goes, so memory use does not grow with the keyspace.
func writeSnapshotFile(f *os.File, view *store.Snapshot) error {
	hasher := crc32.New(crcTable)
	bw := bufio.NewWriterSize(io.MultiWriter(f, hasher), 1<<20)

	header := make([]byte, snapshotHeaderSize)
	copy(header[0:8], snapshotMagic)
	binary.LittleEndian.PutUint16(header[8:10], snapshotVersion)
	binary.LittleEndian.PutUint16(header[10:12], 0) // flags, reserved
	createdAt := view.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	binary.LittleEndian.PutUint64(header[12:20], view.Seq)
	binary.LittleEndian.PutUint64(header[20:28], uint64(createdAt.UnixMilli()))
	binary.LittleEndian.PutUint64(header[28:36], uint64(len(view.Items)))
	if _, err := bw.Write(header); err != nil {
		return err
	}

	for i := range view.Items {
		item := &view.Items[i]
		if err := writeLengthPrefixed(bw, []byte(item.Key)); err != nil {
			return err
		}
		if err := bw.WriteByte(byte(item.Kind)); err != nil {
			return err
		}
		if err := writeInt64(bw, item.ExpireAt); err != nil {
			return err
		}
		switch item.Kind {
		case store.KindString:
			if err := writeLengthPrefixed(bw, item.Value); err != nil {
				return err
			}
		case store.KindHash:
			if err := writeUint32(bw, uint32(len(item.Pairs))); err != nil {
				return err
			}
			for _, p := range item.Pairs {
				if err := writeLengthPrefixed(bw, []byte(p.Name)); err != nil {
					return err
				}
				if err := writeLengthPrefixed(bw, p.Value); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("cannot snapshot key %q with unknown kind %d", item.Key, item.Kind)
		}
	}

	if err := bw.Flush(); err != nil {
		return err
	}

	// The checksum covers every byte before it and is written outside the
	// hasher.
	var footer [snapshotFooterSize]byte
	binary.LittleEndian.PutUint32(footer[:], hasher.Sum32())
	if _, err := f.Write(footer[:]); err != nil {
		return err
	}
	return nil
}

// loadSnapshotFile validates and decodes a snapshot file.
//
// Validation happens in two passes: the checksum is verified over the whole
// file first, and only then is the content decoded. A file that fails either
// pass is rejected with ErrCorruptSnapshot, and the running server is never
// handed partial contents.
func loadSnapshotFile(path string, maxField int) (*store.Snapshot, error) {
	if maxField <= 0 {
		maxField = defaultMaxSnapshotField
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
	size := fi.Size()
	if size < snapshotMinSize {
		return nil, fmt.Errorf("%w: %s is %d bytes, smaller than the %d byte minimum",
			ErrCorruptSnapshot, path, size, snapshotMinSize)
	}

	// Pass 1: checksum.
	if err := verifySnapshotChecksum(f, size); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	// Pass 2: decode.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	br := bufio.NewReaderSize(io.LimitReader(f, size-snapshotFooterSize), 1<<20)
	r := &snapReader{br: br, max: maxField}

	header := make([]byte, snapshotHeaderSize)
	if _, err := io.ReadFull(br, header); err != nil {
		return nil, fmt.Errorf("%w: reading header: %v", ErrCorruptSnapshot, err)
	}
	if string(header[0:8]) != snapshotMagic {
		return nil, fmt.Errorf("%w: bad magic %q", ErrCorruptSnapshot, sanitiseMagic(header[0:8]))
	}
	if v := binary.LittleEndian.Uint16(header[8:10]); v != snapshotVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrCorruptSnapshot, v)
	}
	if flags := binary.LittleEndian.Uint16(header[10:12]); flags != 0 {
		return nil, fmt.Errorf("%w: unsupported flags 0x%04x", ErrCorruptSnapshot, flags)
	}

	view := &store.Snapshot{
		Seq:       binary.LittleEndian.Uint64(header[12:20]),
		CreatedAt: time.UnixMilli(int64(binary.LittleEndian.Uint64(header[20:28]))),
	}
	keyCount := binary.LittleEndian.Uint64(header[28:36])
	// A key needs at least 5 bytes (key length + kind), so the remaining bytes
	// bound how many keys the file can possibly hold.
	maxPossible := uint64((size - snapshotMinSize) / 5)
	if keyCount > maxPossible {
		return nil, fmt.Errorf("%w: header declares %d keys but the file can hold at most %d",
			ErrCorruptSnapshot, keyCount, maxPossible)
	}

	view.Items = make([]store.SnapshotItem, 0, keyCount)
	for i := uint64(0); i < keyCount; i++ {
		item, err := r.item()
		if err != nil {
			return nil, fmt.Errorf("%s: %w: item %d: %v", path, ErrCorruptSnapshot, i, err)
		}
		view.Items = append(view.Items, item)
	}
	if err := r.finish(); err != nil {
		return nil, fmt.Errorf("%s: %w: %v", path, ErrCorruptSnapshot, err)
	}
	return view, nil
}

// verifySnapshotChecksum streams the file through a CRC and compares it with
// the stored footer, without buffering the whole file.
func verifySnapshotChecksum(f *os.File, size int64) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	hasher := crc32.New(crcTable)
	body := io.LimitReader(f, size-snapshotFooterSize)
	if _, err := io.Copy(hasher, body); err != nil {
		return err
	}
	var footer [snapshotFooterSize]byte
	if _, err := io.ReadFull(f, footer[:]); err != nil {
		return fmt.Errorf("%w: reading checksum footer: %v", ErrCorruptSnapshot, err)
	}
	if got, want := hasher.Sum32(), binary.LittleEndian.Uint32(footer[:]); got != want {
		return fmt.Errorf("%w: checksum mismatch (stored 0x%08x, computed 0x%08x)", ErrCorruptSnapshot, want, got)
	}
	return nil
}

// snapReader decodes a snapshot stream with a strict bound on every field.
type snapReader struct {
	br  *bufio.Reader
	max int
	err error
}

func (r *snapReader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = fmt.Errorf(format, args...)
	}
}

func (r *snapReader) item() (store.SnapshotItem, error) {
	var item store.SnapshotItem

	key := r.bytes()
	kind := r.u8()
	deadline := r.i64()
	if err := r.err; err != nil {
		return item, err
	}
	if kind != byte(store.KindString) && kind != byte(store.KindHash) {
		return item, fmt.Errorf("unknown value kind %d", kind)
	}
	if deadline < 0 {
		return item, fmt.Errorf("negative deadline %d", deadline)
	}

	item.Key = string(key)
	item.Kind = store.Kind(kind)
	item.ExpireAt = deadline

	switch item.Kind {
	case store.KindString:
		item.Value = r.bytes()
	case store.KindHash:
		count := r.u32()
		if err := r.err; err != nil {
			return item, err
		}
		item.Pairs = make([]store.FieldValue, 0, count)
		for i := uint32(0); i < count; i++ {
			name := r.str()
			value := r.bytes()
			if err := r.err; err != nil {
				return item, err
			}
			item.Pairs = append(item.Pairs, store.FieldValue{Name: name, Value: value})
		}
	}
	if err := r.err; err != nil {
		return item, err
	}
	return item, nil
}

// finish reports both a latched decoding error and any unexpected trailing
// bytes, which would mean the file and its declared key count disagree.
func (r *snapReader) finish() error {
	if r.err != nil {
		return r.err
	}
	if _, err := r.br.ReadByte(); err == nil {
		return errors.New("file has trailing data after the declared keys")
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func (r *snapReader) u8() byte {
	b, err := r.br.ReadByte()
	if err != nil {
		r.fail("reading byte: %v", err)
		return 0
	}
	return b
}

func (r *snapReader) u32() uint32 {
	var b [4]byte
	if _, err := io.ReadFull(r.br, b[:]); err != nil {
		r.fail("reading uint32: %v", err)
		return 0
	}
	return binary.LittleEndian.Uint32(b[:])
}

func (r *snapReader) i64() int64 {
	var b [8]byte
	if _, err := io.ReadFull(r.br, b[:]); err != nil {
		r.fail("reading int64: %v", err)
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b[:]))
}

func (r *snapReader) bytes() []byte {
	n := r.u32()
	if r.err != nil {
		return nil
	}
	if int64(n) > int64(r.max) {
		r.fail("field of %d bytes exceeds the %d byte limit", n, r.max)
		return nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r.br, buf); err != nil {
		r.fail("reading %d byte field: %v", n, err)
		return nil
	}
	return buf
}

func (r *snapReader) str() string { return string(r.bytes()) }

// ---------------------------------------------------------------------------
// Small writers
// ---------------------------------------------------------------------------

func writeLengthPrefixed(w *bufio.Writer, b []byte) error {
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(b)))
	if _, err := w.Write(n[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func writeUint32(w *bufio.Writer, v uint32) error {
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], v)
	_, err := w.Write(n[:])
	return err
}

func writeInt64(w *bufio.Writer, v int64) error {
	var n [8]byte
	binary.LittleEndian.PutUint64(n[:], uint64(v))
	_, err := w.Write(n[:])
	return err
}
