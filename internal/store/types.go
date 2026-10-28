// Package store implements MiniRedis's in-memory database.
//
// Design summary
//
//   - The keyspace is a Go map from key to *Entry. Keys and hash fields are Go
//     strings, which are binary safe: a key may contain NUL, CR, LF or any
//     other byte.
//   - One sync.RWMutex protects the whole keyspace. This is the simplest
//     design that is obviously correct; sharding or per-key locks are only
//     worth adding once a benchmark shows the single lock is the bottleneck,
//     and docs/BENCHMARKS.md reports that measurement for this project.
//   - Every write command that mutates state is described by a Mutation. The
//     mutation is handed to a Recorder (the persistence journal) *before* the
//     in-memory change is applied and while the write lock is held. That is
//     what makes the journal order equal to the commit order.
//   - Expiration uses absolute Unix-millisecond deadlines, so a recovered key
//     keeps the same deadline it had before a restart.
package store

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/amandev11/miniredis/internal/clock"
)

// Kind is the type of value stored under a key. A key holds exactly one kind
// at a time; operations that would require another kind fail with
// ErrWrongType instead of converting the value.
type Kind uint8

// Supported value kinds.
const (
	// KindString is a binary-safe byte string.
	KindString Kind = 1
	// KindHash is a map of binary-safe fields to binary-safe values.
	KindHash Kind = 2
)

// String returns the name used by the TYPE command.
func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindHash:
		return "hash"
	default:
		return "unknown"
	}
}

// Entry is a single key's value plus its expiration metadata.
//
// Exactly one of Str and Hash is set, according to Kind. ExpireAt is an
// absolute Unix-millisecond deadline; zero means the key never expires.
type Entry struct {
	Kind     Kind
	Str      []byte
	Hash     map[string][]byte
	ExpireAt int64
}

// Clone returns a deep copy so callers can read a value without holding the
// store lock and without being able to mutate stored state.
func (e *Entry) Clone() *Entry {
	if e == nil {
		return nil
	}
	out := &Entry{Kind: e.Kind, ExpireAt: e.ExpireAt}
	switch e.Kind {
	case KindString:
		out.Str = append([]byte(nil), e.Str...)
	case KindHash:
		out.Hash = make(map[string][]byte, len(e.Hash))
		for f, v := range e.Hash {
			out.Hash[f] = append([]byte(nil), v...)
		}
	}
	return out
}

// Size is the entry's payload size in bytes, excluding map overhead. It feeds
// the approximate memory gauge reported by INFO.
func (e *Entry) Size() int {
	if e == nil {
		return 0
	}
	switch e.Kind {
	case KindString:
		return len(e.Str)
	case KindHash:
		n := 0
		for f, v := range e.Hash {
			n += len(f) + len(v)
		}
		return n
	default:
		return 0
	}
}

// FieldValue is one hash field/value pair.
type FieldValue struct {
	Name  string
	Value []byte
}

// Op identifies a journalled mutation. The values are stable and are written
// to disk, so they must never be renumbered.
type Op uint8

// Journalled mutation kinds. See docs/PERSISTENCE.md for the record format.
const (
	// OpSet writes a string value and an absolute expiration deadline.
	OpSet Op = 1
	// OpMSet writes several string values atomically. Pairs holds key/value.
	OpMSet Op = 2
	// OpDel removes whole keys. Keys holds the key names.
	OpDel Op = 3
	// OpAppend appends Value to the string stored under Key.
	OpAppend Op = 4
	// OpHashSet writes hash fields. Pairs holds field/value.
	OpHashSet Op = 5
	// OpHashDel removes hash fields. Fields holds the field names.
	OpHashDel Op = 6
	// OpExpire sets (or, with ExpireAt zero, clears) a key's deadline.
	OpExpire Op = 7
	// OpFlush removes every key.
	OpFlush Op = 8
)

// String names the operation for logs and diagnostics.
func (o Op) String() string {
	switch o {
	case OpSet:
		return "SET"
	case OpMSet:
		return "MSET"
	case OpDel:
		return "DEL"
	case OpAppend:
		return "APPEND"
	case OpHashSet:
		return "HSET"
	case OpHashDel:
		return "HDEL"
	case OpExpire:
		return "EXPIRE"
	case OpFlush:
		return "FLUSH"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(o))
	}
}

// Mutation is a single committed state transition.
//
// Mutations are *post-state* descriptions: ExpireAt always carries the
// absolute deadline the key has after the operation, never a relative TTL.
// Replaying the same journal therefore produces exactly the same keyspace even
// if the replay happens days later.
type Mutation struct {
	Op       Op
	Key      string
	ExpireAt int64
	Value    []byte
	Keys     []string
	Pairs    []FieldValue
	Fields   []string
}

// Recorder receives mutations before they are applied in memory.
//
// Record and Seq are always called with the store's write lock held, so a
// Recorder can rely on calls being serialised and never concurrent.
type Recorder interface {
	// Record persists m and returns its sequence number.
	Record(m Mutation) (uint64, error)
	// Seq returns the sequence number of the last committed mutation.
	Seq() uint64
}

// AmbiguousAppendError reports that a journal append failed *after* the record
// may already have reached the file, for example when the write succeeded but
// the durability barrier (fsync) failed.
//
// The store applies the mutation anyway in that case, because the journal may
// already contain the record and recovery replays every record it can read.
// The error is still returned to the caller so the failure is never silent.
type AmbiguousAppendError struct{ Err error }

// Error implements error.
func (e *AmbiguousAppendError) Error() string {
	return "journal append outcome is uncertain: " + e.Err.Error()
}

// Unwrap exposes the underlying failure.
func (e *AmbiguousAppendError) Unwrap() error { return e.Err }

// Sentinel errors. They carry no wire formatting; the command layer maps them
// to RESP error strings so that the storage engine stays protocol agnostic.
var (
	// ErrWrongType means an operation was attempted against a key holding a
	// different kind of value.
	ErrWrongType = errors.New("operation against a key holding the wrong kind of value")
	// ErrNotAnInteger means a string value could not be parsed as an integer.
	ErrNotAnInteger = errors.New("value is not an integer or out of range")
	// ErrIntOverflow means an arithmetic operation would leave the int64 range.
	ErrIntOverflow = errors.New("increment or decrement would overflow")
	// ErrValueTooLarge means a string value would exceed the configured limit.
	ErrValueTooLarge = errors.New("string value exceeds the configured size limit")
	// ErrKeyTooLarge means a key would exceed the configured limit.
	ErrKeyTooLarge = errors.New("key exceeds the configured size limit")
	// ErrHashTooLarge means a hash would exceed the configured field limit.
	ErrHashTooLarge = errors.New("hash exceeds the configured field limit")
	// ErrFieldTooLarge means a hash field or value would exceed the limit.
	ErrFieldTooLarge = errors.New("hash field or value exceeds the configured size limit")
	// ErrInvalidExpiry means a deadline was outside the representable range.
	ErrInvalidExpiry = errors.New("invalid expiration deadline")
	// ErrReplayOrphan means a journal record referenced a key that is not
	// present in the state being replayed. Recovery counts these as warnings
	// instead of ignoring them, because they indicate that the journal and the
	// keyspace disagree.
	ErrReplayOrphan = errors.New("journal record targets a key that is not present")
)

// Options configures a Store. The zero value is usable, except that limits of
// zero are replaced with the defaults from DefaultOptions.
type Options struct {
	// Recorder persists mutations. A nil Recorder disables persistence, which
	// is only appropriate for tests and for the --appendonly=no mode.
	Recorder Recorder
	// MaxValueBytes bounds a single string value.
	MaxValueBytes int
	// MaxKeyBytes bounds a single key name.
	MaxKeyBytes int
	// MaxHashFields bounds the number of fields in one hash.
	MaxHashFields int
	// MaxFieldBytes bounds a single hash field name or value.
	MaxFieldBytes int
}

// Default limits. They mirror the server defaults in internal/config.
const (
	DefaultMaxValueBytes = 512 << 20
	DefaultMaxKeyBytes   = 64 << 10
	DefaultMaxHashFields = 1 << 20
	DefaultMaxFieldBytes = 512 << 20
)

// DefaultOptions returns the option set used by tests and tooling.
func DefaultOptions() Options {
	return Options{
		MaxValueBytes: DefaultMaxValueBytes,
		MaxKeyBytes:   DefaultMaxKeyBytes,
		MaxHashFields: DefaultMaxHashFields,
		MaxFieldBytes: DefaultMaxFieldBytes,
	}
}

// withDefaults fills unset limits and clamps them to the package ceilings.
func (o Options) withDefaults() Options {
	d := DefaultOptions()
	if o.MaxValueBytes <= 0 {
		o.MaxValueBytes = d.MaxValueBytes
	}
	if o.MaxKeyBytes <= 0 {
		o.MaxKeyBytes = d.MaxKeyBytes
	}
	if o.MaxHashFields <= 0 {
		o.MaxHashFields = d.MaxHashFields
	}
	if o.MaxFieldBytes <= 0 {
		o.MaxFieldBytes = d.MaxFieldBytes
	}
	if o.MaxValueBytes > HardMaxValueBytes {
		o.MaxValueBytes = HardMaxValueBytes
	}
	if o.MaxFieldBytes > HardMaxValueBytes {
		o.MaxFieldBytes = HardMaxValueBytes
	}
	if o.MaxKeyBytes > HardMaxKeyBytes {
		o.MaxKeyBytes = HardMaxKeyBytes
	}
	if o.MaxHashFields > HardMaxHashFields {
		o.MaxHashFields = HardMaxHashFields
	}
	return o
}

// Absolute ceilings on stored data, independent of configuration.
const (
	HardMaxValueBytes = 1 << 30
	HardMaxKeyBytes   = 1 << 20
	HardMaxHashFields = 1 << 24
)

// SetMode selects the conditional behaviour of SET.
type SetMode uint8

// SET condition modes.
const (
	// SetAlways writes unconditionally.
	SetAlways SetMode = iota
	// SetNX writes only if the key does not already hold a live value.
	SetNX
	// SetXX writes only if the key already holds a live value.
	SetXX
)

// SetOptions carries SET's conditional and expiration behaviour.
type SetOptions struct {
	Mode SetMode
	// ExpireAt is an absolute Unix-millisecond deadline, 0 for none.
	ExpireAt int64
	// KeepTTL preserves the existing deadline instead of clearing it. It is
	// mutually exclusive with ExpireAt at the command layer.
	KeepTTL bool
}

// SnapshotItem is one key in an exported keyspace view.
type SnapshotItem struct {
	Key      string
	Kind     Kind
	ExpireAt int64
	Value    []byte
	Pairs    []FieldValue // sorted by field name, only for KindHash
}

// Snapshot is a consistent, immutable copy of the keyspace.
type Snapshot struct {
	// Seq is the sequence number of the last mutation included in the view.
	Seq   uint64
	Items []SnapshotItem
	// CreatedAt is the wall-clock time the view was captured.
	CreatedAt time.Time
}

// Bytes reports the approximate payload size of the view.
func (s *Snapshot) Bytes() int64 {
	if s == nil {
		return 0
	}
	var n int64
	for i := range s.Items {
		n += int64(len(s.Items[i].Key) + len(s.Items[i].Value))
		for _, p := range s.Items[i].Pairs {
			n += int64(len(p.Name) + len(p.Value))
		}
	}
	return n
}

// Store is the in-memory keyspace.
//
// Concurrency: all fields are protected by mu. Read methods use RLock, write
// methods use Lock. Methods never perform blocking I/O other than the journal
// append performed through Recorder, and they never call back into the store.
type Store struct {
	mu   sync.RWMutex
	clk  clock.Clock
	opt  Options
	data map[string]*Entry
}

// New returns an empty store. A nil clock is replaced with the system clock.
func New(clk clock.Clock, opt Options) *Store {
	if clk == nil {
		clk = clock.System{}
	}
	return &Store{
		clk:  clk,
		opt:  opt.withDefaults(),
		data: make(map[string]*Entry),
	}
}

// Clock exposes the store's clock so callers evaluate deadlines against the
// same source of truth.
func (s *Store) Clock() clock.Clock { return s.clk }

// Options returns the effective limits.
func (s *Store) Options() Options { return s.opt }

// now returns the current time in Unix milliseconds.
func (s *Store) now() int64 { return s.clk.NowMS() }

// liveLocked returns the entry for key, or nil when the key is missing or its
// deadline has passed. The caller must hold the lock.
//
// Lazy expiration is intentionally "look, do not delete": deleting on a read
// path would require upgrading the read lock to a write lock. Expired entries
// are removed by the next write to that key, by Delete, or by the active
// expiration cycle; in the meantime they are invisible to every read.
func (s *Store) liveLocked(key string, now int64) *Entry {
	e := s.data[key]
	if e == nil {
		return nil
	}
	if e.ExpireAt > 0 && e.ExpireAt <= now {
		return nil
	}
	return e
}

// ExistsLive reports whether key is currently present and not expired.
func (s *Store) ExistsLive(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.liveLocked(key, s.now()) != nil
}

// Kind reports the kind of value stored under key. ok is false when the key is
// missing or expired.
func (s *Store) Kind(key string) (Kind, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.liveLocked(key, s.now())
	if e == nil {
		return 0, false
	}
	return e.Kind, true
}

// Len reports the number of live keys.
//
// The count is exact: entries whose deadline has passed are excluded even if
// the active expiration cycle has not removed them yet.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	n := 0
	for _, e := range s.data {
		if e.ExpireAt == 0 || e.ExpireAt > now {
			n++
		}
	}
	return n
}

// ExpiresCount reports the number of live keys that carry a deadline.
func (s *Store) ExpiresCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	n := 0
	for _, e := range s.data {
		if e.ExpireAt > 0 && e.ExpireAt > now {
			n++
		}
	}
	return n
}

// ApproxBytes is an approximation of the payload bytes held in memory. It sums
// key and value lengths and deliberately excludes Go's map and slice overhead,
// so it under-reports real RSS by a significant margin.
func (s *Store) ApproxBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	var n int64
	for k, e := range s.data {
		if e.ExpireAt > 0 && e.ExpireAt <= now {
			continue
		}
		n += int64(len(k)) + int64(e.Size())
	}
	return n
}

// KeyNames returns every live key in lexicographic byte order. Ordering is
// deterministic, which Redis does not guarantee; see docs/PROTOCOL.md.
func (s *Store) KeyNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	out := make([]string, 0, len(s.data))
	for k, e := range s.data {
		if e.ExpireAt > 0 && e.ExpireAt <= now {
			continue
		}
		out = append(out, k)
	}
	sortStrings(out)
	return out
}

// sortStrings sorts byte-wise, matching the ordering used by every other
// sorted output in MiniRedis (SCAN, snapshots, HGETALL).
func sortStrings(s []string) { slices.Sort(s) }
