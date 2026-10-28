package store

import (
	"errors"
	"strconv"

	"github.com/amandev11/miniredis/internal/clock"
)

// NewWithSystemClock is a convenience constructor for callers that do not need
// a controllable clock.
func NewWithSystemClock(opt Options) *Store { return New(clock.System{}, opt) }

// ---------------------------------------------------------------------------
// String reads
// ---------------------------------------------------------------------------

// Get returns a copy of the string value stored under key.
//
// ok is false when the key is missing or expired, or when it holds a hash;
// in the latter case err is ErrWrongType.
func (s *Store) Get(key string) (value []byte, ok bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.liveLocked(key, s.now())
	if e == nil {
		return nil, false, nil
	}
	if e.Kind != KindString {
		return nil, false, ErrWrongType
	}
	return append([]byte(nil), e.Str...), true, nil
}

// StrLen returns the length of the string stored under key.
func (s *Store) StrLen(key string) (n int, ok bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.liveLocked(key, s.now())
	if e == nil {
		return 0, false, nil
	}
	if e.Kind != KindString {
		return 0, false, ErrWrongType
	}
	return len(e.Str), true, nil
}

// GetString is like Get but parses the value as a strictly formatted integer.
// It is the read half of INCR, INCRBY, DECR, DECRBY and SET-with-integer
// semantics.
func (s *Store) GetString(key string) (string, bool, error) {
	v, ok, err := s.Get(key)
	if !ok || err != nil {
		return "", ok, err
	}
	return string(v), true, nil
}

// Exists counts how many of the given keys are present. Repeated keys are
// counted repeatedly, matching Redis.
func (s *Store) Exists(keys ...string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	n := 0
	for _, k := range keys {
		if s.liveLocked(k, now) != nil {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Hash reads
// ---------------------------------------------------------------------------

// HashGet returns a copy of one hash field.
func (s *Store) HashGet(key, field string) (value []byte, ok bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.liveLocked(key, s.now())
	if e == nil {
		return nil, false, nil
	}
	if e.Kind != KindHash {
		return nil, false, ErrWrongType
	}
	v, found := e.Hash[field]
	if !found {
		return nil, false, nil
	}
	return append([]byte(nil), v...), true, nil
}

// HashGetMany looks up several fields at once. The result has one entry per
// requested field, in request order; a nil entry means the field is missing.
// A missing key yields all-nil results, not an error.
func (s *Store) HashGetMany(key string, fields []string) (values [][]byte, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values = make([][]byte, len(fields))
	e := s.liveLocked(key, s.now())
	if e == nil {
		return values, nil
	}
	if e.Kind != KindHash {
		return nil, ErrWrongType
	}
	for i, f := range fields {
		if v, found := e.Hash[f]; found {
			values[i] = append([]byte(nil), v...)
		}
	}
	return values, nil
}

// HashLen returns the number of fields in the hash.
func (s *Store) HashLen(key string) (n int, ok bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.liveLocked(key, s.now())
	if e == nil {
		return 0, false, nil
	}
	if e.Kind != KindHash {
		return 0, false, ErrWrongType
	}
	return len(e.Hash), true, nil
}

// HashExists reports whether a field is present.
func (s *Store) HashExists(key, field string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.liveLocked(key, s.now())
	if e == nil {
		return false, nil
	}
	if e.Kind != KindHash {
		return false, ErrWrongType
	}
	_, found := e.Hash[field]
	return found, nil
}

// HashGetAll returns every field of a hash sorted by field name.
//
// Redis returns fields in hash order, which is unspecified. MiniRedis sorts
// them so that tests, diffs and documentation are deterministic; the cost is
// O(n log n) per call.
func (s *Store) HashGetAll(key string) (pairs []FieldValue, ok bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.liveLocked(key, s.now())
	if e == nil {
		return nil, false, nil
	}
	if e.Kind != KindHash {
		return nil, false, ErrWrongType
	}
	pairs = make([]FieldValue, 0, len(e.Hash))
	for f, v := range e.Hash {
		pairs = append(pairs, FieldValue{Name: f, Value: append([]byte(nil), v...)})
	}
	sortFieldValues(pairs)
	return pairs, true, nil
}

// ---------------------------------------------------------------------------
// Expiration reads
// ---------------------------------------------------------------------------

// TTLState describes the result of a TTL query.
type TTLState uint8

// TTL query results. The numeric values are the ones the TTL command returns.
const (
	// TTLMissing means the key does not exist (-2).
	TTLMissing TTLState = iota
	// TTLNoExpiry means the key exists and never expires (-1).
	TTLNoExpiry
	// TTLRemaining means the key exists and carries a deadline.
	TTLRemaining
)

// TTL returns the remaining lifetime of a key in milliseconds.
//
// An expired key is reported as missing, so a key never returns a positive
// remaining lifetime after its deadline.
func (s *Store) TTL(key string) (remainingMS int64, state TTLState) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	e := s.liveLocked(key, now)
	if e == nil {
		return 0, TTLMissing
	}
	if e.ExpireAt == 0 {
		return 0, TTLNoExpiry
	}
	remaining := e.ExpireAt - now
	if remaining < 0 {
		// Unreachable: liveLocked already rejected expired keys. Kept as a
		// defensive guard so a positive TTL can never escape.
		return 0, TTLMissing
	}
	return remaining, TTLRemaining
}

// ExpireAt returns the key's absolute deadline in Unix milliseconds. ok is
// false when the key is missing or expired.
func (s *Store) ExpireAt(key string) (deadline int64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.liveLocked(key, s.now())
	if e == nil {
		return 0, false
	}
	return e.ExpireAt, true
}

// ---------------------------------------------------------------------------
// Mutation plumbing
// ---------------------------------------------------------------------------

// validateKeyLocked enforces the key size limit.
func (s *Store) validateKeyLocked(key string) error {
	if len(key) > s.opt.MaxKeyBytes {
		return ErrKeyTooLarge
	}
	return nil
}

// commitLocked records m and then applies it.
//
// Ordering and failure policy (see docs/PERSISTENCE.md):
//
//  1. The journal record is written first, while the write lock is held, so
//     the journal order is exactly the commit order.
//  2. If the record cannot be written at all (I/O error, poisoned journal),
//     apply is not called: the command fails and the in-memory state is
//     unchanged.
//  3. If the record reached the file but the durability barrier failed, the
//     mutation is applied anyway, because recovery would replay the record.
//     Skipping it here would make memory and journal disagree. The error is
//     still returned, so the failure is never silent.
func (s *Store) commitLocked(m Mutation, apply func()) error {
	if s.opt.Recorder == nil {
		apply()
		return nil
	}
	_, err := s.opt.Recorder.Record(m)
	switch {
	case err == nil:
		apply()
		return nil
	default:
		var ambiguous *AmbiguousAppendError
		if errors.As(err, &ambiguous) {
			apply()
		}
		return err
	}
}

// ---------------------------------------------------------------------------
// String writes
// ---------------------------------------------------------------------------

// Set writes a string value.
//
// applied is false when a NX or XX condition was not met; in that case nothing
// is recorded and nothing changes.
func (s *Store) Set(key string, value []byte, opt SetOptions) (applied bool, err error) {
	if len(value) > s.opt.MaxValueBytes {
		return false, ErrValueTooLarge
	}
	if err := s.validateKeyLocked(key); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	existing := s.liveLocked(key, now)
	switch opt.Mode {
	case SetNX:
		if existing != nil {
			return false, nil
		}
	case SetXX:
		if existing == nil {
			return false, nil
		}
	}

	deadline := opt.ExpireAt
	if opt.KeepTTL {
		if existing != nil {
			deadline = existing.ExpireAt
		} else {
			deadline = 0
		}
	}
	if deadline < 0 {
		return false, ErrInvalidExpiry
	}
	if deadline > 0 && deadline <= now {
		// Setting an already-past deadline is legal and produces a key that
		// is immediately gone. Redis accepts it, so MiniRedis does too.
		deadline = now
	}

	// Copy the value: the caller's slice must never alias stored state.
	stored := append([]byte(nil), value...)
	m := Mutation{Op: OpSet, Key: key, ExpireAt: deadline, Value: stored}
	err = s.commitLocked(m, func() {
		s.data[key] = &Entry{Kind: KindString, Str: stored, ExpireAt: deadline}
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

// MSet writes several string values atomically: every key is written, or none
// is. All deadlines are cleared, matching Redis.
func (s *Store) MSet(pairs []FieldValue) error {
	if len(pairs) == 0 {
		return nil
	}
	for _, p := range pairs {
		if len(p.Name) > s.opt.MaxKeyBytes {
			return ErrKeyTooLarge
		}
		if len(p.Value) > s.opt.MaxValueBytes {
			return ErrValueTooLarge
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Snapshot copies first: the caller's slices must not be retained.
	stored := make([]FieldValue, len(pairs))
	for i, p := range pairs {
		stored[i] = FieldValue{Name: p.Name, Value: append([]byte(nil), p.Value...)}
	}
	m := Mutation{Op: OpMSet, Pairs: stored}
	return s.commitLocked(m, func() {
		for _, p := range stored {
			s.data[p.Name] = &Entry{Kind: KindString, Str: p.Value}
		}
	})
}

// Append appends delta to the string stored under key and returns the new
// length. The existing deadline is preserved.
func (s *Store) Append(key string, delta []byte) (newLen int, err error) {
	if err := s.validateKeyLocked(key); err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	existing := s.liveLocked(key, now)
	deadline := int64(0)
	var base []byte
	if existing != nil {
		if existing.Kind != KindString {
			return 0, ErrWrongType
		}
		base = existing.Str
		deadline = existing.ExpireAt
	}
	if len(base)+len(delta) > s.opt.MaxValueBytes {
		return 0, ErrValueTooLarge
	}

	d := append([]byte(nil), delta...)
	m := Mutation{Op: OpAppend, Key: key, ExpireAt: deadline, Value: d}
	err = s.commitLocked(m, func() {
		merged := make([]byte, 0, len(base)+len(d))
		merged = append(merged, base...)
		merged = append(merged, d...)
		s.data[key] = &Entry{Kind: KindString, Str: merged, ExpireAt: deadline}
	})
	if err != nil {
		return 0, err
	}
	return len(base) + len(d), nil
}

// Incr adds delta to the integer stored under key and returns the new value.
//
// The existing deadline is preserved, matching Redis. The operation is atomic:
// the read, the bounds check and the write happen under one lock, so
// concurrent INCRs on the same key cannot lose an update.
func (s *Store) Incr(key string, delta int64) (value int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	existing := s.liveLocked(key, now)
	var current int64
	deadline := int64(0)
	if existing != nil {
		if existing.Kind != KindString {
			return 0, ErrWrongType
		}
		deadline = existing.ExpireAt
		parsed, ok := parseStrictInt(existing.Str)
		if !ok {
			return 0, ErrNotAnInteger
		}
		current = parsed
	}

	if (delta > 0 && current > maxInt64-delta) || (delta < 0 && current < minInt64-delta) {
		return 0, ErrIntOverflow
	}
	next := current + delta

	stored := []byte(strconv.FormatInt(next, 10))
	m := Mutation{Op: OpSet, Key: key, ExpireAt: deadline, Value: stored}
	err = s.commitLocked(m, func() {
		s.data[key] = &Entry{Kind: KindString, Str: stored, ExpireAt: deadline}
	})
	if err != nil {
		return 0, err
	}
	return next, nil
}

const (
	maxInt64 = int64(^uint64(0) >> 1)
	minInt64 = -maxInt64 - 1
)

// Delete removes whole keys and returns the number actually removed.
//
// The deletion is atomic across all keys and produces a single journal record,
// so a crash cannot leave half of a multi-key DEL applied.
func (s *Store) Delete(keys ...string) (removed int, err error) {
	if len(keys) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	victims := make([]string, 0, len(keys))
	for _, k := range keys {
		// A key whose deadline has passed is already logically absent, but it
		// still occupies a map slot: remove it without counting it as a DEL.
		e := s.data[k]
		if e == nil {
			continue
		}
		if e.ExpireAt > 0 && e.ExpireAt <= now {
			delete(s.data, k)
			continue
		}
		victims = append(victims, k)
	}
	if len(victims) == 0 {
		return 0, nil
	}

	m := Mutation{Op: OpDel, Keys: victims}
	if err := s.commitLocked(m, func() {
		for _, k := range victims {
			delete(s.data, k)
		}
	}); err != nil {
		return 0, err
	}
	return len(victims), nil
}

// ---------------------------------------------------------------------------
// Hash writes
// ---------------------------------------------------------------------------

// HashSet sets hash fields and returns how many fields were newly created.
// The key's deadline is preserved, matching Redis.
func (s *Store) HashSet(key string, pairs []FieldValue) (added int, err error) {
	if len(pairs) == 0 {
		return 0, nil
	}
	if err := s.validateKeyLocked(key); err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	existing := s.liveLocked(key, now)
	if existing != nil && existing.Kind != KindHash {
		return 0, ErrWrongType
	}

	// Collapse repeated fields (last value wins) so that the record, the
	// "added" count and the resulting state all agree with Redis semantics.
	stored := make([]FieldValue, 0, len(pairs))
	index := make(map[string]int, len(pairs))
	deadline := int64(0)
	if existing != nil {
		deadline = existing.ExpireAt
	}
	for _, p := range pairs {
		if len(p.Name) > s.opt.MaxFieldBytes || len(p.Value) > s.opt.MaxFieldBytes {
			return 0, ErrFieldTooLarge
		}
		if at, seen := index[p.Name]; seen {
			stored[at].Value = append([]byte(nil), p.Value...)
			continue
		}
		index[p.Name] = len(stored)
		stored = append(stored, FieldValue{Name: p.Name, Value: append([]byte(nil), p.Value...)})
	}

	// HSET returns the number of fields that did not exist before.
	newFields := 0
	for _, p := range stored {
		if existing != nil {
			if _, present := existing.Hash[p.Name]; present {
				continue
			}
		}
		newFields++
	}

	resultLen := newFields
	if existing != nil {
		resultLen = len(existing.Hash) + newFields
	}
	if resultLen > s.opt.MaxHashFields {
		return 0, ErrHashTooLarge
	}

	m := Mutation{Op: OpHashSet, Key: key, ExpireAt: deadline, Pairs: stored}
	err = s.commitLocked(m, func() {
		target := existing
		if target == nil || target.Kind != KindHash {
			target = &Entry{Kind: KindHash, Hash: make(map[string][]byte), ExpireAt: deadline}
			s.data[key] = target
		}
		for _, p := range stored {
			target.Hash[p.Name] = p.Value
		}
	})
	if err != nil {
		return 0, err
	}
	return newFields, nil
}

// HashDel removes hash fields and returns how many were removed. When the last
// field is removed, the key itself is deleted.
func (s *Store) HashDel(key string, fields []string) (removed int, err error) {
	if len(fields) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.liveLocked(key, s.now())
	if existing == nil {
		return 0, nil
	}
	if existing.Kind != KindHash {
		return 0, ErrWrongType
	}

	victims := make([]string, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		if _, dup := seen[f]; dup {
			continue
		}
		seen[f] = struct{}{}
		if _, ok := existing.Hash[f]; ok {
			victims = append(victims, f)
		}
	}
	if len(victims) == 0 {
		return 0, nil
	}

	m := Mutation{Op: OpHashDel, Key: key, Fields: victims}
	err = s.commitLocked(m, func() {
		for _, f := range victims {
			delete(existing.Hash, f)
		}
		if len(existing.Hash) == 0 {
			delete(s.data, key)
		}
	})
	if err != nil {
		return 0, err
	}
	return len(victims), nil
}

// ---------------------------------------------------------------------------
// Expiration writes
// ---------------------------------------------------------------------------

// SetExpiry sets an absolute deadline. A deadline of zero clears the deadline
// (PERSIST). exists is false when the key is missing or already expired.
//
// A call that does not change the deadline is not journalled, but still
// reports success so that the command layer matches Redis's return value.
func (s *Store) SetExpiry(key string, deadline int64) (exists bool, err error) {
	if deadline < 0 {
		return false, ErrInvalidExpiry
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	existing := s.liveLocked(key, now)
	if existing == nil {
		return false, nil
	}
	if deadline > 0 && deadline <= now {
		// Redis deletes the key when the deadline is already in the past.
		m := Mutation{Op: OpExpire, Key: key, ExpireAt: now}
		if err := s.commitLocked(m, func() {
			existing.ExpireAt = now
		}); err != nil {
			return false, err
		}
		return true, nil
	}
	if existing.ExpireAt == deadline {
		return true, nil
	}

	m := Mutation{Op: OpExpire, Key: key, ExpireAt: deadline}
	if err := s.commitLocked(m, func() { existing.ExpireAt = deadline }); err != nil {
		return false, err
	}
	return true, nil
}

// Flush removes every key. It is a single journalled record.
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.data) == 0 {
		return nil
	}
	return s.commitLocked(Mutation{Op: OpFlush}, func() {
		s.data = make(map[string]*Entry)
	})
}

// ---------------------------------------------------------------------------
// Integer parsing
// ---------------------------------------------------------------------------

// parseStrictInt parses a base-10 int64 with Redis's rules: an optional
// leading '-', at least one digit, no leading zeros (except "0" itself), no
// surrounding whitespace and no '+' sign. Anything else fails, which is what
// makes "SET k 007" + "INCR k" an error rather than a surprise.
func parseStrictInt(b []byte) (int64, bool) {
	if len(b) == 0 || len(b) > 20 {
		return 0, false
	}
	i := 0
	negative := false
	if b[0] == '-' {
		negative = true
		i++
		if len(b) == 1 {
			return 0, false
		}
	}
	if b[i] == '0' {
		// "0" and "-0" are both 0; "00" and "01" are rejected.
		if len(b)-i != 1 {
			return 0, false
		}
		return 0, true
	}
	var n uint64
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		d := uint64(c - '0')
		if n > (maxUint64-d)/10 {
			return 0, false
		}
		n = n*10 + d
	}
	if negative {
		if n > uint64(maxInt64)+1 {
			return 0, false
		}
		if n == uint64(maxInt64)+1 {
			return minInt64, true
		}
		return -int64(n), true
	}
	if n > uint64(maxInt64) {
		return 0, false
	}
	return int64(n), true
}

const maxUint64 = ^uint64(0)
