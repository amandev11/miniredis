package store

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// ActiveExpireCycle removes expired keys using randomised sampling.
//
// The cycle is bounded: it examines at most maxCheck entries and removes those
// whose deadline has passed. Go's map iteration order is randomised, so
// starting a fresh range each cycle gives an approximately uniform sample
// without maintaining a separate index of expiring keys.
//
// The cycle completes even when almost nothing is expired, and it never scans
// a large keyspace in one pass, so it cannot stall the server. The trade-off is
// that an expired key may keep occupying memory until a sample reaches it;
// every read path treats such a key as absent (see liveLocked), so this is a
// memory-retention question, not a correctness one.
//
// Returns the number of keys removed and the number of entries examined.
func (s *Store) ActiveExpireCycle(maxCheck int) (removed, examined int) {
	if maxCheck <= 0 {
		return 0, 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	for key, e := range s.data {
		if e.ExpireAt > 0 && e.ExpireAt <= now {
			delete(s.data, key)
			removed++
		}
		examined++
		if examined >= maxCheck {
			break
		}
	}
	return removed, examined
}

// PurgeExpired removes every expired key and returns how many were removed.
//
// This is a full scan and is therefore O(keyspace). It is used by snapshot
// creation, where the whole keyspace is being walked anyway, and by the
// FLUSHALL-style administrative paths. It is never called from a read or write
// path of a single command.
func (s *Store) PurgeExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	removed := 0
	for key, e := range s.data {
		if e.ExpireAt > 0 && e.ExpireAt <= now {
			delete(s.data, key)
			removed++
		}
	}
	return removed
}

// Snapshot captures a consistent point-in-time view of the keyspace.
//
// The write lock is held only for the copy, never for serialisation, so a
// multi-gigabyte snapshot does not block the server. Keys that are already
// expired are dropped from the view, and Seq records the last mutation that is
// included. Journal records with a higher sequence number are replayed on top
// of the restored snapshot, which is what makes snapshotting safe while
// clients keep writing.
func (s *Store) Snapshot() *Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	items := make([]SnapshotItem, 0, len(s.data))
	for key, e := range s.data {
		if e.ExpireAt > 0 && e.ExpireAt <= now {
			// Drop expired keys from the view instead of serialising data
			// that is already logically gone.
			delete(s.data, key)
			continue
		}
		item := SnapshotItem{Key: key, Kind: e.Kind, ExpireAt: e.ExpireAt}
		switch e.Kind {
		case KindString:
			item.Value = append([]byte(nil), e.Str...)
		case KindHash:
			item.Pairs = make([]FieldValue, 0, len(e.Hash))
			for f, v := range e.Hash {
				item.Pairs = append(item.Pairs, FieldValue{Name: f, Value: append([]byte(nil), v...)})
			}
			sortFieldValues(item.Pairs)
		}
		items = append(items, item)
	}
	slices.SortFunc(items, func(a, b SnapshotItem) int {
		switch {
		case a.Key < b.Key:
			return -1
		case a.Key > b.Key:
			return 1
		default:
			return 0
		}
	})

	var seq uint64
	if s.opt.Recorder != nil {
		seq = s.opt.Recorder.Seq()
	}
	return &Snapshot{Seq: seq, Items: items, CreatedAt: time.Now()}
}

// ApplySnapshot replaces the keyspace with a recovered snapshot.
//
// It is recovery-only: it must not be called once the store is serving
// clients, and it never writes journal records. Replaying a snapshot would
// otherwise duplicate every mutation in the journal.
func (s *Store) ApplySnapshot(snap *Snapshot) error {
	if snap == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	data := make(map[string]*Entry, len(snap.Items))
	for _, item := range snap.Items {
		entry := &Entry{Kind: item.Kind, ExpireAt: item.ExpireAt}
		switch item.Kind {
		case KindString:
			entry.Str = append([]byte(nil), item.Value...)
		case KindHash:
			entry.Hash = make(map[string][]byte, len(item.Pairs))
			for _, p := range item.Pairs {
				entry.Hash[p.Name] = append([]byte(nil), p.Value...)
			}
		default:
			return fmt.Errorf("snapshot contains key %q with unknown kind %d", item.Key, item.Kind)
		}
		if entry.ExpireAt < 0 {
			return fmt.Errorf("snapshot contains key %q with a negative deadline", item.Key)
		}
		data[item.Key] = entry
	}
	s.data = data
	return nil
}

// Replay applies one journalled mutation without recording it.
//
// Recovery-only. It is strict about consistency: a record that cannot have
// been produced by this store (for example HSET on a key holding a string)
// returns an error instead of silently producing a different keyspace.
func (s *Store) Replay(m Mutation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch m.Op {
	case OpSet:
		s.data[m.Key] = &Entry{
			Kind:     KindString,
			Str:      append([]byte(nil), m.Value...),
			ExpireAt: m.ExpireAt,
		}

	case OpMSet:
		for _, p := range m.Pairs {
			s.data[p.Name] = &Entry{Kind: KindString, Str: append([]byte(nil), p.Value...)}
		}

	case OpDel:
		for _, k := range m.Keys {
			delete(s.data, k)
		}

	case OpAppend:
		existing := s.data[m.Key]
		if existing != nil && existing.Kind != KindString {
			return fmt.Errorf("journal APPEND %q targets a %s value", m.Key, existing.Kind)
		}
		if existing == nil {
			s.data[m.Key] = &Entry{
				Kind:     KindString,
				Str:      append([]byte(nil), m.Value...),
				ExpireAt: m.ExpireAt,
			}
			break
		}
		merged := make([]byte, 0, len(existing.Str)+len(m.Value))
		merged = append(merged, existing.Str...)
		merged = append(merged, m.Value...)
		existing.Str = merged
		existing.ExpireAt = m.ExpireAt

	case OpHashSet:
		existing := s.data[m.Key]
		if existing != nil && existing.Kind != KindHash {
			return fmt.Errorf("journal HSET %q targets a %s value", m.Key, existing.Kind)
		}
		if existing == nil {
			existing = &Entry{Kind: KindHash, Hash: make(map[string][]byte), ExpireAt: m.ExpireAt}
			s.data[m.Key] = existing
		}
		for _, p := range m.Pairs {
			existing.Hash[p.Name] = append([]byte(nil), p.Value...)
		}
		existing.ExpireAt = m.ExpireAt

	case OpHashDel:
		existing := s.data[m.Key]
		if existing == nil {
			// The key is missing in the replayed state, so the record cannot
			// be applied. This is reported instead of being swallowed.
			return fmt.Errorf("%w: HDEL %q", ErrReplayOrphan, m.Key)
		}
		if existing.Kind != KindHash {
			return fmt.Errorf("journal HDEL %q targets a %s value", m.Key, existing.Kind)
		}
		for _, f := range m.Fields {
			delete(existing.Hash, f)
		}
		if len(existing.Hash) == 0 {
			delete(s.data, m.Key)
		}

	case OpExpire:
		existing := s.data[m.Key]
		if existing == nil {
			return fmt.Errorf("%w: EXPIRE %q", ErrReplayOrphan, m.Key)
		}
		existing.ExpireAt = m.ExpireAt

	case OpFlush:
		s.data = make(map[string]*Entry)

	default:
		return fmt.Errorf("journal record uses unknown operation %d", uint8(m.Op))
	}
	return nil
}

// ValidateReplay guards against a journal that would produce an entry this
// store cannot represent. It is separated from Replay only to keep Replay's
// switch readable; recovery calls ValidateReplay before Replay.
func (s *Store) ValidateReplay(m Mutation) error {
	if m.ExpireAt < 0 {
		return errors.New("journal record carries a negative deadline")
	}
	switch m.Op {
	case OpSet, OpMSet, OpDel, OpAppend, OpHashSet, OpHashDel, OpExpire, OpFlush:
		return nil
	default:
		return fmt.Errorf("journal record uses unknown operation %d", uint8(m.Op))
	}
}

func sortFieldValues(pairs []FieldValue) {
	slices.SortFunc(pairs, func(a, b FieldValue) int {
		switch {
		case a.Name < b.Name:
			return -1
		case a.Name > b.Name:
			return 1
		default:
			return 0
		}
	})
}
