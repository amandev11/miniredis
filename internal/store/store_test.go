package store_test

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/amandev11/miniredis/internal/clock"
	"github.com/amandev11/miniredis/internal/store"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// memRecorder is an in-memory store.Recorder. It is used to assert *what* the
// store journals and to inject failures; the real on-disk journal is exercised
// by internal/persistence.
type memRecorder struct {
	mu        sync.Mutex
	mutations []store.Mutation
	seq       uint64
	err       error
	ambiguous bool
}

func (r *memRecorder) Record(m store.Mutation) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		if r.ambiguous {
			return 0, &store.AmbiguousAppendError{Err: r.err}
		}
		return 0, r.err
	}
	r.seq++
	r.mutations = append(r.mutations, m)
	return r.seq, nil
}

func (r *memRecorder) Seq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seq
}

func (r *memRecorder) recorded() []store.Mutation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]store.Mutation(nil), r.mutations...)
}

func (r *memRecorder) fail(err error, ambiguous bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
	r.ambiguous = ambiguous
}

func (r *memRecorder) heal() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = nil
}

const startMS = int64(1_700_000_000_000)

func newStore(t *testing.T, rec store.Recorder) (*store.Store, *clock.Manual) {
	t.Helper()
	clk := clock.NewManual(startMS)
	return store.New(clk, store.Options{Recorder: rec}), clk
}

func fv(name, value string) store.FieldValue {
	return store.FieldValue{Name: name, Value: []byte(value)}
}

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

func TestSetGet(t *testing.T) {
	s, _ := newStore(t, nil)

	applied, err := s.Set("k", []byte("v"), store.SetOptions{})
	if err != nil || !applied {
		t.Fatalf("Set: applied=%v err=%v", applied, err)
	}
	got, ok, err := s.Get("k")
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if string(got) != "v" {
		t.Fatalf("Get = %q, want %q", got, "v")
	}
}

func TestGetMissingKey(t *testing.T) {
	s, _ := newStore(t, nil)
	got, ok, err := s.Get("nope")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok || got != nil {
		t.Fatalf("got (%q, %v), want (nil, false)", got, ok)
	}
}

func TestSetOverwrite(t *testing.T) {
	s, _ := newStore(t, nil)
	mustSet(t, s, "k", "first")
	mustSet(t, s, "k", "second-longer")

	got, _, _ := s.Get("k")
	if string(got) != "second-longer" {
		t.Fatalf("got %q, want %q", got, "second-longer")
	}
	if n := s.Len(); n != 1 {
		t.Fatalf("Len() = %d, want 1", n)
	}
}

// TestStoredValuesAreIsolated proves that callers can neither mutate stored
// state through a returned slice nor through the slice they wrote.
func TestStoredValuesAreIsolated(t *testing.T) {
	s, _ := newStore(t, nil)

	written := []byte("hello")
	if _, err := s.Set("k", written, store.SetOptions{}); err != nil {
		t.Fatal(err)
	}
	written[0] = 'X' // must not affect stored state

	got, _, _ := s.Get("k")
	if string(got) != "hello" {
		t.Fatalf("stored value changed to %q after the caller mutated its slice", got)
	}
	got[1] = 'X' // must not affect stored state either
	again, _, _ := s.Get("k")
	if string(again) != "hello" {
		t.Fatalf("stored value changed to %q after the caller mutated the returned slice", again)
	}
}

func TestBinarySafeValues(t *testing.T) {
	s, _ := newStore(t, nil)
	key := "key\x00with\r\nbinary"
	value := []byte{0x00, 0xff, '\r', '\n', 0x7f, 'a'}

	if _, err := s.Set(key, value, store.SetOptions{}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.Get(key)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("got %q, want %q", got, value)
	}
}

func TestSetNX(t *testing.T) {
	s, _ := newStore(t, nil)

	applied, err := s.Set("k", []byte("first"), store.SetOptions{Mode: store.SetNX})
	if err != nil || !applied {
		t.Fatalf("first NX: applied=%v err=%v", applied, err)
	}
	applied, err = s.Set("k", []byte("second"), store.SetOptions{Mode: store.SetNX})
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("NX overwrote an existing key")
	}
	got, _, _ := s.Get("k")
	if string(got) != "first" {
		t.Fatalf("got %q, want %q", got, "first")
	}
}

func TestSetXX(t *testing.T) {
	s, _ := newStore(t, nil)

	applied, err := s.Set("k", []byte("v"), store.SetOptions{Mode: store.SetXX})
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("XX created a key that did not exist")
	}
	if _, ok, _ := s.Get("k"); ok {
		t.Fatal("XX created the key anyway")
	}

	mustSet(t, s, "k", "v")
	applied, err = s.Set("k", []byte("v2"), store.SetOptions{Mode: store.SetXX})
	if err != nil || !applied {
		t.Fatalf("XX on an existing key: applied=%v err=%v", applied, err)
	}
}

func TestSetKeepTTL(t *testing.T) {
	s, clk := newStore(t, nil)

	if _, err := s.Set("k", []byte("v"), store.SetOptions{ExpireAt: startMS + 10_000}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(5 * time.Second)

	// A plain SET clears the deadline.
	mustSet(t, s, "k", "v2")
	if _, st := s.TTL("k"); st != store.TTLNoExpiry {
		t.Fatalf("plain SET kept the deadline: state=%v", st)
	}

	if _, err := s.Set("k", []byte("v3"), store.SetOptions{ExpireAt: startMS + 20_000}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Set("k", []byte("v4"), store.SetOptions{KeepTTL: true}); err != nil {
		t.Fatal(err)
	}
	remaining, st := s.TTL("k")
	if st != store.TTLRemaining {
		t.Fatalf("KEEPTTL lost the deadline: state=%v", st)
	}
	if want := int64(15_000); remaining != want {
		t.Fatalf("remaining = %d, want %d", remaining, want)
	}
}

func TestAppend(t *testing.T) {
	s, _ := newStore(t, nil)

	n, err := s.Append("k", []byte("abc"))
	if err != nil || n != 3 {
		t.Fatalf("Append: n=%d err=%v", n, err)
	}
	n, err = s.Append("k", []byte("de"))
	if err != nil || n != 5 {
		t.Fatalf("Append: n=%d err=%v", n, err)
	}
	got, _, _ := s.Get("k")
	if string(got) != "abcde" {
		t.Fatalf("got %q, want %q", got, "abcde")
	}

	// Appending nothing is a no-op that still reports the length.
	if n, err := s.Append("other", nil); err != nil || n != 0 {
		t.Fatalf("Append(nil): n=%d err=%v", n, err)
	}
	if _, ok, _ := s.Get("other"); !ok {
		t.Fatal("APPEND with an empty payload should create the key, as in Redis")
	}
}

func TestAppendPreservesTTL(t *testing.T) {
	s, clk := newStore(t, nil)
	if _, err := s.Set("k", []byte("a"), store.SetOptions{ExpireAt: startMS + 5_000}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Second)
	if _, err := s.Append("k", []byte("b")); err != nil {
		t.Fatal(err)
	}
	remaining, st := s.TTL("k")
	if st != store.TTLRemaining || remaining != 4_000 {
		t.Fatalf("TTL after APPEND = (%d, %v), want (4000, remaining)", remaining, st)
	}
}

func TestStrLen(t *testing.T) {
	s, _ := newStore(t, nil)
	mustSet(t, s, "k", "hello")

	if n, ok, err := s.StrLen("k"); err != nil || !ok || n != 5 {
		t.Fatalf("StrLen = (%d, %v, %v), want (5, true, nil)", n, ok, err)
	}
	if _, ok, err := s.StrLen("missing"); err != nil || ok {
		t.Fatalf("StrLen(missing) = (_, %v, %v), want (false, nil)", ok, err)
	}
}

// ---------------------------------------------------------------------------
// Integers
// ---------------------------------------------------------------------------

func TestIncr(t *testing.T) {
	s, _ := newStore(t, nil)

	got, err := s.Incr("n", 1)
	if err != nil || got != 1 {
		t.Fatalf("Incr = (%d, %v), want (1, nil)", got, err)
	}
	got, err = s.Incr("n", 41)
	if err != nil || got != 42 {
		t.Fatalf("Incr = (%d, %v), want (42, nil)", got, err)
	}
	got, err = s.Incr("n", -50)
	if err != nil || got != -8 {
		t.Fatalf("Incr = (%d, %v), want (-8, nil)", got, err)
	}

	value, _, _ := s.Get("n")
	if string(value) != "-8" {
		t.Fatalf("stored value = %q, want %q", value, "-8")
	}
}

func TestIncrRejectsNonInteger(t *testing.T) {
	s, _ := newStore(t, nil)
	for _, bad := range []string{"abc", "1.5", " 1", "1 ", "007", "0x10", "", "1e3", "+3", "99999999999999999999"} {
		mustSet(t, s, "k", bad)
		if _, err := s.Incr("k", 1); !errors.Is(err, store.ErrNotAnInteger) {
			t.Fatalf("Incr on %q returned %v, want ErrNotAnInteger", bad, err)
		}
	}
}

func TestIncrAcceptsCanonicalIntegers(t *testing.T) {
	s, _ := newStore(t, nil)
	cases := map[string]int64{
		"0":                    1,
		"1":                    2,
		"-1":                   0,
		"9223372036854775806":  9223372036854775807,
		"-9223372036854775807": -9223372036854775806,
	}
	for input, want := range cases {
		mustSet(t, s, "k", input)
		got, err := s.Incr("k", 1)
		if err != nil {
			t.Fatalf("Incr(%q): %v", input, err)
		}
		if got != want {
			t.Fatalf("Incr(%q) = %d, want %d", input, got, want)
		}
	}
}

func TestIncrOverflow(t *testing.T) {
	s, _ := newStore(t, nil)

	mustSet(t, s, "max", "9223372036854775807")
	if _, err := s.Incr("max", 1); !errors.Is(err, store.ErrIntOverflow) {
		t.Fatalf("got %v, want ErrIntOverflow", err)
	}
	// The failed increment must not have changed the value.
	value, _, _ := s.Get("max")
	if string(value) != "9223372036854775807" {
		t.Fatalf("value changed to %q after a rejected increment", value)
	}

	mustSet(t, s, "min", "-9223372036854775808")
	if _, err := s.Incr("min", -1); !errors.Is(err, store.ErrIntOverflow) {
		t.Fatalf("got %v, want ErrIntOverflow", err)
	}

	// Boundary cases that must succeed.
	mustSet(t, s, "edge", "-1")
	if got, err := s.Incr("edge", 1); err != nil || got != 0 {
		t.Fatalf("Incr(-1, +1) = (%d, %v)", got, err)
	}
}

func TestIncrPreservesTTL(t *testing.T) {
	s, clk := newStore(t, nil)
	if _, err := s.Set("n", []byte("1"), store.SetOptions{ExpireAt: startMS + 9_000}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(3 * time.Second)
	if _, err := s.Incr("n", 1); err != nil {
		t.Fatal(err)
	}
	if remaining, st := s.TTL("n"); st != store.TTLRemaining || remaining != 6_000 {
		t.Fatalf("TTL after INCR = (%d, %v), want (6000, remaining)", remaining, st)
	}
}

// ---------------------------------------------------------------------------
// Deletion and existence
// ---------------------------------------------------------------------------

func TestDelete(t *testing.T) {
	s, _ := newStore(t, nil)
	mustSet(t, s, "a", "1")
	mustSet(t, s, "b", "2")
	mustSet(t, s, "c", "3")

	n, err := s.Delete("a", "c", "missing")
	if err != nil || n != 2 {
		t.Fatalf("Delete = (%d, %v), want (2, nil)", n, err)
	}
	if s.Exists("a", "c") != 0 {
		t.Fatal("deleted keys still exist")
	}
	if s.Exists("b") != 1 {
		t.Fatal("surviving key disappeared")
	}
}

func TestExistsCountsDuplicates(t *testing.T) {
	s, _ := newStore(t, nil)
	mustSet(t, s, "a", "1")
	if got := s.Exists("a", "a", "b"); got != 2 {
		t.Fatalf("Exists = %d, want 2 (Redis counts duplicates)", got)
	}
}

func TestDeleteSkipsAlreadyExpiredEntries(t *testing.T) {
	s, clk := newStore(t, nil)
	mustSet(t, s, "gone", "1")
	if _, err := s.SetExpiry("gone", startMS+1); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Second)

	n, err := s.Delete("gone")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("Delete reported %d, want 0 (the key was already gone)", n)
	}
	// The map slot must still be reclaimed.
	s.PurgeExpired()
	if _, ok, _ := s.Get("gone"); ok {
		t.Fatal("key came back")
	}
}

// ---------------------------------------------------------------------------
// Hashes
// ---------------------------------------------------------------------------

func TestHashBasics(t *testing.T) {
	s, _ := newStore(t, nil)

	added, err := s.HashSet("h", []store.FieldValue{fv("a", "1"), fv("b", "2")})
	if err != nil || added != 2 {
		t.Fatalf("HashSet = (%d, %v), want (2, nil)", added, err)
	}
	added, err = s.HashSet("h", []store.FieldValue{fv("a", "updated"), fv("c", "3")})
	if err != nil || added != 1 {
		t.Fatalf("HashSet = (%d, %v), want (1, nil)", added, err)
	}

	if v, ok, err := s.HashGet("h", "a"); err != nil || !ok || string(v) != "updated" {
		t.Fatalf("HashGet = (%q, %v, %v)", v, ok, err)
	}
	if v, ok, err := s.HashGet("h", "missing"); err != nil || ok || v != nil {
		t.Fatalf("HashGet(missing) = (%q, %v, %v)", v, ok, err)
	}
	if n, ok, err := s.HashLen("h"); err != nil || !ok || n != 3 {
		t.Fatalf("HashLen = (%d, %v, %v), want (3, true, nil)", n, ok, err)
	}
	if ok, err := s.HashExists("h", "c"); err != nil || !ok {
		t.Fatalf("HashExists = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := s.HashExists("h", "zzz"); err != nil || ok {
		t.Fatalf("HashExists = (%v, %v), want (false, nil)", ok, err)
	}
}

func TestHashSetDuplicateFieldsInOneCall(t *testing.T) {
	s, _ := newStore(t, nil)

	added, err := s.HashSet("h", []store.FieldValue{fv("a", "1"), fv("a", "2"), fv("a", "3")})
	if err != nil {
		t.Fatal(err)
	}
	if added != 1 {
		t.Fatalf("added = %d, want 1 (a repeated field is one new field)", added)
	}
	if v, _, _ := s.HashGet("h", "a"); string(v) != "3" {
		t.Fatalf("value = %q, want the last one (%q)", v, "3")
	}
	if n, _, _ := s.HashLen("h"); n != 1 {
		t.Fatalf("HashLen = %d, want 1", n)
	}
}

func TestHashGetMany(t *testing.T) {
	s, _ := newStore(t, nil)
	if _, err := s.HashSet("h", []store.FieldValue{fv("a", "1"), fv("c", "3")}); err != nil {
		t.Fatal(err)
	}

	values, err := s.HashGetMany("h", []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 {
		t.Fatalf("got %d values, want 3", len(values))
	}
	if string(values[0]) != "1" || values[1] != nil || string(values[2]) != "3" {
		t.Fatalf("got %q", values)
	}

	missing, err := s.HashGetMany("absent", []string{"a"})
	if err != nil {
		t.Fatalf("missing key should not error: %v", err)
	}
	if len(missing) != 1 || missing[0] != nil {
		t.Fatalf("got %q, want one nil entry", missing)
	}
}

func TestHashGetAllIsSorted(t *testing.T) {
	s, _ := newStore(t, nil)
	// Insert in deliberately unsorted order.
	if _, err := s.HashSet("h", []store.FieldValue{fv("c", "3"), fv("a", "1"), fv("b", "2")}); err != nil {
		t.Fatal(err)
	}

	pairs, ok, err := s.HashGetAll("h")
	if err != nil || !ok {
		t.Fatalf("HashGetAll = (_, %v, %v)", ok, err)
	}
	want := []string{"a", "b", "c"}
	for i, p := range pairs {
		if p.Name != want[i] {
			t.Fatalf("pair %d = %q, want %q (order must be deterministic)", i, p.Name, want[i])
		}
	}
}

func TestHashDel(t *testing.T) {
	s, _ := newStore(t, nil)
	if _, err := s.HashSet("h", []store.FieldValue{fv("a", "1"), fv("b", "2")}); err != nil {
		t.Fatal(err)
	}

	n, err := s.HashDel("h", []string{"a", "nope", "a"})
	if err != nil || n != 1 {
		t.Fatalf("HashDel = (%d, %v), want (1, nil)", n, err)
	}
	if _, ok, _ := s.HashGet("h", "a"); ok {
		t.Fatal("field a survived HDEL")
	}

	// Removing the last field removes the key.
	if n, err := s.HashDel("h", []string{"b"}); err != nil || n != 1 {
		t.Fatalf("HashDel = (%d, %v)", n, err)
	}
	if _, ok := s.Kind("h"); ok {
		t.Fatal("the key survived the removal of its last field")
	}
}

func TestHashPreservesTTL(t *testing.T) {
	s, clk := newStore(t, nil)
	if _, err := s.Set("k", []byte("v"), store.SetOptions{ExpireAt: startMS + 10_000}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Second)
	if _, err := s.Delete("k"); err != nil {
		t.Fatal(err)
	}

	if _, err := s.HashSet("h", []store.FieldValue{fv("a", "1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetExpiry("h", startMS+9_000); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HashSet("h", []store.FieldValue{fv("b", "2")}); err != nil {
		t.Fatal(err)
	}
	if remaining, st := s.TTL("h"); st != store.TTLRemaining || remaining != 8_000 {
		t.Fatalf("TTL after HSET = (%d, %v), want (8000, remaining)", remaining, st)
	}
}

// ---------------------------------------------------------------------------
// Type safety
// ---------------------------------------------------------------------------

func TestWrongTypeErrors(t *testing.T) {
	s, _ := newStore(t, nil)
	mustSet(t, s, "str", "value")
	if _, err := s.HashSet("str", []store.FieldValue{fv("f", "v")}); !errors.Is(err, store.ErrWrongType) {
		t.Fatalf("HSET on a string: got %v, want ErrWrongType", err)
	}

	if _, err := s.HashSet("hash", []store.FieldValue{fv("f", "v")}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Get("hash"); !errors.Is(err, store.ErrWrongType) {
		t.Fatalf("GET on a hash: got %v, want ErrWrongType", err)
	}
	if _, _, err := s.StrLen("hash"); !errors.Is(err, store.ErrWrongType) {
		t.Fatalf("STRLEN on a hash: got %v, want ErrWrongType", err)
	}
	if _, err := s.Incr("hash", 1); !errors.Is(err, store.ErrWrongType) {
		t.Fatalf("INCR on a hash: got %v, want ErrWrongType", err)
	}
	if _, err := s.Append("hash", []byte("x")); !errors.Is(err, store.ErrWrongType) {
		t.Fatalf("APPEND on a hash: got %v, want ErrWrongType", err)
	}
	if _, _, err := s.HashGet("str", "f"); !errors.Is(err, store.ErrWrongType) {
		t.Fatalf("HGET on a string: got %v, want ErrWrongType", err)
	}
	if _, err := s.HashDel("str", []string{"f"}); !errors.Is(err, store.ErrWrongType) {
		t.Fatalf("HDEL on a string: got %v, want ErrWrongType", err)
	}

	// A rejected operation must leave the value untouched.
	got, _, _ := s.Get("str")
	if string(got) != "value" {
		t.Fatalf("value changed to %q after a rejected operation", got)
	}
}

func TestKind(t *testing.T) {
	s, _ := newStore(t, nil)
	mustSet(t, s, "str", "v")
	if _, err := s.HashSet("hash", []store.FieldValue{fv("f", "v")}); err != nil {
		t.Fatal(err)
	}

	if k, ok := s.Kind("str"); !ok || k != store.KindString {
		t.Fatalf("Kind(str) = (%v, %v)", k, ok)
	}
	if k, ok := s.Kind("hash"); !ok || k != store.KindHash {
		t.Fatalf("Kind(hash) = (%v, %v)", k, ok)
	}
	if _, ok := s.Kind("missing"); ok {
		t.Fatal("Kind(missing) reported a kind")
	}
}

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

func TestLimitsAreEnforced(t *testing.T) {
	clk := clock.NewManual(startMS)
	s := store.New(clk, store.Options{MaxValueBytes: 8, MaxKeyBytes: 4, MaxHashFields: 2, MaxFieldBytes: 8})

	if _, err := s.Set("key", []byte("123456789"), store.SetOptions{}); !errors.Is(err, store.ErrValueTooLarge) {
		t.Fatalf("got %v, want ErrValueTooLarge", err)
	}
	if _, err := s.Set("toolongkey", []byte("v"), store.SetOptions{}); !errors.Is(err, store.ErrKeyTooLarge) {
		t.Fatalf("got %v, want ErrKeyTooLarge", err)
	}
	if _, err := s.Append("key", []byte("123456789")); !errors.Is(err, store.ErrValueTooLarge) {
		t.Fatalf("got %v, want ErrValueTooLarge", err)
	}
	if _, err := s.HashSet("h", []store.FieldValue{fv("a", "123456789")}); !errors.Is(err, store.ErrFieldTooLarge) {
		t.Fatalf("got %v, want ErrFieldTooLarge", err)
	}
	if _, err := s.HashSet("h", []store.FieldValue{fv("a", "1"), fv("b", "2"), fv("c", "3")}); !errors.Is(err, store.ErrHashTooLarge) {
		t.Fatalf("got %v, want ErrHashTooLarge", err)
	}
	if _, err := s.Set("k", []byte("12345678"), store.SetOptions{}); err != nil {
		t.Fatalf("value exactly at the limit was rejected: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TTL and expiration
// ---------------------------------------------------------------------------

func TestTTLStates(t *testing.T) {
	s, clk := newStore(t, nil)

	if _, st := s.TTL("missing"); st != store.TTLMissing {
		t.Fatalf("TTL(missing) state = %v, want TTLMissing", st)
	}

	mustSet(t, s, "persistent", "v")
	if _, st := s.TTL("persistent"); st != store.TTLNoExpiry {
		t.Fatalf("TTL(persistent) state = %v, want TTLNoExpiry", st)
	}

	if _, err := s.Set("volatile", []byte("v"), store.SetOptions{ExpireAt: startMS + 30_000}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(10 * time.Second)
	remaining, st := s.TTL("volatile")
	if st != store.TTLRemaining {
		t.Fatalf("state = %v, want TTLRemaining", st)
	}
	if remaining != 20_000 {
		t.Fatalf("remaining = %d, want 20000", remaining)
	}
}

func TestExpiredKeyBehavesAsMissing(t *testing.T) {
	s, clk := newStore(t, nil)
	if _, err := s.Set("k", []byte("v"), store.SetOptions{ExpireAt: startMS + 1_000}); err != nil {
		t.Fatal(err)
	}

	if _, ok, _ := s.Get("k"); !ok {
		t.Fatal("key expired too early")
	}
	clk.Advance(1_000 * time.Millisecond)

	if _, ok, _ := s.Get("k"); ok {
		t.Fatal("expired key is still readable")
	}
	if s.Exists("k") != 0 {
		t.Fatal("expired key still exists")
	}
	if _, ok := s.Kind("k"); ok {
		t.Fatal("expired key still reports a kind")
	}
	if _, st := s.TTL("k"); st != store.TTLMissing {
		t.Fatal("expired key does not report TTLMissing")
	}
	if s.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", s.Len())
	}

	// No positive TTL may ever be reported for an expired key.
	if remaining, _ := s.TTL("k"); remaining > 0 {
		t.Fatalf("TTL returned %d for an expired key", remaining)
	}
}

func TestExpireAtExactlyNowIsExpired(t *testing.T) {
	s, clk := newStore(t, nil)
	if _, err := s.Set("k", []byte("v"), store.SetOptions{ExpireAt: startMS}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Get("k"); ok {
		t.Fatal("a deadline equal to now must already be expired")
	}
	clk.Advance(time.Second)
	if _, ok, _ := s.Get("k"); ok {
		t.Fatal("key is still present")
	}
}

func TestOverwriteResetsExpiration(t *testing.T) {
	s, clk := newStore(t, nil)
	if _, err := s.Set("k", []byte("v"), store.SetOptions{ExpireAt: startMS + 1_000}); err != nil {
		t.Fatal(err)
	}
	// A plain overwrite must clear the deadline.
	mustSet(t, s, "k", "v2")
	clk.Advance(5 * time.Second)
	if _, ok, _ := s.Get("k"); !ok {
		t.Fatal("overwriting did not clear the deadline")
	}
}

func TestSetExpiryAndPersist(t *testing.T) {
	s, clk := newStore(t, nil)
	mustSet(t, s, "k", "v")

	if ok, err := s.SetExpiry("k", startMS+5_000); err != nil || !ok {
		t.Fatalf("SetExpiry = (%v, %v)", ok, err)
	}
	clk.Advance(time.Second)
	if remaining, st := s.TTL("k"); st != store.TTLRemaining || remaining != 4_000 {
		t.Fatalf("TTL = (%d, %v)", remaining, st)
	}

	// PERSIST clears the deadline.
	if ok, err := s.SetExpiry("k", 0); err != nil || !ok {
		t.Fatalf("PERSIST = (%v, %v)", ok, err)
	}
	if _, st := s.TTL("k"); st != store.TTLNoExpiry {
		t.Fatalf("state = %v, want TTLNoExpiry", st)
	}

	if ok, err := s.SetExpiry("missing", startMS+1_000); err != nil || ok {
		t.Fatalf("SetExpiry on a missing key = (%v, %v), want (false, nil)", ok, err)
	}
}

func TestSetExpiryInThePastMakesKeyImmediatelyGone(t *testing.T) {
	s, _ := newStore(t, nil)
	mustSet(t, s, "k", "v")
	if ok, err := s.SetExpiry("k", startMS-1); err != nil || !ok {
		t.Fatalf("SetExpiry = (%v, %v)", ok, err)
	}
	if _, ok, _ := s.Get("k"); ok {
		t.Fatal("key with a past deadline is still readable")
	}
}

func TestActiveExpireCycle(t *testing.T) {
	s, clk := newStore(t, nil)
	for i := 0; i < 50; i++ {
		if _, err := s.Set(fmt.Sprintf("vol-%d", i), []byte("v"), store.SetOptions{ExpireAt: startMS + 500}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 50; i++ {
		mustSet(t, s, fmt.Sprintf("keep-%d", i), "v")
	}

	// Nothing is expired yet.
	if removed, _ := s.ActiveExpireCycle(1000); removed != 0 {
		t.Fatalf("removed %d keys before any deadline passed", removed)
	}

	clk.Advance(time.Second)

	// A bounded cycle must never examine more entries than its budget.
	if _, examined := s.ActiveExpireCycle(10); examined > 10 {
		t.Fatalf("cycle examined %d entries, above its budget of 10", examined)
	}

	// Repeated bounded cycles eventually clear everything expired.
	for i := 0; i < 200 && s.Len() > 50; i++ {
		s.ActiveExpireCycle(64)
	}
	if got := s.Len(); got != 50 {
		t.Fatalf("Len() = %d after sweeping, want 50", got)
	}
	if got := s.ExpiresCount(); got != 0 {
		t.Fatalf("ExpiresCount() = %d, want 0", got)
	}
}

func TestActiveExpireCycleZeroBudget(t *testing.T) {
	s, _ := newStore(t, nil)
	mustSet(t, s, "k", "v")
	if removed, examined := s.ActiveExpireCycle(0); removed != 0 || examined != 0 {
		t.Fatalf("cycle with a zero budget did work: (%d, %d)", removed, examined)
	}
}

func TestPurgeExpired(t *testing.T) {
	s, clk := newStore(t, nil)
	for i := 0; i < 10; i++ {
		if _, err := s.Set(fmt.Sprintf("k%d", i), []byte("v"), store.SetOptions{ExpireAt: startMS + 100}); err != nil {
			t.Fatal(err)
		}
	}
	clk.Advance(time.Second)

	if removed := s.PurgeExpired(); removed != 10 {
		t.Fatalf("PurgeExpired removed %d, want 10", removed)
	}
	if s.Len() != 0 {
		t.Fatal("keys survived the purge")
	}
}

// ---------------------------------------------------------------------------
// Journaling behaviour of the store
// ---------------------------------------------------------------------------

func TestMutationsAreRecorded(t *testing.T) {
	rec := &memRecorder{}
	s, _ := newStore(t, rec)

	mustSet(t, s, "k", "v")
	if _, err := s.SetExpiry("k", startMS+1_000); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append("k", []byte("!")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HashSet("h", []store.FieldValue{fv("f", "v")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HashDel("h", []string{"f"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Incr("n", 5); err != nil {
		t.Fatal(err)
	}
	if err := s.MSet([]store.FieldValue{fv("a", "1"), fv("b", "2")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	wantOps := []store.Op{
		store.OpSet, store.OpExpire, store.OpAppend, store.OpHashSet,
		store.OpHashDel, store.OpSet, store.OpMSet, store.OpDel, store.OpFlush,
	}
	got := rec.recorded()
	if len(got) != len(wantOps) {
		t.Fatalf("recorded %d mutations, want %d: %v", len(got), len(wantOps), got)
	}
	for i, want := range wantOps {
		if got[i].Op != want {
			t.Errorf("mutation %d = %v, want %v", i, got[i].Op, want)
		}
	}

	// Deadlines must be absolute post-state values, never relative TTLs.
	expireRec := got[1]
	if expireRec.ExpireAt != startMS+1_000 {
		t.Errorf("EXPIRE record carries %d, want the absolute deadline %d", expireRec.ExpireAt, startMS+1_000)
	}
	// A no-op must not be recorded.
	before := len(rec.recorded())
	if _, err := s.Delete("definitely-missing"); err != nil {
		t.Fatal(err)
	}
	if after := len(rec.recorded()); after != before {
		t.Fatalf("a no-op recorded %d extra mutations", after-before)
	}
}

func TestFailedRecordLeavesStateUnchanged(t *testing.T) {
	rec := &memRecorder{}
	s, _ := newStore(t, rec)
	mustSet(t, s, "k", "original")

	rec.fail(errors.New("disk unavailable"), false)

	if _, err := s.Set("k", []byte("new"), store.SetOptions{}); err == nil {
		t.Fatal("Set succeeded despite a journal failure")
	}
	got, _, _ := s.Get("k")
	if string(got) != "original" {
		t.Fatalf("in-memory state changed to %q after a journal failure", got)
	}

	if _, err := s.Incr("k2", 1); err == nil {
		t.Fatal("Incr succeeded despite a journal failure")
	}
	if _, ok, _ := s.Get("k2"); ok {
		t.Fatal("Incr created a key despite a journal failure")
	}
	if _, err := s.HashSet("h", []store.FieldValue{fv("f", "v")}); err == nil {
		t.Fatal("HashSet succeeded despite a journal failure")
	}

	rec.heal()
	if _, err := s.Set("k", []byte("new"), store.SetOptions{}); err != nil {
		t.Fatalf("Set failed after the journal recovered: %v", err)
	}
	got, _, _ = s.Get("k")
	if string(got) != "new" {
		t.Fatalf("value = %q, want %q", got, "new")
	}
}

func TestAmbiguousRecordFailureAppliesMutation(t *testing.T) {
	rec := &memRecorder{}
	s, _ := newStore(t, rec)

	rec.fail(errors.New("fsync failed"), true)

	// The record may already be in the journal, so memory must match it even
	// though the call reports an error.
	_, err := s.Set("k", []byte("v"), store.SetOptions{})
	if err == nil {
		t.Fatal("expected the durability failure to be reported")
	}
	var ambiguous *store.AmbiguousAppendError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("got %v (%T), want an AmbiguousAppendError", err, err)
	}
	got, ok, _ := s.Get("k")
	if !ok || string(got) != "v" {
		t.Fatalf("mutation was not applied for an ambiguous append: ok=%v value=%q", ok, got)
	}
}

func TestNoRecorderMeansNoJournal(t *testing.T) {
	s, _ := newStore(t, nil)
	if _, err := s.Set("k", []byte("v"), store.SetOptions{}); err != nil {
		t.Fatalf("Set with persistence disabled: %v", err)
	}
	if _, ok, _ := s.Get("k"); !ok {
		t.Fatal("value was not stored")
	}
}

// ---------------------------------------------------------------------------
// Snapshot and replay
// ---------------------------------------------------------------------------

func TestSnapshotRoundTrip(t *testing.T) {
	src, _ := newStore(t, nil)
	mustSet(t, src, "str", "value")
	if _, err := src.HashSet("hash", []store.FieldValue{fv("b", "2"), fv("a", "1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Set("volatile", []byte("v"), store.SetOptions{ExpireAt: startMS + 60_000}); err != nil {
		t.Fatal(err)
	}
	// An already-expired key must not appear in the view.
	if _, err := src.Set("dead", []byte("v"), store.SetOptions{ExpireAt: startMS}); err != nil {
		t.Fatal(err)
	}

	snap := src.Snapshot()
	if len(snap.Items) != 3 {
		t.Fatalf("snapshot has %d items, want 3", len(snap.Items))
	}
	for i := 1; i < len(snap.Items); i++ {
		if snap.Items[i-1].Key >= snap.Items[i].Key {
			t.Fatalf("snapshot items are not sorted: %q then %q", snap.Items[i-1].Key, snap.Items[i].Key)
		}
	}

	dst, _ := newStore(t, nil)
	if err := dst.ApplySnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if dst.Len() != src.Len() {
		t.Fatalf("restored %d keys, want %d", dst.Len(), src.Len())
	}
	got, ok, err := dst.Get("str")
	if err != nil || !ok || string(got) != "value" {
		t.Fatalf("Get = (%q, %v, %v)", got, ok, err)
	}
	pairs, ok, err := dst.HashGetAll("hash")
	if err != nil || !ok || len(pairs) != 2 {
		t.Fatalf("HashGetAll = (%v, %v, %v)", pairs, ok, err)
	}
	if remaining, st := dst.TTL("volatile"); st != store.TTLRemaining || remaining != 60_000 {
		t.Fatalf("restored TTL = (%d, %v), want (60000, remaining)", remaining, st)
	}
}

func TestReplayReproducesState(t *testing.T) {
	rec := &memRecorder{}
	src, _ := newStore(t, rec)

	mustSet(t, src, "str", "value")
	mustSet(t, src, "goner", "bye")
	if _, err := src.SetExpiry("str", startMS+10_000); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Append("str", []byte("+")); err != nil {
		t.Fatal(err)
	}
	if _, err := src.HashSet("h", []store.FieldValue{fv("a", "1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := src.HashSet("h", []store.FieldValue{fv("b", "2")}); err != nil {
		t.Fatal(err)
	}
	if _, err := src.HashDel("h", []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Incr("counter", 7); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Delete("goner"); err != nil {
		t.Fatal(err)
	}

	dst, _ := newStore(t, nil)
	for i, m := range rec.recorded() {
		if err := dst.ValidateReplay(m); err != nil {
			t.Fatalf("mutation %d is not replayable: %v", i, err)
		}
		if err := dst.Replay(m); err != nil {
			t.Fatalf("replay of mutation %d (%v): %v", i, m.Op, err)
		}
	}

	assertStoresEqual(t, src, dst)
}

func assertStoresEqual(t *testing.T, want, got *store.Store) {
	t.Helper()
	wantKeys := want.KeyNames()
	gotKeys := got.KeyNames()
	if len(wantKeys) != len(gotKeys) {
		t.Fatalf("keyspace size differs: want %v, got %v", wantKeys, gotKeys)
	}
	for i := range wantKeys {
		if wantKeys[i] != gotKeys[i] {
			t.Fatalf("key %d: want %q, got %q", i, wantKeys[i], gotKeys[i])
		}
	}
	for _, k := range wantKeys {
		wantKind, _ := want.Kind(k)
		gotKind, ok := got.Kind(k)
		if !ok {
			t.Fatalf("key %q missing from the replayed store", k)
		}
		if wantKind != gotKind {
			t.Fatalf("key %q: kind %v != %v", k, wantKind, gotKind)
		}
		wantDeadline, _ := want.ExpireAt(k)
		gotDeadline, _ := got.ExpireAt(k)
		if wantDeadline != gotDeadline {
			t.Fatalf("key %q: deadline %d != %d", k, wantDeadline, gotDeadline)
		}
		switch wantKind {
		case store.KindString:
			wantValue, _, _ := want.Get(k)
			gotValue, _, _ := got.Get(k)
			if !bytes.Equal(wantValue, gotValue) {
				t.Fatalf("key %q: value %q != %q", k, wantValue, gotValue)
			}
		case store.KindHash:
			wantPairs, _, _ := want.HashGetAll(k)
			gotPairs, _, _ := got.HashGetAll(k)
			if len(wantPairs) != len(gotPairs) {
				t.Fatalf("key %q: %d fields != %d", k, len(wantPairs), len(gotPairs))
			}
			for i := range wantPairs {
				if wantPairs[i].Name != gotPairs[i].Name || !bytes.Equal(wantPairs[i].Value, gotPairs[i].Value) {
					t.Fatalf("key %q field %d: %v != %v", k, i, wantPairs[i], gotPairs[i])
				}
			}
		}
	}
}

func TestReplayRejectsTypeConflicts(t *testing.T) {
	s, _ := newStore(t, nil)
	mustSet(t, s, "k", "v")

	err := s.Replay(store.Mutation{Op: store.OpHashSet, Key: "k", Pairs: []store.FieldValue{fv("f", "v")}})
	if err == nil {
		t.Fatal("replaying HSET over a string did not report an inconsistency")
	}
}

func TestValidateReplayRejectsUnknownOpAndNegativeDeadline(t *testing.T) {
	s, _ := newStore(t, nil)
	if err := s.ValidateReplay(store.Mutation{Op: store.Op(200)}); err == nil {
		t.Fatal("unknown operation accepted")
	}
	if err := s.ValidateReplay(store.Mutation{Op: store.OpSet, ExpireAt: -1}); err == nil {
		t.Fatal("negative deadline accepted")
	}
}

func TestApplySnapshotRejectsUnknownKind(t *testing.T) {
	s, _ := newStore(t, nil)
	err := s.ApplySnapshot(&store.Snapshot{Items: []store.SnapshotItem{{Key: "k", Kind: store.Kind(99)}}})
	if err == nil {
		t.Fatal("snapshot with an unknown kind was accepted")
	}
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

func TestConcurrentIncrDoesNotLoseUpdates(t *testing.T) {
	s, _ := newStore(t, nil)

	const goroutines = 16
	const perGoroutine = 500
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				if _, err := s.Incr("counter", 1); err != nil {
					t.Errorf("Incr: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	value, _, err := s.Get("counter")
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprint(goroutines * perGoroutine)
	if string(value) != want {
		t.Fatalf("counter = %q, want %q: updates were lost", value, want)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	rec := &memRecorder{}
	s, _ := newStore(t, rec)

	const writers = 8
	const readers = 8
	const ops = 300
	var wg sync.WaitGroup

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			for j := 0; j < ops; j++ {
				// A mix of key kinds and value sizes.
				switch j % 4 {
				case 0:
					if _, err := s.Set(key, []byte(fmt.Sprint(j)), store.SetOptions{}); err != nil {
						t.Errorf("Set: %v", err)
						return
					}
				case 1:
					if _, err := s.Append(key, []byte("x")); err != nil {
						// APPEND on a hash is a legitimate type error.
						if !errors.Is(err, store.ErrWrongType) {
							t.Errorf("Append: %v", err)
							return
						}
					}
				case 2:
					if _, err := s.HashSet(key, []store.FieldValue{fv("f", "v")}); err != nil {
						if !errors.Is(err, store.ErrWrongType) {
							t.Errorf("HashSet: %v", err)
							return
						}
					}
				case 3:
					if _, err := s.Delete(key); err != nil {
						t.Errorf("Delete: %v", err)
						return
					}
				}
			}
		}(i)
	}

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				key := fmt.Sprintf("key-%d", j%writers)
				_, _, _ = s.Get(key)
				_, _, _ = s.HashGetAll(key)
				_, _ = s.HashGetMany(key, []string{"f"})
				_ = s.Exists(key)
				_, _ = s.TTL(key)
				_ = s.KeyNames()
				_, _, _ = s.StrLen(key)
				_, _ = s.Incr("shared-counter", 1)
			}
		}()
	}
	wg.Wait()

	if s.Exists("shared-counter") != 1 {
		t.Fatal("the shared counter is missing")
	}
}

func TestConcurrentExpirationAndWrites(t *testing.T) {
	s, clk := newStore(t, nil)

	var wg sync.WaitGroup
	const keys = 64

	// Writers constantly set short deadlines.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 400; j++ {
				key := fmt.Sprintf("k-%d", (id*8+j)%keys)
				if _, err := s.Set(key, []byte("v"), store.SetOptions{ExpireAt: clk.NowMS() + 5}); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
			}
		}(i)
	}

	// A sweeper removes expired keys concurrently.
	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		for i := 0; i < 400; i++ {
			s.ActiveExpireCycle(32)
			clk.Advance(2 * time.Millisecond)
		}
	}()

	// Readers must never observe a stale value for an expired key.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 400; j++ {
				key := fmt.Sprintf("k-%d", j%keys)
				if v, ok, err := s.Get(key); err != nil {
					t.Errorf("Get: %v", err)
					return
				} else if ok && len(v) == 0 {
					t.Error("read an empty value from a live key")
					return
				}
				if remaining, st := s.TTL(key); st == store.TTLRemaining && remaining <= 0 {
					t.Error("TTL reported a non-positive remaining lifetime for a live key")
					return
				}
			}
		}()
	}

	wg.Wait()
	<-sweepDone
	s.PurgeExpired()
}

func TestConcurrentSnapshotsDuringWrites(t *testing.T) {
	s, _ := newStore(t, nil)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				if _, err := s.Set(fmt.Sprintf("w%d-%d", id, j), []byte("v"), store.SetOptions{}); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
			}
		}(i)
	}

	for i := 0; i < 8; i++ {
		snap := s.Snapshot()
		// A snapshot must be internally consistent: no duplicate keys.
		seen := make(map[string]bool, len(snap.Items))
		for _, item := range snap.Items {
			if seen[item.Key] {
				t.Errorf("snapshot contains key %q twice", item.Key)
				break
			}
			seen[item.Key] = true
		}
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
