# Persistence

MiniRedis has two independent persistence mechanisms, both implemented in
`internal/persistence` from scratch:

* an **append-only journal** (`miniredis.mrj`) that records every committed
  mutation, and
* **versioned snapshots** (`miniredis.mrs`) that store a complete keyspace at a
  point in the mutation sequence.

Startup loads the snapshot if one exists and then replays the journal records
that are newer than the snapshot. Both formats carry explicit versions and
checksums, and both are validated before anything is accepted into the running
database.

## 1. Ordering guarantee

The store calls `Recorder.Record(mutation)` **while holding its write lock and
before it applies the change in memory** (see
[CONCURRENCY.md](CONCURRENCY.md)). Therefore:

* journal order is exactly commit order, so replaying the journal reproduces the
  keyspace;
* a mutation that a client can observe has already been written to the journal
  file (its durability then depends on the fsync mode);
* a failed append cannot silently change the in-memory store. If the failure
  proves that no byte reached the file, the command fails and memory is
  unchanged. If the outcome is unknown (the write landed but the `fsync`
  failed), the mutation *is* applied and the client receives an
  `AmbiguousAppendError` — recovery would replay the record anyway, so refusing
  the write would be the inconsistent choice. Either way, the client sees an
  error and `INFO persistence` records the write failure.

## 2. Journal file format

```
offset   size  field
------   ----  ---------------------------------------------------------
0        8     magic "MRJRNL01"
8        2     format version, little-endian (currently 1)
10       2     flags, reserved, must be 0
12       4     reserved, must be 0
16       ...   records
```

Every record is a **frame**:

```
u32 payload length   (little-endian)
u32 CRC32C checksum of the payload (Castagnoli polynomial)
payload bytes
```

The checksum makes a torn or corrupted record detectable: a partially written
record fails either the length check (the file ends early) or the checksum.

The payload begins with a fixed prefix and then an operation-specific body.
All integers are little-endian; every byte string is a `u32` length followed by
the raw bytes, which is what makes keys, hash fields and values binary safe.

```
u64 sequence number
u8  operation
```

Operation bodies:

| Op | Code | Body |
| --- | --- | --- |
| `SET` | 1 | `key`, `i64 expire_at_ms`, `value` |
| `MSET` | 2 | `u32 pair count`, then per pair: `key`, `i64 reserved (0)`, `value` |
| `DEL` | 3 | `u32 key count`, then each `key` |
| `APPEND` | 4 | `key`, `i64 expire_at_ms`, `appended bytes` |
| `HSET` | 5 | `key`, `i64 expire_at_ms`, `u32 pair count`, then per pair: `field`, `value` |
| `HDEL` | 6 | `key`, `u32 field count`, then each `field` |
| `EXPIRE` | 7 | `key`, `i64 expire_at_ms` (0 clears the deadline) |
| `FLUSH` | 8 | *(no body)* |

Design points:

* **Mutations describe the committed state, not the client's wording.** `SET
  key value EX 60` is recorded as the value plus the absolute deadline it
  produced, and `INCR` is recorded as the resulting value. Replaying such a
  journal a week later yields the same keyspace; a naive `SET ... EX 60` log
  would not.
* **Absolute deadlines.** Expiration is stored as Unix milliseconds, so a
  restored key keeps the deadline it had, and a key that expired while the
  server was down is still expired afterwards.
* **Sequence numbers** are per record, assigned while the store's write lock is
  held, so they are dense and ordered. A rejected append does not consume a
  number.
* **Size limit.** A record larger than `--max-journal-record-bytes` (default
  64 MiB) is rejected before any bytes are written, which also bounds how much
  a corrupt length field can make the reader allocate.
* **Operation codes are stable**: they are written to disk and must never be
  renumbered. An unknown code is a corruption error, not something to skip.

## 3. Snapshot file format

```
offset  size  field
------  ----  -----------------------------------------------------------
0       8     magic "MRSNAP01"
8       2     format version, little-endian (currently 1)
10      2     flags, reserved, must be 0
12      8     sequence number of the last mutation included
20      8     creation time, Unix milliseconds
28      8     key count
36      ...   entries
...     4     CRC32C checksum of every preceding byte
```

Each entry:

```
u32 key length, key bytes
u8  kind             (1 = string, 0x02 = hash)
i64 expire_at_ms     (0 = no deadline)
then, for a string:  u32 value length, value bytes
then, for a hash:    u32 pair count, then per pair: u32 field length, field,
                                                  u32 value length, value
```

Entries are written in lexicographic key order and hash pairs are sorted by
field name, so two snapshots of the same keyspace are byte-identical apart from
the creation timestamp. That makes comparison and testing straightforward.

Validation is two-pass: the checksum over the whole file first, then the content
(header fields, per-field lengths bounded by `--max-journal-record-bytes`, kind
in range, no negative deadlines, no trailing bytes, no missing entries). A
snapshot that fails either pass is reported as corrupt and startup **stops**. A
corrupt snapshot is never treated as an empty database, because that would
silently discard all data that the snapshot still contains.

## 4. Atomic snapshot creation

`Manager.SaveSnapshot` performs the standard crash-safe replacement:

1. capture a consistent view (`Store.Snapshot`), holding the write lock only for
   the copy and never for serialisation;
2. create `miniredis.mrs.tmp` with mode `0600`, removing a stale temporary file
   from an interrupted run first;
3. stream the view into it, computing the checksum as it goes, so memory use
   does not grow with the keyspace;
4. `fsync` the temporary file and close it;
5. `os.Rename` it over `miniredis.mrs` — atomic on POSIX file systems;
6. `fsync` the containing directory so the rename itself survives a power loss;
7. remove the temporary file if any step failed.

A crash at any point therefore leaves either the previous snapshot or the new
one, never a mix. Only the journal is ever written in place, and it is
append-only by construction.

Snapshots can be taken while clients keep writing: the view records
`Snapshot.Seq`, records appended after that sequence stay in the journal, and
recovery replays exactly them. `tests.TestSnapshotPlusJournalReplayComposes`
verifies that composition, including that a phase which should not write a
snapshot does not touch the existing file.

## 5. Durability modes

`--appendfsync` (or `MINIREDIS_APPENDFSYNC`) selects the barrier used for each
acknowledged write.

| Mode | What happens before the reply | What can be lost |
| --- | --- | --- |
| `always` | The record is written and `fsync`ed (an `fdatasync`-style barrier on the journal file). | Nothing that has been acknowledged, assuming the storage device honours `fsync`. |
| `everysec` | The record is written to the file; a background flusher `fsync`s at most once per second. | Up to about one second of acknowledged writes after a power loss. A process crash alone loses nothing already written. |
| `no` | The record is written to the file; flushing is left to the operating system. | An unbounded window, from MiniRedis's point of view. |

MiniRedis never claims more than `fsync` returning successfully: a buffered
write is not asserted to be on physical media. The cost of `always` is real and
measured — see [BENCHMARKS.md](BENCHMARKS.md).

## 6. Damaged tail and corruption policy

| Situation | Policy |
| --- | --- |
| The file ends in the middle of a record (torn tail after a crash) | The normal case, not corruption. With `--journal-repair=truncate` (default) the partial tail is discarded, the file is truncated back to the last valid record boundary and `fsynced`, and the number of discarded bytes is reported in the recovery report and in `INFO persistence` (`recovery_tail_repaired`, `recovery_discarded_bytes`). With `--journal-repair=fail` the server refuses to start and leaves the file untouched. |
| A record fails its checksum, or its length, in the middle of the file (more valid records follow) | An error in **both** modes. Recovery never skips arbitrary corruption and then claims that everything was recovered; the operator must decide what to do. |
| The journal header is missing or is not `MRJRNL01` | Refused: `unrecognised journal header`. The file is left untouched. |
| The journal version is newer than this build supports | Refused, with the version reported. |
| The snapshot fails validation | Startup stops with the snapshot error. The previous state cannot be guessed safely. |
| A record describes an operation that cannot apply to the recovered state (for example `HDEL` on a key the snapshot does not contain) | Counted as an orphan record, reported in the recovery report and as an `INFO` counter, and logged as a warning. Records that would corrupt the state (a type mismatch, an out-of-range operation) fail recovery instead. |
| A disk write fails during normal operation | The command returns an error; the failure is counted; a failure whose outcome is unknown poisons the journal (see below). |

**Poisoning.** If an append fails in a way that leaves the file's state
unknown — the write or the `fsync` returned an error after some bytes may have
landed — the journal is marked unusable (`journal_poisoned`). From then on every
write command fails with `journal is unusable after an earlier write failure`
instead of appending after a possibly incomplete record. The in-memory state
stays available for reads, the HTTP readiness endpoint reports `503`, and an
operator restarts the server (recovery repairs the torn tail) or inspects the
file. Skipping the failure and continuing to write would produce a journal whose
sequence and contents no longer describe the keyspace; MiniRedis refuses to do
that.

## 7. Startup recovery

1. Create/validate the data directory (`0700`; symlinks and non-directories are
   refused).
2. Open the journal, validating its header and positioning the writer at the end
   of the last valid record.
3. Load `miniredis.mrs` if present: validate, decode, and apply it to the store
   as a whole (`Store.ApplySnapshot`). The store's sequence is set to the
   snapshot's.
4. Scan the journal and apply every record whose sequence number is greater than
   the snapshot's. Records at or below the snapshot's sequence are counted as
   *skipped*, not re-applied.
5. Count orphan records and warnings; repair a torn tail according to policy.
6. Produce a `RecoveryReport` with a status of `empty`, `ok` or `repaired`, the
   counts above, the discarded byte count and the elapsed time. `INFO
   persistence` exposes it, and the HTTP readiness endpoint mentions a repair
   while still reporting the server as ready, because the surviving data is
   consistent.

Recovery runs before the listener is opened, so a problem is reported to the
operator at startup rather than to the first client.

## 8. Journal growth and compaction

The journal grows without bound until it is compacted. **Compaction is not
implemented**, and the project does not pretend otherwise.

What exists instead:

* `--snapshot-interval` writes a snapshot on a timer, and
  `--snapshot-on-shutdown` writes one on a clean shutdown. A snapshot shortens
  the *replay* time on startup but does not shrink the journal.
* Because the journal is only ever appended to, there is no in-place rewrite to
  crash in. Implementing compaction safely requires a documented protocol
  (write a compacted journal from a snapshot, `fsync`, atomically rename, then
  record the new journal's sequence base) and restart tests for each
  interrupted step. Until that protocol exists and is tested, the honest
  position is: snapshots reduce recovery time, and long-running write-heavy
  deployments will accumulate journal bytes.

Operationally, a deployment with a journal that has grown too large can be
compacted by stopping the server, deleting the journal after a successful
snapshot has been written, and starting again — with the caveat that this
loses any write that happened after that snapshot. `DBSIZE`, `INFO
persistence` (`journal_size_bytes`) and `journal_sequence` make the situation
visible.

## 9. Tests

Persistence is tested with real files in real temporary directories, never with
mocks:

| Behaviour | Test |
| --- | --- |
| Journal creation, header, version refusal | `internal/persistence.TestJournalHeaderIsWrittenOnCreate`, `TestOpenJournalRejectsUnsupportedVersion` |
| Append, scan, sequence numbers | `internal/persistence.TestAppendAndScan` |
| Oversized records rejected without consuming a sequence number | `internal/persistence.TestAppendRejectsOversizedRecord` |
| Poisoned journal refuses further appends | `internal/persistence.TestPoisonedJournalRefusesAppends` |
| Fsync modes (`always` syncs per write, `no` does not) | `internal/persistence.TestFsyncAlwaysModeSyncsEachWrite`, `TestFsyncNoModeDoesNotSyncPerWrite` |
| Symlinked data files refused | `internal/persistence.TestOpenRefusesSymlinkedDataFiles` |
| Corrupt length claiming a huge record | `internal/persistence.TestCorruptRecordLengthIsRejected` |
| Truncation back to a valid boundary | `internal/persistence.TestTruncateJournal` |
| Snapshot round trip, corruption, atomic replace | `internal/persistence` snapshot tests |
| Restart recovery across a closed and reopened engine | `internal/persistence.TestRecoveryAfterRestart` |
| Journal-only recovery over the wire | `tests.TestJournalReplayRestoresStateAfterRestart` |
| Snapshot plus journal composition | `tests.TestSnapshotPlusJournalReplayComposes` |
| Torn tail repaired, bytes discarded and reported | `tests.TestTruncatedJournalTailIsRepaired` |
| Corrupt snapshot stops startup | `tests.TestCorruptSnapshotStopsStartup` |
