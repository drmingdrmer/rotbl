# DB Manifest Design

The `DB` gains an LSM-style **manifest**: the authoritative, persisted record of
which tables exist and how they are organized. For an MVP, persist the whole
manifest through `Storage` and cap its encoded size. If that cap becomes too
small, introduce an edit-log manifest separately; do not expose that machinery
in the storage-snapshot manifest API.

## Model overview

- A `DB` owns a set of immutable `rotbl` tables, organized into **levels**.
- Each new write batch is flushed into a new table at the **top** level; reads
  merge from the top (newest) down to level 0 (oldest), newest wins.
- Compaction merges a contiguous run of levels into fewer tables, reclaiming
  space and bounding read amplification.
- The manifest is the single source of truth for: the table set, table-id
  allocation, level allocation, and the last used key sequence number.

The current design is not an RSM/WAL design. The DB mutates a private `Manifest`
snapshot, writes the complete encoded manifest through atomic storage, and only
then publishes it to readers.

## Data model

Widths: `table_id` and `level` are `u32` for now. Key sequence values are `u64`
because they churn per key/write. The persistent manifest `table_id` is a
logical table identity and may start at `0`. It may be passed to `Rotbl` as the
block-cache namespace, but it must not be confused with the rotbl file's on-disk
reserved table-id field.

```rust
/// The persisted manifest snapshot.
///
/// `UserData` is an application-supplied payload persisted inside the
/// manifest (e.g. databend-meta metadata such as a last-applied log id). It is
/// part of the encoded schema: it is written between `last_key_seq` and
/// `levels`, so changing its encoding is a format change.
struct Manifest<UserData: Encode + Decode = ()> {
    /// All levels, keyed by level number (`Levels` wraps the map and owns
    /// level-set validation).
    levels: Levels, // BTreeMap<u32 /*level*/, LevelManifest>

    /// Next table id to allocate. Starts at 0; logical manifest table id 0 is
    /// valid even though the rotbl file's on-disk table-id field stays reserved.
    next_table_id: u32,

    /// Next level to allocate. Starts at 0. Each fresh flush uses this value
    /// and then increments it. Compaction never advances it.
    next_level: u32,

    /// Last allocated key sequence number. 0 is reserved as the not-found
    /// sentinel, so the first allocated key sequence is 1.
    last_key_seq: u64,

    /// Application payload persisted with the manifest.
    user_data: UserData,

    /// Last committed manifest sequence. This is only for manifest slot
    /// rotation and must not be reused as a key sequence number.
    manifest_seq: u64,
}

struct LevelManifest {
    /// Keyed by `Arc::clone(&record.smallest)`: the key and the record's
    /// `smallest` field share one allocation, so the smallest key is stored once.
    tables: BTreeMap<Arc<str> /*smallest*/, Arc<TableRecord>>,
}

/// A complete, self-describing table entry: its id and inclusive key extent.
/// `smallest` is an `Arc<str>` so it can also serve as the level map key without
/// a second copy. `level` is the outer map key.
struct TableRecord {
    table_id: u32,
    /// Inclusive key extent `[smallest, largest]` — the table's true first/last
    /// key, mirroring `BlockIndexEntry`. Within one level, table ranges are
    /// disjoint with strict ordering: `A.largest < B.smallest`.
    smallest: Arc<str>,
    largest: String,
}

/// Derived runtime entry used by callers: a level plus the shared record.
/// Constructing one from a stored record is a pointer clone.
struct TableInfo {
    level: u32,
    record: Arc<TableRecord>,
}
```

Mutation semantics:

- `allocate_table_id()` and `allocate_level()` return the current next value and
  immediately increment the stored cursor. They panic on counter overflow.
- `allocate_key_seq()` increments `last_key_seq` first, then returns it. It
  panics on counter overflow, like the other allocators. Since sequence `0`
  means not found, the first allocated key sequence is `1`.
- `add_table(table)` inserts one already-built `TableInfo`. It rejects
  unallocated ids/levels, duplicate table ids and range overlap with cheap
  local checks (neighbor lookups) *before* mutating, so a failed insert needs
  no rollback and a k-table compaction does not pay a full O(n) validation per
  table. A rejected insert never leaves an empty level behind.
- `remove_table(table_id)` removes one existing table and returns its
  `TableInfo`, dropping the level if it becomes empty. Removal cannot break
  level invariants.
- `advance_manifest_seq()` increments the manifest snapshot sequence exactly
  once (the commit boundary); it panics on overflow, like the other counters.
  The full O(n) `validate()` runs once per write in `encode` (and once per read
  in `decode`), so it is not repeated here.
- Callers mutate a private `Manifest` clone, persist the complete encoded file,
  and publish the clone only after storage commit succeeds.

## Levels and recency

`level` is a **generation/recency rank**, not a RocksDB fixed tier:

- **Flush** creates a table at `level = next_level`, then updates
  `next_level = level + 1`. New data always lands on top.
- **Compaction** merges a *contiguous* run of levels `[l .. l+k]` and writes the
  output at `level = l` (the **lowest** input level). `next_level` is untouched,
  so the freed numbers `l+1 .. l+k` leave gaps — levels are non-consecutive.

**Invariant (read correctness): higher level ⇒ strictly newer data.** This holds
because flushes only ever add to the top and compaction outputs at the min of a
*contiguous* input run, so the merged data never jumps above data that is newer
than it. Reads therefore resolve conflicts purely by level order (first hit
scanning top→down wins), the same mechanism RocksDB relies on.

> Compaction inputs **must** be a contiguous range of levels. Compacting
> non-adjacent levels (e.g. 0 and 2, skipping 1) would let the output at the low
> level shadow newer data at the skipped level. Key sequence values remain the
> ground-truth tiebreaker, but the design relies on level stratification for the
> normal read path.

`last_key_seq` is **not** used for cross-level conflict resolution. It only
tracks key sequence allocation for snapshot visibility and tombstone GC.

## Read path

Point read of key `k` at snapshot `S` (default `S` is the latest committed key
sequence):

1. For `level` from highest to lowest: locate the (≤1, since ranges are
   disjoint) table whose `[smallest, largest]` covers `k` via binary search.
2. Load its block (through the shared block cache, keyed by that `table_id`) and
   look up `k`, taking the newest version with key sequence `<= S` if one
   exists.
3. The first level that yields a visible version wins. A tombstone resolves to
   "absent". Stop at the first hit (stratification guarantees it is newest).

Range scans merge-iterate across all overlapping tables, deduplicating by key
(highest level with a visible version wins) and suppressing tombstones.

## Write path

fsync ordering is **table first, then manifest** — the table file must be durable
before the manifest state that references it.

**Flush:**

1. Acquire the mutator lock (held for the whole flush — see *Locking* below),
   clone the current manifest and allocate `table_id = allocate_table_id()`,
   `level = allocate_level()`, and the needed non-zero key sequence values
   with `allocate_key_seq()`.
2. Build the rotbl file (path derived from `table_id`) and **fsync it**.
3. Create `TableInfo::new(level, Arc<TableRecord>)` and call
   `add_table(table)` on the private manifest.
4. Call `advance_manifest_seq()`, write the complete encoded manifest snapshot
   to `manifest_dir/<manifest_seq % 4>`, and commit storage.
5. Publish the private manifest to readers only after commit succeeds.

**Compaction:**

1. Choose a contiguous level run `[l .. l+k]`; merge their range-partitioned
   tables, keeping the newest version per key. A tombstone may be **dropped**
   only when `l` is the lowest live level; otherwise it must be kept, because
   it still shadows older versions below the run.
2. Build the output table(s) with fresh `table_id`s at `level = l`; **fsync**.
3. On a private manifest clone, call `remove_table()` for the input tables and
   `add_table()` for the output tables, then call `advance_manifest_seq()`.
4. Write and commit the complete manifest snapshot, then publish.

The merge in step 1 runs outside the commit lock, so the input set must be
re-validated at commit time, not just at plan time. This is cheap: flushes can
only create levels *above* the run, so they never break contiguity, and if a
concurrent compaction consumed an input table, `remove_table()` in step 3
fails on the missing table and the whole compaction aborts (its orphan output
files are removed by startup GC). That failure path is the entire concurrency
story; no extra coordination state is needed.

Crash safety:

- Crash between table fsync and manifest commit → the table is an unreferenced
  orphan, removed by startup GC.
- Crash before successful manifest commit → the table is an orphan unless the
  manifest state had already reached durable storage.
- Crash after successful manifest commit but before reader-visible publish → the
  manifest state is durable and recovered at the next startup.
- A manifest write can reach durable storage and then fail to acknowledge
  (e.g. an I/O error on the final directory sync). After a crash, recovery
  treats that manifest as committed — the referenced tables are already
  durable, so this is safe. Callers must therefore treat a failed flush or
  compaction commit as **unknown outcome**, never as "not applied".
- If the process survives a failed commit, the in-memory manifest still holds
  the old `manifest_seq`, so the next commit re-advances to the same sequence
  and — because the slot is `manifest_seq % 4` — deterministically overwrites
  the orphaned write with the new content. The failure self-heals.

## Persistence and recovery (Storage snapshot)

The simplest manifest implementation writes full snapshots into four fixed
files under a dedicated manifest directory in the DB storage:

1. Serialize a complete manifest file frame `{ header, manifest_seq,
   next_table_id, next_level, last_key_seq, user_data, levels, checksum }`.
   `header` is `Header::new(Type::ManifestFile, Version::V001)` and carries its
   own checksum. The trailing `checksum` covers the whole frame; the inner
   `levels`/`level` frames live inside it and are **not** separately checksummed
   (a second checksum would only re-cover already-covered bytes). `levels` is a
   nested frame `{ header(Type::Levels, V001), uncompressed_len, payload }` where
   `payload` is the zstd-compressed level/table map — the bulky, repetitive
   part; the small fixed-width cursors stay uncompressed. The header version
   determines the compression algorithm, so no separate compression tag is stored.
2. Use exactly four fixed manifest keys: `manifest_dir/0`, `manifest_dir/1`,
   `manifest_dir/2`, and `manifest_dir/3`.
3. On update, clone the in-memory manifest, mutate the clone directly, increment
   `manifest_seq` by exactly 1 with `advance_manifest_seq()`, validate
   invariants, warn if the encoded payload exceeds `MANIFEST_MAX_BYTES`, write
   `manifest_dir/<manifest_seq % 4>`, and commit the writer.
4. Publish the new manifest to readers only after commit succeeds.
5. On open, read the four fixed keys directly and use the valid manifest with
   the highest `manifest_seq`. A manifest file is valid only if it decodes with
   all checksums intact and its slot matches `manifest_seq % 4`. **Absent and
   invalid are different conditions**:
   - All four slot keys absent → fresh DB, start empty.
   - Slot bytes present but none valid → storage corruption; **fail startup**.
     Starting empty here would let startup GC delete every table file.
   - Otherwise recover the highest valid seq. Log every missing/invalid slot
     and the recovered sequence — after a crash, one torn slot (always the
     oldest seq, since writes are serialized) is expected; silently falling
     back further than that must be visible to operators.

This avoids an edit-log path dependency and the codeq-version split. It is
O(manifest size) per edit and has no edit history, but that is acceptable while
the manifest is capped small. It also avoids requiring `Storage::list()` or
`Storage::delete()` for manifest recovery.

Do **not** derive `manifest_seq` from key sequence values; they are different
counters with different meanings. `manifest_seq` exists only to rotate manifest
slots and to determine which snapshot is newest.

Manifest file sequences should be incremental and consecutive for committed
snapshots. File names are not monotonic; they are fixed slots. The sequence is
encoded once, as the manifest's own `manifest_seq` field; there is no separate
frame copy to cross-check.

Why four slots: writes are serialized, so a torn write can only corrupt the
slot holding the oldest sequence. Two slots already guarantee one intact
committed manifest; each extra slot keeps one more, so four slots hold
**three** intact committed manifests at all times — a round power-of-two count
and cheap insurance against bit rot in a cold slot.

## Manifest compression and size limit

The manifest has no natural finite maximum unless key length is bounded:
`smallest` and `largest` are variable-length strings. `MANIFEST_MAX_BYTES` is an
advisory warning threshold for the complete encoded manifest file, not a hard
failure limit.

Always compress the V001 `levels` payload with `zstd` level 1, and store
`uncompressed_len` in the `Levels` frame. The header version selects the
compression algorithm, so no separate compression tag is stored. On decode,
read the compressed payload without trusting its length prefix (read at most the
bytes actually available), then bound decompression by the declared
`uncompressed_len` (read at most `uncompressed_len + 1` bytes and require an
exact length match), so a bad frame can never trigger an unbounded allocation;
the outer `Manifest` checksum then covers the whole frame. On read and write,
log a warning when compressed or uncompressed payload sizes exceed
`MANIFEST_MAX_BYTES`. Compression reduces I/O for
repetitive paths and key prefixes, but oversized manifests are still valid.

Compact binary estimate per table:

```text
entry_size ~= 32 + smallest_key_len + largest_key_len bytes
```

Without storing relative paths:

| Avg key size | Approx entry | 1 MiB | 4 MiB | 8 MiB |
| --- | ---: | ---: | ---: | ---: |
| 64 B | 160 B | ~6.5k tables | ~26k tables | ~52k tables |
| 128 B | 288 B | ~3.6k tables | ~14k tables | ~29k tables |
| 256 B | 544 B | ~1.9k tables | ~7.7k tables | ~15k tables |

Advisory uncompressed limit: target ≤ 1 MiB, warn above 4 MiB, and warn more
strongly above 8 MiB. An 8 MiB manifest already implies thousands to tens of
thousands of tables, which is beyond a healthy read/compaction shape for this
design. If a real workload needs more, switch to an edit-log backend or compact
range-bound encoding before raising the advisory threshold.

## Deferred edit-log option

If the full-snapshot cap becomes too small, introduce a separate edit-log
manifest backend then. That backend can define its own action type and RSM-style
replay, but the storage-snapshot manifest should stay a direct mutable snapshot.

## File GC — startup only

No runtime ref-counting. At startup, **only after the manifest is recovered
successfully** (GC must never run when recovery failed or refused to start —
see the recovery rules above):

- List the rotbl files present in storage.
- Delete every file that **matches the table naming scheme** and whose
  `table_id` is **not** present in any `manifest.levels[*].tables`. Files that
  do not parse as table paths are never touched.
- Sweep leftover `*.tmp-*` files from writes that crashed before commit.
  (`FsWriter` also removes its temp file on drop, so runtime aborts do not
  leak; the startup sweep covers process crashes.)

Looking up a `table_id` in the manifest is a linear scan over all levels;
that is acceptable at the advisory manifest scale and GC runs once per
startup.

This is safe because no readers are active yet. Tables removed by runtime
compaction stay on disk until the next startup sweep — an acceptable, bounded
amount of dead space in exchange for a trivially crash-safe, lock-free scheme.

This requires extending the storage surface or constraining manifest-backed DBs
to filesystem storage. The current `Storage` trait has only `reader_at()` and
`writer()`, so it cannot list or delete table files.

## Runtime Snapshot Protection

Snapshot protection is runtime state, not manifest state — and with
startup-only GC it is trivial: a snapshot holds its `Arc<Manifest>` (and open
table handles), and the files it references stay on disk by construction,
because nothing deletes files while the process runs. Compaction removing a
table from the *manifest* does not affect readers holding an older `Arc`.
After restart, those readers are gone, so no snapshot-pinning state needs to
be recovered from the manifest.

Only the deferred **runtime file GC** option would need real pinning (deciding
which dead tables cannot be deleted yet); that machinery belongs to that
option, not to this design.

## DB integration

```rust
struct DB {
    config: Config,
    block_cache: BlockCache,              // shared, unchanged
    manifest: RwLock<Arc<Manifest>>,      // publish swap only; O(1) reader snapshots
    manifest_store: Mutex<ManifestStore>, // serializes mutators end-to-end
}
```

**Locking.** Two locks with distinct roles, because allocations happen on a
private clone that is invisible until publish:

- `manifest_store: Mutex<_>` is the **mutator lock**: flush and compaction
  commit hold it across their *entire* critical path (clone + allocate → build
  table + fsync → write + commit manifest → publish). This is what makes
  cursor allocation safe: with mutators serialized, the published manifest a
  mutator clones always contains every previously allocated id. Releasing it
  between allocation and publish would let a concurrent mutator clone the
  published manifest and allocate the same `table_id`/`level`/key seqs.
- `manifest: RwLock<Arc<Manifest>>` is taken for writing only for the final
  publish swap (an `Arc` store, nanoseconds). Readers clone the `Arc` under
  the read lock in O(1) and then run the whole merge scan against an immutable
  snapshot with no lock held. Reads are never blocked by table builds or
  fsyncs.

- The standalone `next_table_id: AtomicU32` added earlier is **removed** —
  superseded by the manifest's persisted `next_table_id`. The block-cache
  namespace can use the allocated persistent `table_id`, stable across restarts.
  The rotbl file's on-disk
  `table_id` field stays reserved/0 (a file must not hardcode a DB-assigned id).
- `table_id`/`level`/key sequence allocation happens under the mutator lock on
  the private manifest clone that will be persisted.

## Encoding

The storage-snapshot manifest uses `codeq::Encode`/`Decode` for a stable compact
encoding of `Manifest` and primitive fields (`u32`/`u64`/`String`/`BTreeMap`),
plus the existing pinned `zstd` dependency. If the design later switches to an
edit-log backend, that backend can choose its own action encoding in a separate
change.

`Manifest`, `Levels` and `LevelManifest` implement encoding manually. Only the
outer `Manifest` frame is checksummed; `Levels` (`Type::Levels`, zstd +
`uncompressed_len`) and `LevelManifest` (`Type::LevelManifest`) are type-tagged
frames nested inside it, so a second checksum on either would only re-cover
bytes the outer checksum already protects. `LevelManifest` does not persist its
map keys; each table is encoded as `(smallest, largest, table_id)` read straight
from the self-contained `TableRecord` (the map key is `Arc::clone`d from that
same `smallest`). `TableRecord` and `TableInfo` stay runtime structs.

Implementation layout: `v001::manifest` is a module directory, one file per
type: `state.rs` (`Manifest`), `levels.rs`, `level_manifest.rs`,
`table_record.rs`, and `table_info.rs`, plus `v001.rs` for the versioned frame
codec. File framing is implemented as methods on `Manifest`; there is no
separate `ManifestFile` state struct.

## Issues to address before implementation

- **Manifest path and API**: `DB::open(config)` has no manifest storage. Add
  `DB::open_with_manifest(config, storage, manifest_dir)` for the
  storage-snapshot path and keep `DB::open(config)` for the current cache-only
  use case.
- **Manifest storage ownership**: a manifest-backed DB cannot accept arbitrary
  storage per table operation. The DB must own one cloned `Storage` handle and
  derive table paths from manifest state.
- **Storage mismatch**: the four-slot manifest can use only `reader_at()` and
  `writer()`, but table-file GC still needs list/delete. Either extend `Storage`
  for GC or keep GC filesystem-only.
- **Key-range capture**: the manifest edit needs inclusive `[smallest, largest]`.
  Capture first and last keys while building the table, or expose a table-level
  range accessor from `Rotbl`. Do not scan after commit.
- **Durability boundary**: the full manifest snapshot commit must report success
  before the table is visible through the manifest.
- **Storage commit contract**: `Storage::Writer::commit()` must guarantee atomic
  durable replacement for the four manifest slots, or the manifest store must
  use a backend-specific protocol that provides it.
- **Directory-entry durability**: table creation/rename and manifest slot
  replacement need parent-directory durability, not just file `fsync` —
  otherwise a power loss can revert a slot to its previous content, and the
  acknowledged commit rolls back (then startup GC deletes the tables it
  referenced). This is a hard precondition for every crash-safety claim above.
  *Done for unix in `FsWriter::commit()` (parent-dir `sync_all` after rename);
  windows and other `Storage` backends must provide the equivalent.*
- **Crash cleanup**: table-first ordering intentionally creates possible orphan
  files. Startup GC must be implemented before manifest-backed DBs are used in
  production.
- **Manifest mutation API**: keep the storage-snapshot API direct. Do not expose
  an edit-log action/apply API; add that only if an edit-log backend is actually
  implemented.
- **Manifest invariant validation**: direct mutation methods must reject
  duplicate table ids, missing removals, overlapping ranges in a level, and
  invalid unallocated table ids/levels. Do not rely on caller discipline.
  *Done: `add_table`/`remove_table` run cheap local checks per mutation;
  `encode`/`decode` run the full `validate()` at the commit boundary (write and
  read respectively).*
- **Manifest sequence**: `manifest_seq` is a separate counter stored in
  `Manifest`; every committed manifest snapshot must increment it by exactly 1.
  Do not derive it from key sequence values. Validate that the slot agrees
  with the decoded `manifest_seq` (the sequence is encoded once; there is no
  second copy to cross-check).
- **Empty flush handling**: an empty write batch cannot produce `[smallest,
  largest]`. Skip empty flushes or reject them before allocating ids.
- **Manifest size threshold**: treat `MANIFEST_MAX_BYTES` as advisory. Warn when
  the compressed or uncompressed payload exceeds it; do not reject only because
  of this threshold. Do not enforce a total frame-size limit.
- **Compression safety**: validate `uncompressed_len` before allocation or
  decompression. *Done: `Levels::decode` reads the compressed payload without
  trusting its length prefix, bounds decompression by the declared length, and
  requires an exact match.*
- **Recovery visibility**: recovery must log every missing/invalid slot and
  the recovered sequence, and must fail startup when slot bytes exist but none
  is valid. *Done in `Manifest::select_latest_valid`.*
- **Level naming**: this design intentionally treats level 0 as the bottom and
  higher level numbers as newer data. This is the reverse of common RocksDB
  naming and should be called out in API docs to avoid wrong compaction code.
- **Compaction input validation**: the compaction planner must ensure removed
  input levels are contiguous **at commit time**, not just at plan time; the
  direct add/remove API cannot infer compaction intent from individual table
  mutations. Flushes only create levels above the run, so contiguity cannot be
  broken from outside; a concurrent compaction consuming an input surfaces as
  a failed `remove_table()`, which must abort the whole commit.

## Deferred / out of scope

- Compaction *policy* (which level run to compact, when) — the manifest only
  records the result.
- The cross-level merge iterator (seq-dedup + tombstone suppression) — the real
  algorithmic work, separate from the manifest.
- Bloom filters / read-amplification mitigation for point reads.
- A runtime snapshot-pinning API for delayed table deletion.
- Runtime (vs startup-only) file GC.
