# Phase 1 — Write-ahead log (WAL)

> **Concept:** durability, and nothing else.
> **Done when:** write 100 keys, `kill -9` mid-run, restart, and every
> *acknowledged* write is still there. An unacknowledged write (killed before
> `fsync` returned) is allowed to be missing.

The WAL is the floor of the whole system. Every layer above it — memtable,
SSTables, and eventually the Raft-applied state — trusts that "if the call
returned, the data survives a crash." That guarantee is created *here* and
nowhere else. So this phase is small in code and large in getting-it-right.

---

## 1. What we're actually building

An append-only file plus two operations:

- `append(record)` — serialize a `PUT`/`DELETE`, write it to the file,
  make it durable, then return. State (a `HashMap` for now) is updated after.
- `replay()` — on startup, read the file start-to-finish and rebuild state.

That's it. No memtable, no sorting, no disk-format cleverness. The only hard
questions are: **how is each record framed on disk, and how do we know on
replay which records are actually complete?**

---

## 2. Algorithm / design options

There are four real decisions. None is exotic, but each has a correctness
consequence, so we make them deliberately.

### 2a. Record framing — how one record is delimited on disk

| Option | How | Pro | Con |
|---|---|---|---|
| **Length-prefix** | `[len][payload]` — read 4 bytes, then read `len` bytes | Trivial to parse, no escaping, self-describing | Must trust `len`; a corrupt length can send you reading garbage — the checksum guards this |
| Delimiter / newline | separate records with `\n` | Human-readable | Breaks the moment a value contains the delimiter byte; needs escaping. Bad for binary values |
| Fixed-size records | pad every record to N bytes | O(1) seek to record k | Wastes space, caps value size. Wrong for a KV store with variable values |

**This is the main framing decision, and length-prefix is the standard answer**
(RocksDB, etcd, Postgres WAL all use length-prefixed framing). We'll take it.

### 2b. Corruption / completeness detection — how replay knows a record is whole

The crash we must survive: process dies *mid-write*, leaving a half-written
record at the tail. Replay has to detect that and stop cleanly, not parse junk.

| Option | Detects | Notes |
|---|---|---|
| **CRC32C per record** | bit flips *and* torn/truncated tail writes | Hardware-accelerated on modern CPUs (SSE4.2). Same choice as RocksDB/LevelDB |
| xxHash / xxh3 per record | same, slightly faster in software | Not hardware-accelerated; overkill here, and CRC32C is the more "expected" choice to explain |
| No checksum, length only | truncation only, *not* bit rot | A torn write that happens to leave a plausible length silently corrupts. Reject this |

**CRC32C per record.** On replay, if a record's stored CRC doesn't match the
CRC we compute over its bytes, we treat that record and everything after it as
"never completed" and stop. This is what makes "unacknowledged writes may be
missing" *safe* rather than *corrupting*.

### 2c. Durability granularity — what "durable" costs per write

> **First, the mental model this whole storage engine runs on: where does a
> `write()` actually put your bytes?** Four places, least→most durable:
>
> ```
> 1. app buffer   (your Vec<u8>, your process RAM)
>      │ write() syscall
> 2. OS page cache (kernel RAM — write() returns HERE)
>      │ fsync()
> 3. SSD disk cache (volatile RAM on the drive)
>      │ cache-flush
> 4. NAND flash    (persistent — survives power loss)
> ```
>
> The trap: **`write()` returning does NOT mean the data is on the SSD.** It's
> only at level 2 — kernel RAM. That *does* survive a `kill -9` (the page cache
> belongs to the kernel, not your dead process), but it does **not** survive a
> power cut or kernel panic. `fsync()` is what pushes levels 2→4 and makes the
> data truly persistent. A database has to survive power loss, so it fsyncs.
> (Aside: cheap SSDs can lie about flushing; that's a hardware-honesty problem,
> not ours to solve — one sentence and move on.)

| Option | Guarantee | Throughput |
|---|---|---|
| **`fsync` per append** | every acked write survives a crash — literally the phase's done-when | Slowest; one fsync per write. Fine for phase 1; a WAL append is sequential so it's the *cheap* kind of fsync |
| Group commit (batch N writes, one fsync) | same guarantee, amortized | Faster, but adds a batching/queue mechanism we don't need yet — note it for later |
| `fdatasync` instead of `fsync` | skips syncing file *metadata* (mtime etc.), only data | A valid, slightly faster variant once the file already exists at a fixed size |

**`fsync` per append for phase 1. — LOCKED.** Two reasons, in order of weight:
(1) it gives the strongest, simplest-to-explain promise — "if the call
returned, the write survives *anything*, including power loss"; (2) group
commit only pays off when there are *many concurrent writers* to batch into one
flush, and Phase 1 is a single-threaded loop — there's nothing to batch, so
group commit would add a queue + timer + concurrency to debug for near-zero
speedup. Correct first; optimize when there's real concurrent load to measure
(naturally arrives at Phase 8/10, backed by a benchmark of the fsync cost).

> ⚠️ Correctness subtlety worth writing down: on some filesystems you must also
> `fsync` the *directory* the first time the file is created, or the file's
> existence itself isn't durable. We create the WAL once, fsync the dir once at
> creation, and never worry about it again.

### 2d. Record body encoding — how a PUT/DELETE is serialized

| Option | Notes |
|---|---|
| **Hand-rolled binary** (`op:u8, klen:u32, key, vlen:u32, val`) | Zero deps, total control, easy to reason about byte-for-byte. Best for learning what's on disk |
| `bincode` / `postcard` (Rust serde) | Less code, but hides the layout — and the whole point of this phase is to *see* the layout |
| Protobuf / flatbuffers | Overkill; schema tooling for a 3-field record |

**Hand-rolled binary.** The record is three fields. Writing the encoder by hand
is ~20 lines and means we understand every byte — which pays off when we debug a
torn-write test. We can revisit if records grow complex (they won't in Track A).

---

## 3. Recommended record format

Putting 2a–2d together, one WAL record on disk:

```
┌──────────┬──────────┬─────────┬──────────┬─────────┬──────────┐
│ crc32c   │ length   │ op      │ key       │ vlen     │ value    │
│ 4 bytes  │ 4 bytes  │ 1 byte  │ len-pref  │ 4 bytes  │ vlen     │
└──────────┴──────────┴─────────┴──────────┴─────────┴──────────┘
   └── CRC and length cover everything to their right ──┘
```

- `crc32c` — over `[length .. end of value]`. Checked first on replay.
- `length` — byte count of the payload after this field. Lets replay know
  exactly how far this record extends before trusting any inner field.
- `op` — `0x01 = PUT`, `0x02 = DELETE`. (DELETE carries a key, no value.)
- key is itself length-prefixed inside the payload; value uses `vlen`.

`op` + inner length prefixes mean a DELETE just sets `vlen = 0`. Clean.

---

## 4. Implementation approach

Two structs, roughly:

- `WalWriter` — owns the open file handle (append mode). `append(op, k, v)`:
  1. encode payload into a buffer,
  2. compute CRC32C over it,
  3. write `crc || len || payload` to the file,
  4. `fsync`,
  5. return only after fsync succeeds.

  Order matters: state (the `HashMap`) is updated by the *caller* only after
  `append` returns Ok. If fsync fails, we return Err and the caller must not
  treat the write as acked. Never update in-memory state before the WAL is
  durable — that's the invariant the entire project leans on.

- `WalReader` / `replay()` — open the file, loop:
  1. read 4-byte CRC; if EOF here, clean end → stop.
  2. read 4-byte length; if fewer than 4 bytes available → torn tail → stop.
  3. read `length` bytes; if fewer available → torn tail → stop.
  4. recompute CRC; if mismatch → torn/corrupt tail → stop.
  5. decode payload, apply to the `HashMap`.

  Every "stop" in 1–4 is a *clean* stop, not an error: it means "the log ended
  here, possibly mid-write, and that's fine." Return the rebuilt map.

Keep `WalWriter` and `WalReader` in `storage/src/wal.rs` (matches `DESIGN.md`
§9). The `HashMap` lives in a thin `Db` wrapper that calls `wal.append` then
mutates the map — this wrapper becomes the seam where the memtable slots in
next phase.

---

## 5. Edge cases to handle now (cheap here, expensive later)

- **Torn write at tail** — the main event. Handled by CRC + length above.
- **Zero-length / empty WAL file** — replay returns an empty map, no error.
- **fsync failure** — propagate as Err; do not ack. (Rare, but silently
  swallowing it reintroduces exactly the data-loss we're preventing.)
- **DELETE of a missing key** — legal; records a tombstone-ish op. In phase 1
  with a HashMap it just means `remove` on an absent key = no-op, still logged.
- **Very large value** — `length`/`vlen` are u32 (4 GiB cap). Fine; document it.

Explicitly *not* handled yet (later phases): log rotation/truncation after
flush (Phase 3 discards WAL covered by a flushed SSTable), and mid-file
corruption (we only trust a clean prefix; a bit-flip in the *middle* stops
replay early — acceptable for a single-node WAL, and Raft handles the
replicated case).

---

## 6. Test plan (expands the done-when)

1. **Happy path** — write 100 keys, replay, all 100 present with right values.
2. **Overwrite** — PUT k=1, PUT k=2 for same key; replay → last value wins.
3. **Delete** — PUT then DELETE a key; replay → key absent.
4. **kill -9 mid-run** — the headline test. Spawn a writer that PUTs in a loop
   and prints each key *after* append returns; kill it externally; restart and
   replay; assert every printed key is present. (Printed = acked = must survive.)
5. **Torn tail, synthetic** — append 10 good records, then manually append a
   truncated 11th (write CRC+length but only half the payload); replay must
   recover exactly 10 and stop cleanly, no panic.
6. **Corrupt CRC, synthetic** — flip one byte in a record's payload; replay
   stops at that record; everything before it survives.

Tests 5 and 6 are the ones that prove the corruption story — they're worth more
than the happy-path tests and are easy to write by poking bytes into the file.

---

## 7. Open decision to lock before coding

- **Checksum:** CRC32C (recommended) vs xxHash. Recommend CRC32C — hardware
  accelerated, and it's the choice you can point at etcd/RocksDB for.
- **Crate choice** for CRC32C in Rust: `crc32c` crate (uses SSE4.2) vs pulling
  it from a bigger crate. Recommend the small `crc32c` crate.

Everything else above (length-prefix framing, per-append fsync, hand-rolled
encoding) I'd treat as decided unless you want to push back.
