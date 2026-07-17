# quorumkv — design document

A replicated key-value store combining Raft (consensus/ordering) and an
LSM-tree (local storage engine). Core: Rust. Cluster RPC/orchestration: Go.

---

## 1. Problem statement

A single-machine key-value store is fast but has one obvious failure mode:
the machine dies, or a disk fails, and the data is gone. The standard fix is
replication — keep copies on multiple machines — but replication introduces
a much harder problem: **how do multiple machines agree on the order writes
happened in**, especially when some of them crash, restart, or get cut off
from the network at arbitrary times?

That agreement problem is what Raft solves. It's not new — it's a
well-studied, well-proven algorithm (published 2014, designed explicitly to
be more understandable than Paxos). This project is **not** proposing a new
consensus algorithm or a new storage engine design. It is an implementation
of two known, production-proven techniques, built to demonstrate they can be
combined correctly and hold up under simulated failure.

### Existing solutions (so we're honest about prior art)

| System | Consensus | Storage engine |
|---|---|---|
| etcd | Raft | bbolt (B-tree) |
| TiKV / TiDB | Multi-Raft | RocksDB (LSM-tree) |
| CockroachDB | Multi-Raft | Pebble (LSM-tree) |
| Cassandra | Gossip + quorum reads/writes (no single leader) | LSM-tree |

`quorumkv`'s architecture (single Raft group + LSM-tree) is closest to a
minimal, single-shard slice of TiKV. The value of building it isn't
novelty — it's (a) proving the implementation is correct under adversarial
conditions like leader crashes and network partitions, which most portfolio
implementations never actually test, and (b) understanding *why* production
systems are built this way by having built a working version yourself.

### Goals

- Durable, replicated key-value storage: `PUT(k, v)`, `GET(k)`, `DELETE(k)`.
- Survive a minority of node failures with zero data loss on committed writes.
- Local storage that stays fast as data grows past available RAM.
- A documented, testable consistency model (not just "it's consistent, trust me").

### Non-goals (for v1)

- Multi-Raft / sharding across key ranges (candidate for v2, see §7).
- Multi-region / geo-replication.
- SQL or any query layer above simple key-value operations.
- Novel algorithmic contributions — this is an implementation project.

---

## 2. High-level architecture

```
                        ┌──────────┐
                        │  Client  │
                        └────┬─────┘
                             │ PUT/GET (redirected to leader if needed)
                             ▼
      ┌──────────────────────────────────────────┐
      │              Leader node                   │
      │  ┌────────────────────────────────────┐   │
      │  │ Go: RPC layer + Raft log            │   │
      │  └───────────────┬────────────────────┘   │
      │                  │ apply(committed entry)  │
      │  ┌───────────────▼────────────────────┐   │
      │  │ Rust: LSM storage engine            │   │
      │  └────────────────────────────────────┘   │
      └───────────┬───────────────────┬────────────┘
                  │ AppendEntries     │ AppendEntries
                  ▼                   ▼
      ┌───────────────────┐  ┌───────────────────┐
      │  Follower node      │  │  Follower node      │
      │  Go: RPC + Raft log │  │  Go: RPC + Raft log │
      │  Rust: LSM engine   │  │  Rust: LSM engine   │
      └───────────────────┘  └───────────────────┘
```

Two independent layers per node:

1. **Consensus layer (Go)** — owns the Raft log, leader election, RPCs
   between nodes. Doesn't know anything about how data is actually stored.
2. **Storage layer (Rust)** — owns the LSM-tree on that single machine.
   Doesn't know anything about replication; it just durably applies
   whatever command the consensus layer hands it.

They connect at exactly one point: when Raft marks a log entry as
*committed*, it calls `apply(entry)` on the local storage engine.

---

## 3. Component 1: Raft consensus layer, in detail

### State every node tracks

- `currentTerm` — monotonically increasing counter, bumped on every election.
  Terms are how a node detects it has stale information: any message
  carrying a higher term means "whatever you believed is now outdated, defer."
- `votedFor` — who this node voted for in the current term (prevents double voting).
- `log[]` — ordered list of `{term, index, command}` entries.
- `commitIndex` — highest log index known to be replicated to a majority.
- `lastApplied` — highest log index actually applied to the local storage engine.

### Leader election — `RequestVote` RPC

Every follower has a randomized election timeout (~150–300ms). If no
heartbeat arrives in that window, it assumes there is no leader, increments
`currentTerm`, transitions to candidate, and sends
`RequestVote{term, candidateId, lastLogIndex, lastLogTerm}` to every peer.

A voter grants its vote only if:
1. The candidate's term is ≥ its own term, **and**
2. The candidate's log is at least as up-to-date as the voter's own
   (compare `lastLogTerm` first, then `lastLogIndex`).

Condition 2 is the safety-critical check: it prevents a node that missed
recent writes from ever becoming leader and silently overwriting committed data.

The randomized timeout is what prevents every follower from becoming a
candidate at the same instant and splitting the vote indefinitely.

### Log replication — `AppendEntries` RPC

The leader sends `AppendEntries{term, prevLogIndex, prevLogTerm, entries[],
leaderCommit}`. A follower **rejects** the RPC if its log doesn't contain an
entry at `prevLogIndex` with a matching `prevLogTerm`.

This is the **log matching property**: if two logs agree at some index,
they are identical for every entry before it. On rejection, the leader
decrements `prevLogIndex` and retries until it finds the last point of
agreement, then overwrites everything after it with its own entries — this
is exactly the mechanism that repairs a follower that fell behind or diverged.

An entry is **committed** once a majority of nodes have durably appended it.
The leader applies committed entries to its own storage engine and
propagates the updated `commitIndex` on the next heartbeat so followers can
safely apply them too. **Followers never apply an entry before it's marked
committed** — this is what prevents a follower from exposing data that a
future leader change could still overwrite.

### Log compaction — snapshotting

The log can't grow forever. Periodically, a node takes a snapshot of its
currently-applied state — in practice, this can just be the LSM engine's own
current SSTable set, since those are already an immutable point-in-time
view — and discards log entries older than the snapshot. A follower that has
fallen too far behind gets sent the snapshot directly (`InstallSnapshot` RPC)
instead of thousands of individual log entries.

---

## 4. Component 2: LSM-tree storage engine, in detail

### Write path

```
Write(k, v)
   │
   ├──────────────► Write-ahead log (WAL)   — durability, append-only, fsync'd
   │
   └──────────────► Memtable (in-memory, sorted, skip list)
                          │
                          │  fills up
                          ▼
                    Immutable memtable
                          │
                          │  flush
                          ▼
                    SSTable (on disk, sorted, has a Bloom filter)
                          │
                          │  older SSTables accumulate
                          ▼
                    Compaction (merges SSTables, drops overwritten/deleted keys)
```

- **Write-ahead log (WAL):** every write is appended and fsync'd here
  *before* anything else happens. If the process crashes right after
  acknowledging a write, replaying the WAL on restart reconstructs the
  memtable exactly. It's a pure sequential append, so it's fast even with
  fsync on every write.
- **Memtable:** in-memory sorted structure. Skip list is the standard
  choice — O(log n) insert/lookup, no rebalancing needed (unlike a balanced
  tree), which makes concurrent access much easier to reason about. Once it
  hits a size threshold (e.g. 4–64MB) it's frozen as immutable and a fresh
  one takes over — writes never block on the flush.
- **SSTable (Sorted String Table):** the flushed, on-disk form. Key-value
  pairs in sorted order, grouped into fixed-size data blocks, plus a sparse
  block index and a Bloom filter over all keys in the file. **Immutable**
  once written — never edited in place, only superseded by compaction. This
  immutability is exactly what makes concurrent reads safe without locking.
- **Compaction:** merges multiple SSTables into fewer, larger ones, dropping
  overwritten keys and purging delete "tombstones" along the way. Two
  standard strategies:
  - *Size-tiered* — merge similarly-sized SSTables together. Cheaper writes,
    worse read/space amplification. Simpler to implement first.
  - *Leveled* — organize SSTables into levels of increasing size, each level
    non-overlapping. Better read performance, more compaction I/O. What
    RocksDB defaults to.

### Read path

Check, in order: live memtable → immutable memtable → SSTables, newest to
oldest. Before touching an SSTable's data on disk, check its **Bloom
filter** — a probabilistic "definitely not present" / "maybe present" test
that lets most irrelevant files be skipped with zero disk I/O, at the cost
of a small, tunable false-positive rate. This is why LSM reads stay fast
even though the data is spread across many files.

---

## 5. End-to-end data flow

### Write

1. Client sends `PUT(k, v)` to whichever node it believes is leader.
2. If that node isn't leader, it rejects with a redirect to the current
   leader (or a `NotLeaderError` plus a last-known-leader hint).
3. Leader appends `{term, index, PUT(k, v)}` to its own Raft log.
4. Leader sends `AppendEntries` to all followers in parallel.
5. Once a majority have durably appended the entry, the leader marks it
   committed, applies it to its local LSM engine, and responds to the client.
6. On the next heartbeat, followers learn the new `commitIndex` and apply
   the entry to their own LSM engines.

### Read (pick one consistency mode and document the tradeoff)

- **Linearizable:** leader confirms it's still leader (round-trip heartbeat
  to a majority, ruling out a silent partition-induced leader change) before
  answering from its own store. Strongest, slowest.
- **Leader-only, no confirmation:** answers immediately from the leader's
  store. Faster, but a stale read is possible in a narrow partition window.
- **Follower read-index:** follower asks the leader for the current commit
  index, waits for its own applied index to catch up, then answers locally.
  Spreads read load off the leader without sacrificing consistency.

---

## 6. Build roadmap

1. **LSM engine alone**, single-threaded, no networking. WAL + memtable +
   flush to SSTable + basic (size-tiered) compaction + Bloom filter.
   *Test:* kill the process mid-write, restart, verify WAL replay recovers
   exactly the acknowledged writes.
2. **Single-node Raft** ("cluster" of one) to get election timers, term
   handling, and log append mechanics right in isolation, before real
   network failures are in the picture.
3. **Multi-node Raft over real RPC** (Go, gRPC). Bring up 3–5 nodes, verify
   leader election and log replication converge correctly.
4. **Wire Raft's `apply` callback into the LSM engine** — the one point
   where the two halves connect.
5. **Snapshotting** using the LSM engine's current SSTable set as the
   snapshot payload.
6. **Client library** with leader discovery, redirect-following, and retries.
7. **Chaos test suite** (this is what makes the project resume-credible
   rather than "another Raft tutorial repo" — see §8).

---

## 7. Future work (v2 candidate)

- **Multi-Raft sharding:** partition the keyspace into ranges, each with its
  own independent Raft group, so the cluster scales past what a single
  leader can throughput. This is the actual architectural change that gets
  you from "toy" to "TiKV-shaped."
- **Benchmarking against real etcd/TiKV** on the same workload, with a
  written explanation of where and why performance differs.

---

## 8. Testing & validation plan

The correctness claim of this whole project rests on this section, not on
the implementation existing. At minimum:

- Kill the leader mid-write; verify no *acknowledged* write is ever lost.
- Partition a minority of nodes away; verify the majority side keeps
  serving writes and reads correctly.
- Heal a partition; verify the previously-isolated side catches up via log
  repair (or snapshot install, if it fell far enough behind) without
  diverging from the majority's history.
- Fill the disk mid-compaction; verify the engine doesn't corrupt existing
  SSTables (compaction should write to new files and only swap them in on
  success).
- Optional stretch: a Jepsen-style harness that injects these faults
  automatically and checks linearizability of the recorded operation history.

---

## 9. Suggested repo layout

```
quorumkv/
├── DESIGN.md                 (this file)
├── storage/                  (Rust: the LSM-tree engine)
│   ├── src/
│   │   ├── wal.rs
│   │   ├── memtable.rs
│   │   ├── sstable.rs
│   │   ├── compaction.rs
│   │   └── bloom.rs
│   └── Cargo.toml
├── consensus/                 (Go: Raft + cluster RPC)
│   ├── raft.go
│   ├── rpc.go
│   ├── election.go
│   └── snapshot.go
├── client/                    (Go: client library)
├── chaos/                     (fault-injection test harness)
└── docs/
    └── benchmarks.md
```

---

## References

- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm*
  (the original Raft paper) — read this before writing a line of code.
- O'Neil et al., *The Log-Structured Merge-Tree (LSM-Tree)* (1996).
- RocksDB and TiKV documentation, for how these ideas look in a mature
  production implementation once yours is working end-to-end.
