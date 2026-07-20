//! quorumkv storage engine (Track A — local LSM key-value store).
//!
//! Built phase by phase per `planning/`:
//!   Phase 1 — WAL (durability)         -> `wal`, exposed via `db::Db`
//!   Phase 2 — Memtable                 (later)
//!   Phase 3 — SSTable flush            (later)
//!   Phase 4 — Bloom filter             (later)
//!   Phase 5 — Compaction               (later)
//!
//! Nothing here knows about clusters or replication; that is Track B (Go).

pub mod db;
pub mod logger;
pub mod memtable;
pub mod wal;

#[cfg(test)]
mod testutil;
