//! A tiny end-to-end demo of the store, with logging on. Phase 3: it now flushes
//! to an SSTable and reads back across the memtable + disk tiers.
//!
//! Run it and watch the recovery/flush/ops logs on stderr:
//!
//! ```text
//! QUORUMKV_LOG=trace cargo run --example demo
//! ```
//!
//! (default level is `info` if `QUORUMKV_LOG` is unset).

use storage::db::Db;
use storage::logger;

fn main() -> std::io::Result<()> {
    logger::init();

    let dir = std::env::temp_dir().join("quorumkv-demo");
    let _ = std::fs::remove_dir_all(&dir); // start clean each run

    // First session: a few writes, an overwrite, a delete, then a flush to disk.
    {
        let db = Db::open(&dir)?;
        db.put(b"name", b"quorumkv")?;
        db.put(b"lang", b"rust")?;
        db.put(b"name", b"quorumkv-storage")?; // overwrite
        db.delete(b"lang")?; // tombstone
        db.flush()?; // freeze the memtable -> SSTable
        log::info!(target: "demo", "session 1: name = {:?}", show(db.get(b"name")?));
        log::info!(target: "demo", "session 1: lang = {:?}", show(db.get(b"lang")?));
    } // drop -> close

    // Second session: reopen and prove state was rebuilt from SSTable + WAL.
    {
        let db = Db::open(&dir)?;
        log::info!(
            target: "demo",
            "session 2 (after reopen): {} key(s), name = {:?}, lang = {:?}",
            db.len()?,
            show(db.get(b"name")?),
            show(db.get(b"lang")?),
        );
    }

    Ok(())
}

/// Render an optional value as a lossy string for display.
fn show(v: Option<Vec<u8>>) -> Option<String> {
    v.map(|b| String::from_utf8_lossy(&b).into_owned())
}
