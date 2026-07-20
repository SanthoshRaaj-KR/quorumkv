//! A tiny end-to-end demo of the Phase 1 store, with logging on.
//!
//! Run it and watch the recovery/ops logs on stderr:
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
    std::fs::create_dir_all(&dir)?;
    let wal = dir.join("wal.log");
    let _ = std::fs::remove_file(&wal); // start clean each run

    // First session: a few writes, an overwrite, and a delete.
    {
        let db = Db::open(&wal)?;
        db.put(b"name", b"quorumkv")?;
        db.put(b"lang", b"rust")?;
        db.put(b"name", b"quorumkv-storage")?; // overwrite
        db.delete(b"lang")?; // tombstone
        log::info!(target: "demo", "session 1: name = {:?}", show(db.get(b"name")));
        log::info!(target: "demo", "session 1: lang = {:?}", show(db.get(b"lang")));
    } // drop -> close

    // Second session: reopen and prove state was rebuilt from the WAL.
    {
        let db = Db::open(&wal)?;
        log::info!(
            target: "demo",
            "session 2 (after reopen): {} key(s), name = {:?}, lang = {:?}",
            db.len(),
            show(db.get(b"name")),
            show(db.get(b"lang")),
        );
    }

    Ok(())
}

/// Render an optional value as a lossy string for display.
fn show(v: Option<Vec<u8>>) -> Option<String> {
    v.map(|b| String::from_utf8_lossy(&b).into_owned())
}
