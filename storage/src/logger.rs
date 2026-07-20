//! Centralized logging for the storage engine.
//!
//! We use the ecosystem-standard [`log`] facade (so `log::info!`, `log::trace!`,
//! … work anywhere in the crate) backed by a tiny custom writer defined here.
//! We deliberately do *not* pull `env_logger`/`tracing-subscriber`: on this
//! project's windows-gnu toolchain those drag in `windows-sys`, which can't link
//! (see `Cargo.toml`). This backend is pure std.
//!
//! ## Usage
//!
//! Call [`init`] once at the start of a binary (library code never initializes a
//! logger — that's the application's choice). Then use the `log` macros:
//!
//! ```no_run
//! storage::logger::init();
//! log::info!(target: "db", "opening store");
//! ```
//!
//! Output goes to **stderr** (so it never mixes with a program's stdout) as:
//!
//! ```text
//! +    12ms INFO  db: opening C:\...\wal.log
//! ```
//!
//! The level is read once from the `QUORUMKV_LOG` env var (`trace`/`debug`/
//! `info`/`warn`/`error`/`off`), defaulting to `info`. If no logger is
//! initialized, every `log` macro compiles to a cheap no-op.

use std::io::Write;
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Instant;

use log::{LevelFilter, Log, Metadata, Record};

/// A minimal `log` backend that writes level-tagged lines to stderr.
struct StderrLogger {
    level: LevelFilter,
}

static LOGGER: OnceLock<StderrLogger> = OnceLock::new();
/// Process start, so each line carries a monotonic `+Nms` stamp (no date crate).
static START: OnceLock<Instant> = OnceLock::new();

impl Log for StderrLogger {
    fn enabled(&self, meta: &Metadata) -> bool {
        meta.level() <= self.level
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let ms = START.get_or_init(Instant::now).elapsed().as_millis();
        // Lock stderr so lines from multiple threads don't interleave.
        let stderr = std::io::stderr();
        let mut h = stderr.lock();
        let _ = writeln!(
            h,
            "+{ms:>6}ms {:<5} {}: {}",
            record.level(),
            record.target(),
            record.args()
        );
    }

    fn flush(&self) {
        let _ = std::io::stderr().flush();
    }
}

/// Install the logger, taking the level from `QUORUMKV_LOG` (default `info`).
///
/// Idempotent and safe to call more than once: the first call wins, later calls
/// are ignored (the `log` crate only allows one global logger).
pub fn init() {
    init_with(level_from_env());
}

/// Install the logger at an explicit level. See [`init`].
pub fn init_with(level: LevelFilter) {
    START.get_or_init(Instant::now);
    let logger = LOGGER.get_or_init(|| StderrLogger { level });
    // `set_logger` errors only if a logger is already set — that's fine.
    if log::set_logger(logger).is_ok() {
        log::set_max_level(logger.level);
    }
}

fn level_from_env() -> LevelFilter {
    match std::env::var("QUORUMKV_LOG") {
        Ok(s) => LevelFilter::from_str(s.trim()).unwrap_or(LevelFilter::Info),
        Err(_) => LevelFilter::Info,
    }
}
