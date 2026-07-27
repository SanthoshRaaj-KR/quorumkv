//! The Phase 10 sidecar — a tiny local HTTP server over one node's `Db`.
//! See `planning/phase-10-apply-seam.md` §1/§6.
//!
//! Not part of the library API. One process per Raft node, paired: the Go
//! node talks only to *its own* sidecar, never another node's. Speaks just
//! enough of HTTP/1.1 (request line, headers until a blank line, a
//! `Content-Length` body) to be `curl`-able, over `std::net` only — zero new
//! crates, matching `wal_crash_writer`'s own precedent of a plain binary
//! against `Db` directly, and the project's habit of hand-rolling wire
//! formats rather than depending on a library for a locally-trusted link.
//!
//! One connection handled at a time, to completion, before the next is
//! accepted — this is a control link for one client, not a service under
//! load, so there is no threading and no keep-alive to reason about.
//!
//! Usage: `sidecar <dir>`. Binds `127.0.0.1:0` (an OS-assigned port) and
//! prints `port=<N>` as its first stdout line so the parent process can
//! read it back — the same "print an ack, let the parent read it" shape
//! `wal_crash_writer` already uses for its acknowledged-key protocol.

use std::env;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use storage::db::Db;

fn main() {
    let dir = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: sidecar <dir>");
        std::process::exit(2);
    });

    let mut db = Db::open(&dir).unwrap_or_else(|e| {
        eprintln!("sidecar: open db at {dir}: {e}");
        std::process::exit(1);
    });

    let listener = TcpListener::bind("127.0.0.1:0").unwrap_or_else(|e| {
        eprintln!("sidecar: bind: {e}");
        std::process::exit(1);
    });
    let port = listener.local_addr().expect("local_addr").port();

    // The readiness signal the parent process waits for.
    println!("port={port}");
    io::stdout().flush().ok();
    eprintln!("sidecar: dir={dir} port={port}");

    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                if let Err(e) = handle(stream, &mut db, &dir) {
                    eprintln!("sidecar: connection error: {e}");
                }
            }
            Err(e) => eprintln!("sidecar: accept error: {e}"),
        }
    }
}

/// Read one request, dispatch it, write back one response. `db` is `&mut`
/// only because `/restore` replaces the whole store (a fresh `Db` over a
/// wiped directory) — safe with no locking because connections are handled
/// strictly one at a time. `Arc<Db>` (not a bare `Db`) since Phase 5's
/// background compaction (§8 A3) needs a real owning handle that can outlive
/// the `put`/`delete` call that spawned it.
fn handle(mut stream: TcpStream, db: &mut Arc<Db>, dir: &str) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);

    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(()); // client connected and closed without sending anything
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();

    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break; // end of headers
        }
        if let Some(rest) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            content_length = rest.trim().parse().unwrap_or(0);
        }
    }

    let mut body_bytes = vec![0u8; content_length];
    reader.read_exact(&mut body_bytes)?;
    let body = String::from_utf8_lossy(&body_bytes).into_owned();

    let (status, resp_body) = route(&method, &path, &body, db, dir);

    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        resp_body.len()
    )?;
    stream.write_all(resp_body.as_bytes())?;
    stream.flush()
}

fn route(method: &str, path: &str, body: &str, db: &mut Arc<Db>, dir: &str) -> (&'static str, String) {
    match (method, path) {
        ("POST", "/put") => handle_put(body, db),
        ("POST", "/delete") => handle_delete(body, db),
        ("POST", "/get") => handle_get(body, db),
        ("POST", "/snapshot") => handle_snapshot(db),
        ("POST", "/restore") => handle_restore(body, db, dir),
        ("GET", "/stats") => handle_stats(db),
        _ => ("404 Not Found", json_error("unknown endpoint")),
    }
}

fn handle_put(body: &str, db: &Db) -> (&'static str, String) {
    let (Some(k), Some(v)) = (json::get_str(body, "key"), json::get_str(body, "value")) else {
        return ("400 Bad Request", json_error("missing key/value"));
    };
    let (Ok(key), Ok(value)) = (base64::decode(&k), base64::decode(&v)) else {
        return ("400 Bad Request", json_error("invalid base64"));
    };
    match db.put(&key, &value) {
        Ok(()) => ("200 OK", "{}".to_string()),
        Err(e) => ("500 Internal Server Error", json_error(&e.to_string())),
    }
}

fn handle_delete(body: &str, db: &Db) -> (&'static str, String) {
    let Some(k) = json::get_str(body, "key") else {
        return ("400 Bad Request", json_error("missing key"));
    };
    let Ok(key) = base64::decode(&k) else {
        return ("400 Bad Request", json_error("invalid base64"));
    };
    match db.delete(&key) {
        Ok(()) => ("200 OK", "{}".to_string()),
        Err(e) => ("500 Internal Server Error", json_error(&e.to_string())),
    }
}

fn handle_get(body: &str, db: &Db) -> (&'static str, String) {
    let Some(k) = json::get_str(body, "key") else {
        return ("400 Bad Request", json_error("missing key"));
    };
    let Ok(key) = base64::decode(&k) else {
        return ("400 Bad Request", json_error("invalid base64"));
    };
    match db.get(&key) {
        Ok(Some(v)) => ("200 OK", format!("{{\"value\":\"{}\"}}", base64::encode(&v))),
        Ok(None) => ("200 OK", "{\"value\":null}".to_string()),
        Err(e) => ("500 Internal Server Error", json_error(&e.to_string())),
    }
}

fn handle_snapshot(db: &Db) -> (&'static str, String) {
    match db.snapshot_blob() {
        Ok(blob) => ("200 OK", format!("{{\"data\":\"{}\"}}", base64::encode(&blob))),
        Err(e) => ("500 Internal Server Error", json_error(&e.to_string())),
    }
}

/// Replace `*db` entirely with a fresh store restored from the blob — see
/// [`Db::restore`]: it wipes `dir` first (an installed snapshot is
/// authoritative), so whatever this sidecar held before is gone.
///
/// [`Db::restore`] is a plain associated function — it has no way to know
/// about *this* process's separate, still-live `Db` handle over the same
/// directory, so if a background compaction (§8 A3) were still merging
/// files there when the wipe happens, that would be a real, silent
/// directory-out-from-under-a-running-merge race. `wait_for_compactions`
/// drains it first, on the OLD handle, before `Db::restore` ever touches
/// disk.
fn handle_restore(body: &str, db: &mut Arc<Db>, dir: &str) -> (&'static str, String) {
    let Some(d) = json::get_str(body, "data") else {
        return ("400 Bad Request", json_error("missing data"));
    };
    let Ok(blob) = base64::decode(&d) else {
        return ("400 Bad Request", json_error("invalid base64"));
    };
    db.wait_for_compactions();
    match Db::restore(dir, &blob) {
        Ok(new_db) => {
            *db = new_db;
            ("200 OK", "{}".to_string())
        }
        Err(e) => ("500 Internal Server Error", json_error(&e.to_string())),
    }
}

fn handle_stats(db: &Db) -> (&'static str, String) {
    (
        "200 OK",
        format!("{{\"sstables\":{},\"approxSize\":{}}}", db.sstable_count(), db.approx_size()),
    )
}

fn json_error(msg: &str) -> String {
    format!("{{\"error\":\"{}\"}}", json::escape(msg))
}

/// Minimal encode/decode for the flat, fixed-shape JSON objects this
/// protocol uses — not a general JSON library. Sufficient because every
/// value here is either a base64 string, a number we only ever encode, or
/// null.
mod json {
    pub fn escape(s: &str) -> String {
        let mut out = String::with_capacity(s.len() + 2);
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
                c => out.push(c),
            }
        }
        out
    }

    /// Find `"field": "..."` and return its unescaped contents, or `None` if
    /// the field is absent or its value is `null`.
    pub fn get_str(body: &str, field: &str) -> Option<String> {
        let key = format!("\"{field}\"");
        let key_pos = body.find(&key)?;
        let after_key = &body[key_pos + key.len()..];
        let colon = after_key.find(':')?;
        let after_colon = after_key[colon + 1..].trim_start();
        if after_colon.starts_with("null") {
            return None;
        }
        let rest = after_colon.strip_prefix('"')?;

        let mut out = String::new();
        let mut chars = rest.chars();
        while let Some(c) = chars.next() {
            match c {
                '"' => return Some(out),
                '\\' => match chars.next()? {
                    '"' => out.push('"'),
                    '\\' => out.push('\\'),
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    other => out.push(other),
                },
                c => out.push(c),
            }
        }
        None // unterminated string — malformed request
    }
}

/// A hand-rolled, standard (RFC 4648, padded) base64 codec — the same
/// "understand every byte, no new dependency" call as `json` above.
mod base64 {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    pub fn encode(data: &[u8]) -> String {
        let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
        for chunk in data.chunks(3) {
            let b0 = chunk[0];
            let b1 = *chunk.get(1).unwrap_or(&0);
            let b2 = *chunk.get(2).unwrap_or(&0);
            let n = ((b0 as u32) << 16) | ((b1 as u32) << 8) | (b2 as u32);
            out.push(ALPHABET[(n >> 18 & 0x3F) as usize] as char);
            out.push(ALPHABET[(n >> 12 & 0x3F) as usize] as char);
            out.push(if chunk.len() > 1 { ALPHABET[(n >> 6 & 0x3F) as usize] as char } else { '=' });
            out.push(if chunk.len() > 2 { ALPHABET[(n & 0x3F) as usize] as char } else { '=' });
        }
        out
    }

    pub fn decode(s: &str) -> Result<Vec<u8>, String> {
        fn val(c: u8) -> Result<u8, String> {
            match c {
                b'A'..=b'Z' => Ok(c - b'A'),
                b'a'..=b'z' => Ok(c - b'a' + 26),
                b'0'..=b'9' => Ok(c - b'0' + 52),
                b'+' => Ok(62),
                b'/' => Ok(63),
                _ => Err(format!("invalid base64 byte {c:#x}")),
            }
        }
        let s = s.trim_end_matches('=');
        let bytes = s.as_bytes();
        let mut out = Vec::with_capacity(bytes.len() / 4 * 3 + 3);
        for chunk in bytes.chunks(4) {
            let mut n: u32 = 0;
            for (i, &c) in chunk.iter().enumerate() {
                n |= (val(c)? as u32) << (18 - 6 * i);
            }
            out.push((n >> 16) as u8);
            if chunk.len() > 2 {
                out.push((n >> 8) as u8);
            }
            if chunk.len() > 3 {
                out.push(n as u8);
            }
        }
        Ok(out)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn round_trips_arbitrary_bytes() {
            for data in [&b""[..], b"f", b"fo", b"foo", b"foob", b"fooba", b"foobar", &[0u8, 255, 128, 1]] {
                assert_eq!(decode(&encode(data)).unwrap(), data);
            }
        }

        #[test]
        fn matches_known_vectors() {
            // RFC 4648 §10 test vectors.
            assert_eq!(encode(b""), "");
            assert_eq!(encode(b"f"), "Zg==");
            assert_eq!(encode(b"fo"), "Zm8=");
            assert_eq!(encode(b"foo"), "Zm9v");
            assert_eq!(encode(b"foobar"), "Zm9vYmFy");
        }
    }
}
