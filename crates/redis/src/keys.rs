//! The commands over the keyspace: SCAN, KEYS, RENAME, RANDOMKEY, FLUSHDB; and
//! the sweep that deletes the rows of expired keys.
//!
//! Keys are the table's primary key, in byte order, so SCAN walks them from where
//! the last call stopped: every key present from the first call to the last is
//! returned once, whatever is added or deleted meanwhile. A pattern's literal
//! prefix (`session:*`) becomes a range of the key, so it reads only those keys.

use crate::cmd::{Error, STRING, SYNTAX, Store, entry, fail, upper};
use crate::hash;
use crate::resp::Reply;
use rusqlite::{Connection, params};

/// The most rows of expired keys one sweep commit deletes.
pub const SWEEP_BATCH: usize = 1000;

/// Deletes up to SWEEP_BATCH rows of keys expired at now; how many.
///
/// # Errors
///
/// SQLite's.
pub fn sweep(c: &Connection, now: i64) -> rusqlite::Result<usize> {
    let gone: Vec<Vec<u8>> = c
        .prepare_cached("SELECT key FROM bucketdb_kv WHERE expires_at <= ?1 LIMIT ?2")?
        .query_map(params![now, SWEEP_BATCH as i64], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    for key in &gone {
        c.prepare_cached("DELETE FROM bucketdb_kv WHERE key = ?1")?
            .execute([key])?;
        hash::drop_fields(c, key)?;
    }
    Ok(gone.len())
}

/// Whether any key has expired by now, so a sweep has rows to delete.
///
/// # Errors
///
/// SQLite's.
pub fn expired(c: &Connection, now: i64) -> rusqlite::Result<bool> {
    c.prepare_cached("SELECT EXISTS (SELECT 1 FROM bucketdb_kv WHERE expires_at <= ?1)")?
        .query_row([now], |r| r.get(0))
}

/// Whether s matches a Redis glob pattern: `*`, `?`, `[abc]`, `[^a-z]`, `\x`.
pub fn glob(pattern: &[u8], s: &[u8]) -> bool {
    let (mut p, mut i) = (0, 0);
    // Where the last `*` was, and where in s it is tried to end next.
    let mut star: Option<(usize, usize)> = None;
    while i < s.len() {
        if p < pattern.len() {
            match pattern[p] {
                b'*' => {
                    star = Some((p, i));
                    p += 1;
                    continue;
                }
                b'?' => {
                    p += 1;
                    i += 1;
                    continue;
                }
                b'[' => {
                    if let Some((hit, next)) = class(pattern, p, s[i])
                        && hit
                    {
                        p = next;
                        i += 1;
                        continue;
                    }
                }
                b'\\' if p + 1 < pattern.len() => {
                    if pattern[p + 1] == s[i] {
                        p += 2;
                        i += 1;
                        continue;
                    }
                }
                c if c == s[i] => {
                    p += 1;
                    i += 1;
                    continue;
                }
                _ => {}
            }
        }
        // A mismatch: the last `*` takes one more byte, or there is none to.
        let Some((sp, si)) = star else {
            return false;
        };
        star = Some((sp, si + 1));
        p = sp + 1;
        i = si + 1;
    }
    pattern[p..].iter().all(|&c| c == b'*')
}

/// The class at pattern[p] (a `[`) against c: whether it matches, and where the
/// pattern goes on after it. An unclosed class runs to the pattern's end.
fn class(pattern: &[u8], p: usize, c: u8) -> Option<(bool, usize)> {
    let mut j = p + 1;
    let not = pattern.get(j) == Some(&b'^');
    if not {
        j += 1;
    }
    let mut hit = false;
    while j < pattern.len() && pattern[j] != b']' {
        if pattern[j] == b'\\' && j + 1 < pattern.len() {
            hit |= pattern[j + 1] == c;
            j += 2;
        } else if j + 2 < pattern.len() && pattern[j + 1] == b'-' && pattern[j + 2] != b']' {
            let (a, b) = (pattern[j].min(pattern[j + 2]), pattern[j].max(pattern[j + 2]));
            hit |= (a..=b).contains(&c);
            j += 3;
        } else {
            hit |= pattern[j] == c;
            j += 1;
        }
    }
    Some((hit != not, (j + 1).min(pattern.len())))
}

/// The bytes every match of pattern starts with.
fn literal_prefix(pattern: &[u8]) -> &[u8] {
    let end = pattern
        .iter()
        .position(|c| matches!(c, b'*' | b'?' | b'[' | b'\\'))
        .unwrap_or(pattern.len());
    &pattern[..end]
}

/// The first byte string after every one that starts with prefix; None when
/// there is none (a prefix of 0xff bytes only, or empty).
fn after_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// Live keys from `from` (inclusive) to `to` (exclusive, None: the end), in order,
/// at most limit; each with its type.
fn range(
    c: &Connection,
    from: &[u8],
    to: Option<&[u8]>,
    limit: usize,
    now: i64,
) -> rusqlite::Result<Vec<(Vec<u8>, i64)>> {
    let live = "(expires_at IS NULL OR expires_at > ?3)";
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let map = |r: &rusqlite::Row<'_>| Ok((r.get(0)?, r.get(1)?));
    match to {
        Some(to) => c
            .prepare_cached(&format!(
                "SELECT key, type FROM bucketdb_kv WHERE key >= ?1 AND key < ?2 AND {live} ORDER BY key LIMIT ?4"
            ))?
            .query_map(params![from, to, now, limit], map)?
            .collect(),
        None => c
            .prepare_cached(&format!(
                "SELECT key, type FROM bucketdb_kv WHERE key >= ?1 AND ?2 IS NULL AND {live} ORDER BY key LIMIT ?4"
            ))?
            .query_map(params![from, None::<Vec<u8>>, now, limit], map)?
            .collect(),
    }
}

fn type_code(name: &str) -> Option<i64> {
    match name {
        "STRING" => Some(STRING),
        "HASH" => Some(hash::HASH),
        _ => None,
    }
}

/// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
pub fn scan(s: &mut impl Store, args: &[Vec<u8>]) -> Result<Reply, Error> {
    let cursor: u64 = match std::str::from_utf8(&args[0]).ok().and_then(|c| c.parse().ok()) {
        Some(c) => c,
        None => return fail("ERR invalid cursor"),
    };
    let (mut pattern, mut count, mut kind) = (None, 10usize, None);
    let mut i = 1;
    while i < args.len() {
        let value = args.get(i + 1);
        match (upper(&args[i]).as_str(), value) {
            ("MATCH", Some(p)) => pattern = Some(p.clone()),
            ("COUNT", Some(n)) => match std::str::from_utf8(n).ok().and_then(|n| n.parse::<usize>().ok()) {
                Some(n) if n >= 1 => count = n,
                Some(_) => return fail(SYNTAX),
                None => return fail("ERR value is not an integer or out of range"),
            },
            ("TYPE", Some(t)) => match type_code(&upper(t)) {
                Some(k) => kind = Some(k),
                None => return fail(format!("ERR unknown type name '{}'", String::from_utf8_lossy(t))),
            },
            _ => return fail(SYNTAX),
        }
        i += 2;
    }
    // Where to go on from: just after the last key, or the pattern's prefix.
    let prefix = pattern.as_deref().map_or(&b""[..], literal_prefix).to_vec();
    let mut from = prefix.clone();
    if cursor != 0 {
        let Some(mut last) = s.load_cursor(cursor) else {
            return fail("ERR invalid cursor");
        };
        // The next byte string after last.
        last.push(0);
        from = from.max(last);
    }
    let to = after_prefix(&prefix).filter(|_| !prefix.is_empty());
    let rows = s.read(&mut |c, now| Ok(range(c, &from, to.as_deref(), count, now)?))?;
    let next = match rows.last() {
        Some((last, _)) if rows.len() == count => s.save_cursor(last.clone()),
        _ => 0,
    };
    let keys = rows
        .into_iter()
        .filter(|(k, t)| kind.is_none_or(|want| want == *t) && pattern.as_deref().is_none_or(|p| glob(p, k)))
        .map(|(k, _)| Reply::Bulk(k))
        .collect();
    Ok(Reply::Array(vec![
        Reply::Bulk(next.to_string().into_bytes()),
        Reply::Array(keys),
    ]))
}

/// KEYS pattern: every live key that matches, in one reply.
pub fn keys(s: &mut impl Store, pattern: &[u8]) -> Result<Reply, Error> {
    let prefix = literal_prefix(pattern);
    let to = after_prefix(prefix).filter(|_| !prefix.is_empty());
    let rows = s.read(&mut |c, now| Ok(range(c, prefix, to.as_deref(), usize::MAX, now)?))?;
    Ok(Reply::Array(
        rows.into_iter()
            .filter(|(k, _)| glob(pattern, k))
            .map(|(k, _)| Reply::Bulk(k))
            .collect(),
    ))
}

/// RENAME and RENAMENX: the key's value and expiry move to the new name.
pub fn rename(s: &mut impl Store, from: &[u8], to: &[u8], nx: bool) -> Result<Reply, Error> {
    s.write(&mut |c, now| {
        if entry(c, from, now)?.is_none() {
            return fail("ERR no such key");
        }
        if from == to {
            return Ok(if nx { Reply::Int(0) } else { Reply::ok() });
        }
        if nx && entry(c, to, now)?.is_some() {
            return Ok(Reply::Int(0));
        }
        // An expired row under the new name goes too, with any fields.
        c.prepare_cached("DELETE FROM bucketdb_kv WHERE key = ?1")?
            .execute([to])?;
        hash::drop_fields(c, to)?;
        c.prepare_cached("UPDATE bucketdb_kv SET key = ?2 WHERE key = ?1")?
            .execute([from, to])?;
        c.prepare_cached("UPDATE bucketdb_kv_hash SET key = ?2 WHERE key = ?1")?
            .execute([from, to])?;
        Ok(if nx { Reply::Int(1) } else { Reply::ok() })
    })
}

/// RANDOMKEY: a live key, each as likely. Counts the keys: as slow as DBSIZE.
pub fn random(s: &mut impl Store) -> Result<Reply, Error> {
    use std::hash::BuildHasher;
    let seed = std::collections::hash_map::RandomState::new().hash_one(crate::group::now_ms());
    s.read(&mut |c, now| {
        let live = "expires_at IS NULL OR expires_at > ?1";
        let n: i64 = c
            .prepare_cached(&format!("SELECT count(*) FROM bucketdb_kv WHERE {live}"))?
            .query_row([now], |r| r.get(0))?;
        if n == 0 {
            return Ok(Reply::Nil);
        }
        let k: Vec<u8> = c
            .prepare_cached(&format!(
                "SELECT key FROM bucketdb_kv WHERE {live} ORDER BY key LIMIT 1 OFFSET ?2"
            ))?
            .query_row(params![now, (seed % n as u64) as i64], |r| r.get(0))?;
        Ok(Reply::Bulk(k))
    })
}

/// FLUSHDB [ASYNC|SYNC]: every key, in one commit.
pub fn flush(s: &mut impl Store, args: &[Vec<u8>]) -> Result<Reply, Error> {
    match args {
        [] => {}
        [mode] if matches!(upper(mode).as_str(), "ASYNC" | "SYNC") => {}
        _ => return fail(SYNTAX),
    }
    s.write(&mut |c, _| {
        c.execute_batch("DELETE FROM bucketdb_kv; DELETE FROM bucketdb_kv_hash;")?;
        Ok(Reply::ok())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::tests::{bulk, cmd, mem};

    fn strings(r: &Reply) -> Vec<String> {
        match r {
            Reply::Array(items) => items
                .iter()
                .map(|i| match i {
                    Reply::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
                    other => format!("{other:?}"),
                })
                .collect(),
            other => vec![format!("{other:?}")],
        }
    }

    #[test]
    fn globs() {
        let yes = [
            ("*", ""),
            ("*", "abc"),
            ("a*", "abc"),
            ("*c", "abc"),
            ("a?c", "abc"),
            ("a*b*c", "aXbYc"),
            ("*:*:x", "a:b:c:x"),
            ("[abc]x", "bx"),
            ("[^abc]x", "dx"),
            ("[a-c]", "b"),
            ("[c-a]", "b"),
            ("h\\*", "h*"),
            ("[\\]]", "]"),
        ];
        for (p, s) in yes {
            assert!(glob(p.as_bytes(), s.as_bytes()), "{p} {s}");
        }
        let no = [
            ("a", ""),
            ("a*", "bac"),
            ("a?c", "ac"),
            ("[abc]x", "dx"),
            ("[^abc]x", "ax"),
            ("h\\*", "hx"),
            ("*:*:x", "a:b:c:y"),
            ("ab[", "ab"), // an unclosed class still wants a byte
        ];
        for (p, s) in no {
            assert!(!glob(p.as_bytes(), s.as_bytes()), "{p} {s}");
        }
        assert_eq!(literal_prefix(b"session:*"), b"session:");
        assert_eq!(after_prefix(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(after_prefix(b"a\xff"), Some(b"b".to_vec()));
        assert_eq!(after_prefix(b"\xff"), None);
    }

    #[test]
    fn scans() {
        let s = &mut mem();
        for i in 0..25 {
            cmd(s, &format!("SET k:{i:02} v"));
        }
        cmd(s, "SET other v");
        cmd(s, "SET k:gone v PX 1");
        s.now += 1;
        // Page by page, everything once.
        let mut seen = Vec::new();
        let mut cursor = "0".to_string();
        loop {
            let r = cmd(s, &format!("SCAN {cursor} COUNT 10"));
            let Reply::Array(parts) = r else { panic!() };
            cursor = strings(&Reply::Array(vec![parts[0].clone()]))[0].clone();
            seen.extend(strings(&parts[1]));
            if cursor == "0" {
                break;
            }
            // A key added and one deleted behind the cursor change nothing.
            cmd(s, "SET a:new v");
            cmd(s, "DEL k:00");
        }
        assert_eq!(seen.len(), 26);
        assert!(!seen.contains(&"k:gone".to_string()));
        // A prefix reads only its range; the cursor ends at 0 inside it.
        let r = cmd(s, "SCAN 0 MATCH k:1* COUNT 100");
        let Reply::Array(parts) = r else { panic!() };
        assert_eq!(parts[0], bulk("0"));
        assert_eq!(strings(&parts[1]).len(), 10);
        assert_eq!(strings(&cmd(s, "KEYS k:2?")), ["k:20", "k:21", "k:22", "k:23", "k:24"]);
        assert_eq!(strings(&cmd(s, "KEYS *ther")), ["other"]);
        assert_eq!(cmd(s, "SCAN 999"), Reply::err("ERR invalid cursor"));
        assert_eq!(cmd(s, "SCAN 0 COUNT 0"), Reply::err(SYNTAX));
        assert_eq!(cmd(s, "SCAN 0 TYPE nope"), Reply::err("ERR unknown type name 'nope'"));
        // k:01 to k:24, other, a:new.
        let Reply::Array(parts) = cmd(s, "SCAN 0 TYPE string COUNT 1000") else {
            panic!()
        };
        assert_eq!(strings(&parts[1]).len(), 26);
    }

    #[test]
    fn renames_and_flushes() {
        let s = &mut mem();
        cmd(s, "SET a 1 EX 100");
        cmd(s, "SET b 2");
        cmd(s, "SET gone x PX 1");
        assert_eq!(cmd(s, "RENAMENX a b"), Reply::Int(0));
        assert_eq!(cmd(s, "RENAME a c"), Reply::ok());
        assert_eq!(cmd(s, "GET c"), bulk("1"));
        assert_eq!(cmd(s, "TTL c"), Reply::Int(100)); // the expiry moves with it
        assert_eq!(cmd(s, "EXISTS a"), Reply::Int(0));
        assert_eq!(cmd(s, "RENAME c b"), Reply::ok());
        assert_eq!(cmd(s, "GET b"), bulk("1"));
        assert_eq!(cmd(s, "RENAME nope x"), Reply::err("ERR no such key"));
        s.now += 1;
        assert_eq!(cmd(s, "RENAME gone x"), Reply::err("ERR no such key"));
        // Over an expired row of the new name.
        assert_eq!(cmd(s, "RENAMENX b gone"), Reply::Int(1));
        assert_eq!(cmd(s, "GET gone"), bulk("1"));
        assert_eq!(cmd(s, "RANDOMKEY"), bulk("gone"));
        assert_eq!(cmd(s, "EXPIRETIME gone"), Reply::Int((s.now - 1 + 100_000) / 1000));
        assert_eq!(cmd(s, "PEXPIRETIME nope"), Reply::Int(-2));
        assert_eq!(cmd(s, "FLUSHDB ASYNC"), Reply::ok());
        assert_eq!(cmd(s, "DBSIZE"), Reply::Int(0));
        assert_eq!(cmd(s, "RANDOMKEY"), Reply::Nil);
    }

    #[test]
    fn sweeps() {
        let s = &mut mem();
        cmd(s, "SET a v PX 10");
        cmd(s, "SET b v");
        assert!(!expired(&s.conn, s.now).unwrap());
        s.now += 10;
        assert!(expired(&s.conn, s.now).unwrap());
        assert_eq!(sweep(&s.conn, s.now).unwrap(), 1);
        assert!(!expired(&s.conn, s.now).unwrap());
        let rows: i64 = s
            .conn
            .query_row("SELECT count(*) FROM bucketdb_kv", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1);
    }
}
