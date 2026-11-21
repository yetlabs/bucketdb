//! Hashes: a key's fields live in `bucketdb_kv_hash`, its type and expiry in its
//! row of `bucketdb_kv`, so a hash expires, is renamed and deleted as any key.
//! A hash left with no fields is deleted, as Redis does.

use crate::cmd::{Error, NOT_INT, SYNTAX, Store, WRONGTYPE, entry, fail, float, fmt_float, int, upper};
use crate::keys;
use crate::resp::Reply;
use rusqlite::{Connection, OptionalExtension, params};

pub(crate) const HASH: i64 = 1;

/// Whether key is a live hash (false: there is no such key); WRONGTYPE when it
/// is another type.
fn live(c: &Connection, key: &[u8], now: i64) -> Result<bool, Error> {
    match entry(c, key, now)? {
        None => Ok(false),
        Some(e) if e.kind == HASH => Ok(true),
        Some(_) => fail(WRONGTYPE),
    }
}

/// For a write: makes key a hash when there is none, replacing the row (and the
/// fields) an expired key may have left.
fn create(c: &Connection, key: &[u8], now: i64) -> Result<(), Error> {
    if live(c, key, now)? {
        return Ok(());
    }
    drop_fields(c, key)?;
    c.prepare_cached(
        "INSERT INTO bucketdb_kv (key, type, value, expires_at) VALUES (?1, 1, x'', NULL)
         ON CONFLICT (key) DO UPDATE SET type = 1, value = x'', expires_at = NULL",
    )?
    .execute([key])?;
    Ok(())
}

/// Deletes the key's fields, if it had any.
pub(crate) fn drop_fields(c: &Connection, key: &[u8]) -> rusqlite::Result<()> {
    c.prepare_cached("DELETE FROM bucketdb_kv_hash WHERE key = ?1")?
        .execute([key])?;
    Ok(())
}

fn get(c: &Connection, key: &[u8], field: &[u8]) -> rusqlite::Result<Option<Vec<u8>>> {
    c.prepare_cached("SELECT value FROM bucketdb_kv_hash WHERE key = ?1 AND field = ?2")?
        .query_row([key, field], |r| r.get(0))
        .optional()
}

/// Sets a field; whether it is new.
fn put(c: &Connection, key: &[u8], field: &[u8], value: &[u8]) -> rusqlite::Result<bool> {
    let new = get(c, key, field)?.is_none();
    c.prepare_cached("INSERT OR REPLACE INTO bucketdb_kv_hash (key, field, value) VALUES (?1, ?2, ?3)")?
        .execute([key, field, value])?;
    Ok(new)
}

/// The fields after `after` (None: from the first), in order, at most limit.
pub(crate) fn fields(
    c: &Connection,
    key: &[u8],
    after: Option<&[u8]>,
    limit: usize,
) -> rusqlite::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    c.prepare_cached(
        "SELECT field, value FROM bucketdb_kv_hash WHERE key = ?1 AND (?2 IS NULL OR field > ?2)
         ORDER BY field LIMIT ?3",
    )?
    .query_map(params![key, after, limit], |r| Ok((r.get(0)?, r.get(1)?)))?
    .collect()
}

fn len(c: &Connection, key: &[u8]) -> rusqlite::Result<i64> {
    c.prepare_cached("SELECT count(*) FROM bucketdb_kv_hash WHERE key = ?1")?
        .query_row([key], |r| r.get(0))
}

/// Runs a hash command, None when the name is not one. Arity is checked already.
pub(crate) fn run(s: &mut impl Store, name: &str, args: &[Vec<u8>]) -> Option<Result<Reply, Error>> {
    if !name.starts_with('H') {
        return None;
    }
    let key = args.first()?;
    Some(match name {
        "HSET" | "HMSET" => {
            if args.len().is_multiple_of(2) {
                return Some(fail(format!(
                    "ERR wrong number of arguments for '{}' command",
                    name.to_ascii_lowercase()
                )));
            }
            let hmset = name == "HMSET";
            s.write(&mut |c, now| {
                create(c, key, now)?;
                let mut new = 0;
                for fv in args[1..].chunks(2) {
                    new += i64::from(put(c, key, &fv[0], &fv[1])?);
                }
                Ok(if hmset { Reply::ok() } else { Reply::Int(new) })
            })
        }
        "HSETNX" => s.write(&mut |c, now| {
            if live(c, key, now)? && get(c, key, &args[1])?.is_some() {
                return Ok(Reply::Int(0));
            }
            create(c, key, now)?;
            put(c, key, &args[1], &args[2])?;
            Ok(Reply::Int(1))
        }),
        "HGET" => s.read(&mut |c, now| {
            Ok(if live(c, key, now)? {
                Reply::bulk_or_nil(get(c, key, &args[1])?)
            } else {
                Reply::Nil
            })
        }),
        "HMGET" => s.read(&mut |c, now| {
            let alive = live(c, key, now)?;
            let mut out = Vec::with_capacity(args.len() - 1);
            for f in &args[1..] {
                out.push(if alive {
                    Reply::bulk_or_nil(get(c, key, f)?)
                } else {
                    Reply::Nil
                });
            }
            Ok(Reply::Array(out))
        }),
        "HDEL" => s.write(&mut |c, now| {
            if !live(c, key, now)? {
                return Ok(Reply::Int(0));
            }
            let mut n = 0;
            for f in &args[1..] {
                n += c
                    .prepare_cached("DELETE FROM bucketdb_kv_hash WHERE key = ?1 AND field = ?2")?
                    .execute([key, f])? as i64;
            }
            if len(c, key)? == 0 {
                c.prepare_cached("DELETE FROM bucketdb_kv WHERE key = ?1")?
                    .execute([key])?;
            }
            Ok(Reply::Int(n))
        }),
        "HGETALL" | "HKEYS" | "HVALS" => s.read(&mut |c, now| {
            if !live(c, key, now)? {
                return Ok(Reply::Array(vec![]));
            }
            let mut out = Vec::new();
            for (f, v) in fields(c, key, None, usize::MAX)? {
                if name != "HVALS" {
                    out.push(Reply::Bulk(f));
                }
                if name != "HKEYS" {
                    out.push(Reply::Bulk(v));
                }
            }
            Ok(Reply::Array(out))
        }),
        "HLEN" => s.read(&mut |c, now| Ok(Reply::Int(if live(c, key, now)? { len(c, key)? } else { 0 }))),
        "HEXISTS" => s.read(&mut |c, now| {
            Ok(Reply::Int(i64::from(
                live(c, key, now)? && get(c, key, &args[1])?.is_some(),
            )))
        }),
        "HSTRLEN" => s.read(&mut |c, now| {
            let v = if live(c, key, now)? {
                get(c, key, &args[1])?
            } else {
                None
            };
            Ok(Reply::Int(v.map_or(0, |v| v.len() as i64)))
        }),
        "HINCRBY" => {
            let Some(by) = int(&args[2]) else {
                return Some(fail(NOT_INT));
            };
            s.write(&mut |c, now| {
                create(c, key, now)?;
                let n = match get(c, key, &args[1])? {
                    Some(v) => int(&v).map_or_else(|| fail("ERR hash value is not an integer"), Ok)?,
                    None => 0,
                };
                let Some(n) = n.checked_add(by) else {
                    return fail("ERR increment or decrement would overflow");
                };
                put(c, key, &args[1], n.to_string().as_bytes())?;
                Ok(Reply::Int(n))
            })
        }
        "HINCRBYFLOAT" => {
            let Some(by) = float(&args[2]) else {
                return Some(fail("ERR value is not a valid float"));
            };
            s.write(&mut |c, now| {
                create(c, key, now)?;
                let n = match get(c, key, &args[1])? {
                    Some(v) => float(&v).map_or_else(|| fail("ERR hash value is not a float"), Ok)?,
                    None => 0.0,
                };
                let n = n + by;
                if !n.is_finite() {
                    return fail("ERR increment would produce NaN or Infinity");
                }
                let text = fmt_float(n);
                put(c, key, &args[1], text.as_bytes())?;
                Ok(Reply::Bulk(text.into_bytes()))
            })
        }
        "HSCAN" => scan(s, key, &args[1..]),
        _ => return None,
    })
}

/// HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
fn scan(s: &mut impl Store, key: &[u8], args: &[Vec<u8>]) -> Result<Reply, Error> {
    let cursor: u64 = match std::str::from_utf8(&args[0]).ok().and_then(|c| c.parse().ok()) {
        Some(c) => c,
        None => return fail("ERR invalid cursor"),
    };
    let (mut pattern, mut count, mut values) = (None, 10usize, true);
    let mut i = 1;
    while i < args.len() {
        match (upper(&args[i]).as_str(), args.get(i + 1)) {
            ("NOVALUES", _) => {
                values = false;
                i += 1;
                continue;
            }
            ("MATCH", Some(p)) => pattern = Some(p.clone()),
            ("COUNT", Some(n)) => match std::str::from_utf8(n).ok().and_then(|n| n.parse::<usize>().ok()) {
                Some(n) if n >= 1 => count = n,
                Some(_) => return fail(SYNTAX),
                None => return fail(NOT_INT),
            },
            _ => return fail(SYNTAX),
        }
        i += 2;
    }
    let after = if cursor == 0 {
        None
    } else {
        match s.load_cursor(cursor) {
            Some(last) => Some(last),
            None => return fail("ERR invalid cursor"),
        }
    };
    let rows = s.read(&mut |c, now| {
        Ok(if live(c, key, now)? {
            fields(c, key, after.as_deref(), count)?
        } else {
            vec![]
        })
    })?;
    let next = match rows.last() {
        Some((last, _)) if rows.len() == count => s.save_cursor(last.clone()),
        _ => 0,
    };
    let mut out = Vec::new();
    for (f, v) in rows {
        if pattern.as_deref().is_none_or(|p| keys::glob(p, &f)) {
            out.push(Reply::Bulk(f));
            if values {
                out.push(Reply::Bulk(v));
            }
        }
    }
    Ok(Reply::Array(vec![
        Reply::Bulk(next.to_string().into_bytes()),
        Reply::Array(out),
    ]))
}

#[cfg(test)]
mod tests {
    use crate::cmd::tests::{bulk, cmd, mem};
    use crate::resp::Reply;

    fn arr(items: &[&str]) -> Reply {
        Reply::Array(items.iter().map(|i| bulk(i)).collect())
    }

    #[test]
    fn hashes() {
        let s = &mut mem();
        assert_eq!(cmd(s, "HSET h a 1 b 2"), Reply::Int(2));
        assert_eq!(cmd(s, "HSET h b 3 c 4"), Reply::Int(1));
        assert_eq!(
            cmd(s, "HSET h a"),
            Reply::err("ERR wrong number of arguments for 'hset' command")
        );
        assert_eq!(cmd(s, "HGET h b"), bulk("3"));
        assert_eq!(cmd(s, "HGET h zz"), Reply::Nil);
        assert_eq!(
            cmd(s, "HMGET h a zz c"),
            Reply::Array(vec![bulk("1"), Reply::Nil, bulk("4")])
        );
        assert_eq!(cmd(s, "HGETALL h"), arr(&["a", "1", "b", "3", "c", "4"]));
        assert_eq!(cmd(s, "HKEYS h"), arr(&["a", "b", "c"]));
        assert_eq!(cmd(s, "HVALS h"), arr(&["1", "3", "4"]));
        assert_eq!(cmd(s, "HLEN h"), Reply::Int(3));
        assert_eq!(cmd(s, "HEXISTS h a"), Reply::Int(1));
        assert_eq!(cmd(s, "HSTRLEN h a"), Reply::Int(1));
        assert_eq!(cmd(s, "HSETNX h a 9"), Reply::Int(0));
        assert_eq!(cmd(s, "HSETNX h d 9"), Reply::Int(1));
        assert_eq!(cmd(s, "HINCRBY h a 41"), Reply::Int(42));
        assert_eq!(cmd(s, "HINCRBYFLOAT h f 2.5"), bulk("2.5"));
        assert_eq!(cmd(s, "HINCRBYFLOAT h f 0.5"), bulk("3"));
        assert_eq!(cmd(s, "TYPE h"), Reply::Simple("hash".into()));
        // The other type's commands refuse it, and the other way round.
        assert!(matches!(cmd(s, "GET h"), Reply::Error(e) if e.starts_with("WRONGTYPE")));
        cmd(s, "SET str v");
        assert!(matches!(cmd(s, "HGET str a"), Reply::Error(e) if e.starts_with("WRONGTYPE")));
        assert_eq!(cmd(s, "HDEL h a b c d f zz"), Reply::Int(5));
        // Empty: gone.
        assert_eq!(cmd(s, "EXISTS h"), Reply::Int(0));
        assert_eq!(cmd(s, "HGETALL h"), Reply::Array(vec![]));
    }

    #[test]
    fn a_hash_is_a_key() {
        let s = &mut mem();
        cmd(s, "HSET h a 1 b 2");
        assert_eq!(cmd(s, "EXPIRE h 10"), Reply::Int(1));
        assert_eq!(cmd(s, "HSET h c 3"), Reply::Int(1));
        assert_eq!(cmd(s, "TTL h"), Reply::Int(10)); // HSET keeps the TTL
        assert_eq!(cmd(s, "RENAME h g"), Reply::ok());
        assert_eq!(cmd(s, "HGETALL g"), arr(&["a", "1", "b", "2", "c", "3"]));
        assert_eq!(cmd(s, "TTL g"), Reply::Int(10));
        s.now += 10_000;
        assert_eq!(cmd(s, "HGETALL g"), Reply::Array(vec![]));
        // Over the expired hash, a new one: none of the old fields.
        assert_eq!(cmd(s, "HSET g z 1"), Reply::Int(1));
        assert_eq!(cmd(s, "HGETALL g"), arr(&["z", "1"]));
        // SET over a hash takes its fields away; DEL too.
        assert_eq!(cmd(s, "SET g v"), Reply::ok());
        assert_eq!(cmd(s, "HSET h2 a 1"), Reply::Int(1));
        assert_eq!(cmd(s, "DEL h2"), Reply::Int(1));
        let fields: i64 = s
            .conn
            .query_row("SELECT count(*) FROM bucketdb_kv_hash", [], |r| r.get(0))
            .unwrap();
        assert_eq!(fields, 0);
        // The sweep takes an expired hash's fields with its row.
        cmd(s, "HSET h3 a 1 b 2");
        cmd(s, "PEXPIRE h3 1");
        s.now += 1;
        assert_eq!(crate::keys::sweep(&s.conn, s.now).unwrap(), 1);
        let fields: i64 = s
            .conn
            .query_row("SELECT count(*) FROM bucketdb_kv_hash", [], |r| r.get(0))
            .unwrap();
        assert_eq!(fields, 0);
        cmd(s, "HSET h4 a 1");
        assert_eq!(cmd(s, "FLUSHDB"), Reply::ok());
        let fields: i64 = s
            .conn
            .query_row("SELECT count(*) FROM bucketdb_kv_hash", [], |r| r.get(0))
            .unwrap();
        assert_eq!(fields, 0);
    }

    #[test]
    fn hscan() {
        let s = &mut mem();
        for i in 0..15 {
            cmd(s, &format!("HSET h f{i:02} v{i}"));
        }
        let Reply::Array(first) = cmd(s, "HSCAN h 0 COUNT 10 NOVALUES") else {
            panic!()
        };
        let Reply::Bulk(cursor) = &first[0] else { panic!() };
        let cursor = String::from_utf8(cursor.clone()).unwrap();
        assert_ne!(cursor, "0");
        let Reply::Array(rest) = cmd(s, &format!("HSCAN h {cursor} MATCH f1*")) else {
            panic!()
        };
        assert_eq!(rest[0], bulk("0"));
        assert_eq!(
            rest[1],
            arr(&["f10", "v10", "f11", "v11", "f12", "v12", "f13", "v13", "f14", "v14"])
        );
        assert_eq!(
            cmd(s, "HSCAN nope 0"),
            Reply::Array(vec![bulk("0"), Reply::Array(vec![])])
        );
    }
}
