//! The data commands, over the `bucketdb_kv` table (and `bucketdb_kv_hash` for
//! the fields of hashes, in hash.rs).
//!
//! A key lives while `expires_at` (milliseconds since the epoch) is NULL or after
//! now: every read filters on it, so a key is gone the moment it expires, whether
//! or not its row is still there. Reads never delete; a write over an expired key
//! takes it as absent and replaces its row.

use crate::hash;
use crate::keys;
use crate::resp::Reply;
use rusqlite::{Connection, OptionalExtension, params};

/// The keys. WITHOUT ROWID: the key is the B-tree's own, so a write touches one leaf.
pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS bucketdb_kv (
  key BLOB PRIMARY KEY, type INTEGER NOT NULL, value BLOB NOT NULL, expires_at INTEGER
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS bucketdb_kv_expires ON bucketdb_kv (expires_at) WHERE expires_at IS NOT NULL;
CREATE TABLE IF NOT EXISTS bucketdb_kv_hash (
  key BLOB NOT NULL, field BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY (key, field)
) WITHOUT ROWID;";

/// The tables in the connection's temp schema, for a version of the past made
/// before there were: they read as empty. By name, each to make when missing.
pub const TEMP_TABLES: [(&str, &str); 2] = [
    (
        "bucketdb_kv",
        "CREATE TEMP TABLE bucketdb_kv (
           key BLOB PRIMARY KEY, type INTEGER NOT NULL, value BLOB NOT NULL, expires_at INTEGER
         ) WITHOUT ROWID;",
    ),
    (
        "bucketdb_kv_hash",
        "CREATE TEMP TABLE bucketdb_kv_hash (
           key BLOB NOT NULL, field BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY (key, field)
         ) WITHOUT ROWID;",
    ),
];

pub(crate) const STRING: i64 = 0;

pub(crate) const LIVE: &str = "(expires_at IS NULL OR expires_at > ?2)";

pub(crate) const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
pub(crate) const NOT_INT: &str = "ERR value is not an integer or out of range";
pub(crate) const SYNTAX: &str = "ERR syntax error";

/// A command's failure: a reply for the client, or SQLite's.
#[derive(Debug)]
pub enum Error {
    Reply(String),
    Sql(rusqlite::Error),
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Error {
        Error::Sql(e)
    }
}

pub(crate) fn fail<T>(msg: impl Into<String>) -> Result<T, Error> {
    Err(Error::Reply(msg.into()))
}

/// Where commands run. `now` is the clock in milliseconds since the epoch, read
/// once per attempt. A write may run its closure more than once (a lost commit
/// race runs it again), so it must not keep state between runs.
pub trait Store {
    /// Runs f in a read transaction.
    ///
    /// # Errors
    ///
    /// What f returns, or SQLite's errors.
    fn read<T>(&mut self, f: &mut dyn FnMut(&Connection, i64) -> Result<T, Error>) -> Result<T, Error>;

    /// Runs f in a write transaction and commits it.
    ///
    /// # Errors
    ///
    /// What f returns, a commit that failed, or a read-only database.
    fn write<T>(&mut self, f: &mut dyn FnMut(&Connection, i64) -> Result<T, Error>) -> Result<T, Error>;

    /// Keeps where a SCAN stopped (the last key it went past) and names it with
    /// a cursor, never 0: clients take cursors as numbers, keys are bytes.
    fn save_cursor(&mut self, last: Vec<u8>) -> u64;

    /// The key a cursor names, None when it names none (or no longer).
    fn load_cursor(&mut self, cursor: u64) -> Option<Vec<u8>>;
}

#[derive(Debug)]
pub(crate) struct Entry {
    pub kind: i64,
    pub value: Vec<u8>,
    pub expires_at: Option<i64>,
}

pub(crate) fn entry(c: &Connection, key: &[u8], now: i64) -> rusqlite::Result<Option<Entry>> {
    c.prepare_cached(&format!(
        "SELECT type, value, expires_at FROM bucketdb_kv WHERE key = ?1 AND {LIVE}"
    ))?
    .query_row(params![key, now], |r| {
        Ok(Entry {
            kind: r.get(0)?,
            value: r.get(1)?,
            expires_at: r.get(2)?,
        })
    })
    .optional()
}

/// The key's string, None when it does not exist; WRONGTYPE when it is not a string.
fn string(c: &Connection, key: &[u8], now: i64) -> Result<Option<Entry>, Error> {
    match entry(c, key, now)? {
        Some(e) if e.kind != STRING => fail(WRONGTYPE),
        e => Ok(e),
    }
}

/// Sets key to a string, over whatever it was (a hash's fields go).
fn put(c: &Connection, key: &[u8], value: &[u8], expires_at: Option<i64>) -> rusqlite::Result<()> {
    hash::drop_fields(c, key)?;
    c.prepare_cached(
        "INSERT INTO bucketdb_kv (key, type, value, expires_at) VALUES (?1, 0, ?2, ?3)
         ON CONFLICT (key) DO UPDATE SET type = 0, value = excluded.value, expires_at = excluded.expires_at",
    )?
    .execute(params![key, value, expires_at])?;
    Ok(())
}

/// Deletes the key if it lives; whether it did.
fn remove(c: &Connection, key: &[u8], now: i64) -> rusqlite::Result<bool> {
    let n = c
        .prepare_cached(&format!("DELETE FROM bucketdb_kv WHERE key = ?1 AND {LIVE}"))?
        .execute(params![key, now])?;
    if n > 0 {
        hash::drop_fields(c, key)?;
    }
    Ok(n > 0)
}

fn set_expiry(c: &Connection, key: &[u8], expires_at: Option<i64>) -> rusqlite::Result<()> {
    c.prepare_cached("UPDATE bucketdb_kv SET expires_at = ?2 WHERE key = ?1")?
        .execute(params![key, expires_at])?;
    Ok(())
}

/// An integer as Redis takes one: an optional minus, digits, no leading zeros.
pub(crate) fn int(arg: &[u8]) -> Option<i64> {
    let digits = arg.strip_prefix(b"-").unwrap_or(arg);
    if digits.is_empty()
        || digits.len() > 19
        || !digits.iter().all(u8::is_ascii_digit)
        || (digits[0] == b'0' && arg.len() > 1)
    {
        return None;
    }
    std::str::from_utf8(arg).ok()?.parse().ok()
}

/// A float as Redis takes one: finite, no spaces.
pub(crate) fn float(arg: &[u8]) -> Option<f64> {
    let f: f64 = std::str::from_utf8(arg).ok()?.parse().ok()?;
    f.is_finite().then_some(f)
}

/// A float as INCRBYFLOAT writes it: no exponent, no trailing zeros.
pub(crate) fn fmt_float(f: f64) -> String {
    format!("{f}")
}

fn int_arg(arg: &[u8]) -> Result<i64, Error> {
    int(arg).map_or_else(|| fail(NOT_INT), Ok)
}

/// When a key is to expire, as an option gives it.
#[derive(Debug, Clone, Copy)]
enum Expiry {
    /// Milliseconds from now.
    In(i64),
    /// Milliseconds since the epoch.
    At(i64),
}

impl Expiry {
    /// EX, PX, EXAT or PXAT with its argument. For SET and GETEX the time must be
    /// positive; EXPIRE takes any.
    fn parse(opt: &str, arg: &[u8], cmd: &str, positive: bool) -> Result<Expiry, Error> {
        let n = int_arg(arg)?;
        let invalid = || Error::Reply(format!("ERR invalid expire time in '{cmd}' command"));
        if positive && n <= 0 {
            return Err(invalid());
        }
        let ms = |n: i64| n.checked_mul(1000).ok_or_else(invalid);
        Ok(match opt {
            "EX" => Expiry::In(ms(n)?),
            "PX" => Expiry::In(n),
            "EXAT" => Expiry::At(ms(n)?),
            _ => Expiry::At(n),
        })
    }

    fn at(self, now: i64, cmd: &str) -> Result<i64, Error> {
        match self {
            Expiry::At(t) => Ok(t),
            Expiry::In(d) => now
                .checked_add(d)
                .map_or_else(|| fail(format!("ERR invalid expire time in '{cmd}' command")), Ok),
        }
    }
}

pub(crate) fn upper(arg: &[u8]) -> String {
    String::from_utf8_lossy(arg).to_ascii_uppercase()
}

fn arity(name: &str) -> Reply {
    Reply::err(format!(
        "ERR wrong number of arguments for '{}' command",
        name.to_ascii_lowercase()
    ))
}

/// Whether a command is known and has a right number of arguments: Ok, or the
/// error to answer; None for a name that is not a data command.
pub fn check(name: &str, args: &[Vec<u8>]) -> Option<Result<(), Reply>> {
    // The fewest arguments, and whether more are taken.
    let (min, more) = match name {
        "GET" | "GETDEL" | "TTL" | "PTTL" | "PERSIST" | "TYPE" | "STRLEN" | "INCR" | "DECR" => (1, false),
        "SET" | "GETEX" | "MGET" | "DEL" | "UNLINK" | "EXISTS" => (usize::from(name == "SET") + 1, true),
        "SETNX" | "INCRBY" | "DECRBY" | "INCRBYFLOAT" | "APPEND" => (2, false),
        "SETEX" | "PSETEX" => (3, false),
        "MSET" | "MSETNX" => (2, true),
        "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => (2, true),
        "DBSIZE" | "RANDOMKEY" => (0, false),
        "FLUSHDB" => (0, true),
        "KEYS" | "EXPIRETIME" | "PEXPIRETIME" | "HGETALL" | "HKEYS" | "HVALS" | "HLEN" => (1, false),
        "SCAN" => (1, true),
        "RENAME" | "RENAMENX" | "HGET" | "HEXISTS" | "HSTRLEN" => (2, false),
        "HMGET" | "HDEL" | "HSCAN" => (2, true),
        "HSET" | "HMSET" => (3, true),
        "HSETNX" | "HINCRBY" | "HINCRBYFLOAT" => (3, false),
        _ => return None,
    };
    let odd = name.starts_with("MSET") && args.len() % 2 == 1;
    Some(if args.len() < min || (!more && args.len() > min) || odd {
        Err(arity(name))
    } else {
        Ok(())
    })
}

/// Runs a data command (name upper-cased, args after it). None when the name is
/// not one.
pub fn run(s: &mut impl Store, name: &str, args: &[Vec<u8>]) -> Option<Reply> {
    if let Err(e) = check(name, args)? {
        return Some(e);
    }
    if let Some(r) = hash::run(s, name, args) {
        return Some(reply(r));
    }
    let result = match name {
        "GET" => get(s, &args[0]),
        "SET" => set(s, args),
        "SETNX" => setnx(s, &args[0], &args[1]),
        "SETEX" | "PSETEX" => setex(s, name, args),
        "GETDEL" => getdel(s, &args[0]),
        "GETEX" => getex(s, args),
        "MGET" => mget(s, args),
        "MSET" => mset(s, args),
        "MSETNX" => msetnx(s, args),
        "DEL" | "UNLINK" => del(s, args),
        "EXISTS" => exists(s, args),
        "TYPE" => kind(s, &args[0]),
        "STRLEN" => strlen(s, &args[0]),
        "APPEND" => append(s, &args[0], &args[1]),
        "INCR" => incr(s, &args[0], 1),
        "DECR" => incr(s, &args[0], -1),
        "INCRBY" => int_arg(&args[1]).and_then(|n| incr(s, &args[0], n)),
        "INCRBYFLOAT" => incr_float(s, &args[0], &args[1]),
        "DECRBY" => int_arg(&args[1]).and_then(|n| match n.checked_neg() {
            Some(n) => incr(s, &args[0], n),
            None => fail("ERR decrement would overflow"),
        }),
        "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => expire(s, name, args),
        "TTL" => ttl(s, &args[0], true),
        "PTTL" => ttl(s, &args[0], false),
        "PERSIST" => persist(s, &args[0]),
        "EXPIRETIME" => expiretime(s, &args[0], true),
        "PEXPIRETIME" => expiretime(s, &args[0], false),
        "SCAN" => keys::scan(s, args),
        "KEYS" => keys::keys(s, &args[0]),
        "RENAME" => keys::rename(s, &args[0], &args[1], false),
        "RENAMENX" => keys::rename(s, &args[0], &args[1], true),
        "RANDOMKEY" => keys::random(s),
        "FLUSHDB" => keys::flush(s, args),
        _ => dbsize(s),
    };
    Some(reply(result))
}

pub(crate) fn reply(result: Result<Reply, Error>) -> Reply {
    match result {
        Ok(r) => r,
        Err(Error::Reply(msg)) => Reply::Error(msg),
        Err(Error::Sql(e)) => Reply::Error(sql_error(&e)),
    }
}

/// SQLite's error as a reply.
pub fn sql_error(e: &rusqlite::Error) -> String {
    if let rusqlite::Error::SqliteFailure(f, _) = e {
        match f.extended_code {
            13 => return "ERR transaction too large: it writes more than BUCKETDB_MAX_TXN_MB".into(),
            8 => return "READONLY You can't write against a read only replica.".into(),
            5 | 6 | 517 => return "TRYAGAIN another writer committed first; try again".into(),
            _ => {}
        }
    }
    format!("ERR {e}")
}

fn get(s: &mut impl Store, key: &[u8]) -> Result<Reply, Error> {
    s.read(&mut |c, now| Ok(Reply::bulk_or_nil(string(c, key, now)?.map(|e| e.value))))
}

fn set(s: &mut impl Store, args: &[Vec<u8>]) -> Result<Reply, Error> {
    let (key, value) = (&args[0], &args[1]);
    let (mut nx, mut xx, mut get, mut keep) = (false, false, false, false);
    let mut expiry = None;
    let mut i = 2;
    while i < args.len() {
        let opt = upper(&args[i]);
        match opt.as_str() {
            "NX" if !xx => nx = true,
            "XX" if !nx => xx = true,
            "GET" => get = true,
            "KEEPTTL" if expiry.is_none() => keep = true,
            "EX" | "PX" | "EXAT" | "PXAT" if !keep && expiry.is_none() && i + 1 < args.len() => {
                i += 1;
                expiry = Some(Expiry::parse(&opt, &args[i], "set", true)?);
            }
            _ => return fail(SYNTAX),
        }
        i += 1;
    }
    s.write(&mut |c, now| {
        let old = if get { string(c, key, now)? } else { entry(c, key, now)? };
        let exists = old.is_some();
        let old_expiry = old.as_ref().and_then(|e| e.expires_at);
        let reply = if get {
            Reply::bulk_or_nil(old.map(|e| e.value))
        } else {
            Reply::ok()
        };
        if (nx && exists) || (xx && !exists) {
            return Ok(if get { reply } else { Reply::Nil });
        }
        let expires_at = match expiry {
            Some(x) => Some(x.at(now, "set")?),
            None if keep => old_expiry,
            None => None,
        };
        // Set already expired: as Redis, the key is gone.
        if expires_at.is_some_and(|t| t <= now) {
            remove(c, key, now)?;
        } else {
            put(c, key, value, expires_at)?;
        }
        Ok(reply)
    })
}

fn setnx(s: &mut impl Store, key: &[u8], value: &[u8]) -> Result<Reply, Error> {
    s.write(&mut |c, now| {
        if entry(c, key, now)?.is_some() {
            return Ok(Reply::Int(0));
        }
        put(c, key, value, None)?;
        Ok(Reply::Int(1))
    })
}

fn setex(s: &mut impl Store, name: &str, args: &[Vec<u8>]) -> Result<Reply, Error> {
    let opt = if name == "SETEX" { "EX" } else { "PX" };
    let cmd = name.to_ascii_lowercase();
    let expiry = Expiry::parse(opt, &args[1], &cmd, true)?;
    s.write(&mut |c, now| {
        put(c, &args[0], &args[2], Some(expiry.at(now, &cmd)?))?;
        Ok(Reply::ok())
    })
}

fn getdel(s: &mut impl Store, key: &[u8]) -> Result<Reply, Error> {
    s.write(&mut |c, now| {
        let old = string(c, key, now)?;
        if old.is_some() {
            remove(c, key, now)?;
        }
        Ok(Reply::bulk_or_nil(old.map(|e| e.value)))
    })
}

fn getex(s: &mut impl Store, args: &[Vec<u8>]) -> Result<Reply, Error> {
    let key = &args[0];
    // None: leave the expiry; Some(None): PERSIST.
    let change: Option<Option<Expiry>> = match args.len() {
        1 => None,
        2 if upper(&args[1]) == "PERSIST" => Some(None),
        3 => {
            let opt = upper(&args[1]);
            if !matches!(opt.as_str(), "EX" | "PX" | "EXAT" | "PXAT") {
                return fail(SYNTAX);
            }
            Some(Some(Expiry::parse(&opt, &args[2], "getex", true)?))
        }
        _ => return fail(SYNTAX),
    };
    let run = |c: &Connection, now: i64| -> Result<Reply, Error> {
        let Some(e) = string(c, key, now)? else {
            return Ok(Reply::Nil);
        };
        match change {
            Some(Some(x)) => {
                let t = x.at(now, "getex")?;
                if t <= now {
                    remove(c, key, now)?;
                } else {
                    set_expiry(c, key, Some(t))?;
                }
            }
            Some(None) if e.expires_at.is_some() => set_expiry(c, key, None)?,
            _ => {}
        }
        Ok(Reply::Bulk(e.value))
    };
    // Without an option it only reads.
    if change.is_none() {
        s.read(&mut |c, now| run(c, now))
    } else {
        s.write(&mut |c, now| run(c, now))
    }
}

fn mget(s: &mut impl Store, keys: &[Vec<u8>]) -> Result<Reply, Error> {
    s.read(&mut |c, now| {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            out.push(match entry(c, key, now)? {
                Some(e) if e.kind == STRING => Reply::Bulk(e.value),
                _ => Reply::Nil,
            });
        }
        Ok(Reply::Array(out))
    })
}

fn mset(s: &mut impl Store, args: &[Vec<u8>]) -> Result<Reply, Error> {
    s.write(&mut |c, _| {
        for kv in args.chunks(2) {
            put(c, &kv[0], &kv[1], None)?;
        }
        Ok(Reply::ok())
    })
}

fn msetnx(s: &mut impl Store, args: &[Vec<u8>]) -> Result<Reply, Error> {
    s.write(&mut |c, now| {
        for kv in args.chunks(2) {
            if entry(c, &kv[0], now)?.is_some() {
                return Ok(Reply::Int(0));
            }
        }
        for kv in args.chunks(2) {
            put(c, &kv[0], &kv[1], None)?;
        }
        Ok(Reply::Int(1))
    })
}

fn del(s: &mut impl Store, keys: &[Vec<u8>]) -> Result<Reply, Error> {
    s.write(&mut |c, now| {
        let mut n = 0;
        for key in keys {
            n += i64::from(remove(c, key, now)?);
        }
        Ok(Reply::Int(n))
    })
}

fn exists(s: &mut impl Store, keys: &[Vec<u8>]) -> Result<Reply, Error> {
    s.read(&mut |c, now| {
        let mut n = 0;
        for key in keys {
            n += i64::from(entry(c, key, now)?.is_some());
        }
        Ok(Reply::Int(n))
    })
}

fn kind(s: &mut impl Store, key: &[u8]) -> Result<Reply, Error> {
    s.read(&mut |c, now| {
        Ok(Reply::Simple(
            match entry(c, key, now)? {
                None => "none",
                Some(e) if e.kind == STRING => "string",
                Some(e) if e.kind == hash::HASH => "hash",
                Some(_) => "unknown",
            }
            .into(),
        ))
    })
}

fn strlen(s: &mut impl Store, key: &[u8]) -> Result<Reply, Error> {
    s.read(&mut |c, now| Ok(Reply::Int(string(c, key, now)?.map_or(0, |e| e.value.len() as i64))))
}

fn append(s: &mut impl Store, key: &[u8], tail: &[u8]) -> Result<Reply, Error> {
    s.write(&mut |c, now| {
        let (mut value, expires_at) = string(c, key, now)?.map_or((Vec::new(), None), |e| (e.value, e.expires_at));
        value.extend_from_slice(tail);
        put(c, key, &value, expires_at)?;
        Ok(Reply::Int(value.len() as i64))
    })
}

fn incr(s: &mut impl Store, key: &[u8], by: i64) -> Result<Reply, Error> {
    s.write(&mut |c, now| {
        let (n, expires_at) = match string(c, key, now)? {
            Some(e) => (int_arg(&e.value)?, e.expires_at),
            None => (0, None),
        };
        let Some(n) = n.checked_add(by) else {
            return fail("ERR increment or decrement would overflow");
        };
        put(c, key, n.to_string().as_bytes(), expires_at)?;
        Ok(Reply::Int(n))
    })
}

fn incr_float(s: &mut impl Store, key: &[u8], by: &[u8]) -> Result<Reply, Error> {
    let Some(by) = float(by) else {
        return fail("ERR value is not a valid float");
    };
    s.write(&mut |c, now| {
        let (n, expires_at) = match string(c, key, now)? {
            Some(e) => (
                float(&e.value).map_or_else(|| fail("ERR value is not a valid float"), Ok)?,
                e.expires_at,
            ),
            None => (0.0, None),
        };
        let n = n + by;
        if !n.is_finite() {
            return fail("ERR increment would produce NaN or Infinity");
        }
        let text = fmt_float(n);
        put(c, key, text.as_bytes(), expires_at)?;
        Ok(Reply::Bulk(text.into_bytes()))
    })
}

fn expire(s: &mut impl Store, name: &str, args: &[Vec<u8>]) -> Result<Reply, Error> {
    let cmd = name.to_ascii_lowercase();
    let opt = match name {
        "EXPIRE" => "EX",
        "PEXPIRE" => "PX",
        "EXPIREAT" => "EXAT",
        _ => "PXAT",
    };
    let key = &args[0];
    let expiry = Expiry::parse(opt, &args[1], &cmd, false)?;
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for a in &args[2..] {
        match upper(a).as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GT" => gt = true,
            "LT" => lt = true,
            other => return fail(format!("ERR Unsupported option {other}")),
        }
    }
    if nx && (xx || gt || lt) {
        return fail("ERR NX and XX, GT or LT options at the same time are not compatible");
    }
    if gt && lt {
        return fail("ERR GT and LT options at the same time are not compatible");
    }
    s.write(&mut |c, now| {
        let Some(e) = entry(c, key, now)? else {
            return Ok(Reply::Int(0));
        };
        let t = expiry.at(now, &cmd)?;
        // No expiry counts as forever: GT never beats it, LT always does.
        let skip = match e.expires_at {
            Some(cur) => nx || (gt && t <= cur) || (lt && t >= cur),
            None => xx || gt,
        };
        if skip {
            return Ok(Reply::Int(0));
        }
        if t <= now {
            remove(c, key, now)?;
        } else {
            set_expiry(c, key, Some(t))?;
        }
        Ok(Reply::Int(1))
    })
}

fn ttl(s: &mut impl Store, key: &[u8], seconds: bool) -> Result<Reply, Error> {
    s.read(&mut |c, now| {
        Ok(Reply::Int(match entry(c, key, now)? {
            None => -2,
            Some(Entry { expires_at: None, .. }) => -1,
            Some(Entry {
                expires_at: Some(t), ..
            }) if seconds => (t - now + 500) / 1000,
            Some(Entry {
                expires_at: Some(t), ..
            }) => t - now,
        }))
    })
}

fn persist(s: &mut impl Store, key: &[u8]) -> Result<Reply, Error> {
    s.write(&mut |c, now| match entry(c, key, now)? {
        Some(Entry {
            expires_at: Some(_), ..
        }) => {
            set_expiry(c, key, None)?;
            Ok(Reply::Int(1))
        }
        _ => Ok(Reply::Int(0)),
    })
}

fn expiretime(s: &mut impl Store, key: &[u8], seconds: bool) -> Result<Reply, Error> {
    s.read(&mut |c, now| {
        Ok(Reply::Int(match entry(c, key, now)? {
            None => -2,
            Some(Entry { expires_at: None, .. }) => -1,
            Some(Entry {
                expires_at: Some(t), ..
            }) if seconds => t / 1000,
            Some(Entry {
                expires_at: Some(t), ..
            }) => t,
        }))
    })
}

fn dbsize(s: &mut impl Store) -> Result<Reply, Error> {
    s.read(&mut |c, now| {
        let n = c
            .prepare_cached("SELECT count(*) FROM bucketdb_kv WHERE expires_at IS NULL OR expires_at > ?1")?
            .query_row([now], |r| r.get(0))?;
        Ok(Reply::Int(n))
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A plain SQLite connection and a clock the test moves.
    pub(crate) struct Mem {
        pub conn: Connection,
        pub now: i64,
        writes: usize,
        cursors: Vec<Vec<u8>>,
    }

    impl Store for Mem {
        fn read<T>(&mut self, f: &mut dyn FnMut(&Connection, i64) -> Result<T, Error>) -> Result<T, Error> {
            f(&self.conn, self.now)
        }

        fn write<T>(&mut self, f: &mut dyn FnMut(&Connection, i64) -> Result<T, Error>) -> Result<T, Error> {
            self.writes += 1;
            self.conn.execute_batch("BEGIN IMMEDIATE")?;
            let r = f(&self.conn, self.now);
            self.conn.execute_batch(if r.is_ok() { "COMMIT" } else { "ROLLBACK" })?;
            r
        }

        fn save_cursor(&mut self, last: Vec<u8>) -> u64 {
            self.cursors.push(last);
            self.cursors.len() as u64
        }

        fn load_cursor(&mut self, cursor: u64) -> Option<Vec<u8>> {
            self.cursors.get(usize::try_from(cursor).ok()?.checked_sub(1)?).cloned()
        }
    }

    pub(crate) fn mem() -> Mem {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        Mem {
            conn,
            now: 1_000_000,
            writes: 0,
            cursors: Vec::new(),
        }
    }

    pub(crate) fn cmd(s: &mut Mem, line: &str) -> Reply {
        let mut args: Vec<Vec<u8>> = line.split(' ').map(|a| a.as_bytes().to_vec()).collect();
        let name = upper(&args.remove(0));
        run(s, &name, &args).unwrap_or_else(|| panic!("unknown {name}"))
    }

    pub(crate) fn bulk(v: &str) -> Reply {
        Reply::Bulk(v.as_bytes().to_vec())
    }

    #[test]
    fn strings() {
        let s = &mut mem();
        assert_eq!(cmd(s, "GET k"), Reply::Nil);
        assert_eq!(cmd(s, "SET k v"), Reply::ok());
        assert_eq!(cmd(s, "GET k"), bulk("v"));
        assert_eq!(cmd(s, "SET k w NX"), Reply::Nil);
        assert_eq!(cmd(s, "SET k w XX GET"), bulk("v"));
        assert_eq!(cmd(s, "SET n 1 XX"), Reply::Nil);
        assert_eq!(cmd(s, "SET k v NX XX"), Reply::err(SYNTAX));
        assert_eq!(
            cmd(s, "SET k v EX 0"),
            Reply::err("ERR invalid expire time in 'set' command")
        );
        assert_eq!(cmd(s, "SET k v EX"), Reply::err(SYNTAX));
        assert_eq!(cmd(s, "SETNX k x"), Reply::Int(0));
        assert_eq!(cmd(s, "SETNX j x"), Reply::Int(1));
        assert_eq!(cmd(s, "MSET a 1 b 2"), Reply::ok());
        assert_eq!(cmd(s, "MSET a 1 b"), arity("MSET"));
        assert_eq!(
            cmd(s, "MGET a b zz"),
            Reply::Array(vec![bulk("1"), bulk("2"), Reply::Nil])
        );
        assert_eq!(cmd(s, "MSETNX a 3 c 4"), Reply::Int(0));
        assert_eq!(cmd(s, "GET c"), Reply::Nil);
        assert_eq!(cmd(s, "APPEND a 23"), Reply::Int(3));
        assert_eq!(cmd(s, "STRLEN a"), Reply::Int(3));
        assert_eq!(cmd(s, "EXISTS a a zz"), Reply::Int(2));
        assert_eq!(cmd(s, "TYPE a"), Reply::Simple("string".into()));
        assert_eq!(cmd(s, "TYPE zz"), Reply::Simple("none".into()));
        assert_eq!(cmd(s, "DEL a zz b"), Reply::Int(2));
        assert_eq!(cmd(s, "GETDEL k"), bulk("w"));
        assert_eq!(cmd(s, "GET k"), Reply::Nil);
        assert_eq!(cmd(s, "DBSIZE"), Reply::Int(1));
        assert_eq!(cmd(s, "GET"), arity("GET"));
    }

    #[test]
    fn counters() {
        let s = &mut mem();
        assert_eq!(cmd(s, "INCR n"), Reply::Int(1));
        assert_eq!(cmd(s, "INCRBY n 41"), Reply::Int(42));
        assert_eq!(cmd(s, "DECRBY n 50"), Reply::Int(-8));
        assert_eq!(cmd(s, "DECR n"), Reply::Int(-9));
        assert_eq!(cmd(s, "GET n"), bulk("-9"));
        assert_eq!(cmd(s, "INCRBY n 01"), Reply::err(NOT_INT));
        assert_eq!(cmd(s, "SET x abc"), Reply::ok());
        assert_eq!(cmd(s, "INCR x"), Reply::err(NOT_INT));
        assert_eq!(cmd(s, "SET m 9223372036854775807"), Reply::ok());
        assert_eq!(
            cmd(s, "INCR m"),
            Reply::err("ERR increment or decrement would overflow")
        );
        assert_eq!(
            cmd(s, "DECRBY m -9223372036854775808"),
            Reply::err("ERR decrement would overflow")
        );
        // A failed command writes nothing.
        assert_eq!(cmd(s, "GET m"), bulk("9223372036854775807"));
        assert_eq!(cmd(s, "INCRBYFLOAT f 10.5"), bulk("10.5"));
        assert_eq!(cmd(s, "INCRBYFLOAT f 0.1"), bulk("10.6"));
        assert_eq!(cmd(s, "INCRBYFLOAT f -5.6"), bulk("5"));
        assert_eq!(
            cmd(s, "INCRBYFLOAT f inf"),
            Reply::err("ERR value is not a valid float")
        );
        assert_eq!(cmd(s, "INCRBYFLOAT x 1"), Reply::err("ERR value is not a valid float"));
    }

    #[test]
    fn expiry() {
        let s = &mut mem();
        assert_eq!(cmd(s, "SET k v PX 100"), Reply::ok());
        assert_eq!(cmd(s, "PTTL k"), Reply::Int(100));
        assert_eq!(cmd(s, "TTL k"), Reply::Int(0));
        s.now += 99;
        assert_eq!(cmd(s, "GET k"), bulk("v"));
        s.now += 1;
        // Gone the moment it expires, its row still there.
        assert_eq!(cmd(s, "GET k"), Reply::Nil);
        assert_eq!(cmd(s, "EXISTS k"), Reply::Int(0));
        assert_eq!(cmd(s, "TTL k"), Reply::Int(-2));
        assert_eq!(cmd(s, "DBSIZE"), Reply::Int(0));
        // Over an expired key: as if absent.
        assert_eq!(cmd(s, "SET k w NX"), Reply::ok());
        assert_eq!(cmd(s, "TTL k"), Reply::Int(-1));
        assert_eq!(cmd(s, "SET k v PX 1000"), Reply::ok());
        s.now += 1000;
        assert_eq!(cmd(s, "INCR k"), Reply::Int(1));
        assert_eq!(cmd(s, "TTL k"), Reply::Int(-1));

        assert_eq!(cmd(s, "SET k v EX 10"), Reply::ok());
        assert_eq!(cmd(s, "INCR c"), Reply::Int(1));
        assert_eq!(cmd(s, "EXPIRE c 5"), Reply::Int(1));
        assert_eq!(cmd(s, "INCR c"), Reply::Int(2));
        assert_eq!(cmd(s, "TTL c"), Reply::Int(5)); // INCR keeps the TTL
        assert_eq!(cmd(s, "SET c 0 KEEPTTL"), Reply::ok());
        assert_eq!(cmd(s, "TTL c"), Reply::Int(5));
        assert_eq!(cmd(s, "SET c 0"), Reply::ok());
        assert_eq!(cmd(s, "TTL c"), Reply::Int(-1)); // SET drops it
        assert_eq!(cmd(s, "EXPIRE zz 5"), Reply::Int(0));

        assert_eq!(cmd(s, "EXPIRE k 20 NX"), Reply::Int(0));
        assert_eq!(cmd(s, "EXPIRE k 5 GT"), Reply::Int(0));
        assert_eq!(cmd(s, "EXPIRE k 20 GT"), Reply::Int(1));
        assert_eq!(cmd(s, "EXPIRE k 30 LT"), Reply::Int(0));
        assert_eq!(cmd(s, "TTL k"), Reply::Int(20));
        assert_eq!(cmd(s, "EXPIRE c 9 GT"), Reply::Int(0)); // no TTL is forever
        assert_eq!(cmd(s, "EXPIRE c 9 XX"), Reply::Int(0));
        assert_eq!(cmd(s, "EXPIRE c 9 LT"), Reply::Int(1));
        assert_eq!(
            cmd(s, "EXPIRE c 9 NX GT"),
            Reply::err("ERR NX and XX, GT or LT options at the same time are not compatible")
        );
        assert_eq!(cmd(s, "PERSIST c"), Reply::Int(1));
        assert_eq!(cmd(s, "PERSIST c"), Reply::Int(0));
        assert_eq!(cmd(s, "PEXPIREAT k 2000000"), Reply::Int(1));
        assert_eq!(cmd(s, "PTTL k"), Reply::Int(2_000_000 - s.now));
        // An expiry in the past deletes.
        assert_eq!(cmd(s, "EXPIRE k -1"), Reply::Int(1));
        assert_eq!(cmd(s, "EXISTS k"), Reply::Int(0));
        assert_eq!(cmd(s, "SET k v PXAT 5"), Reply::ok());
        assert_eq!(cmd(s, "EXISTS k"), Reply::Int(0));

        assert_eq!(cmd(s, "SETEX k 10 v"), Reply::ok());
        assert_eq!(cmd(s, "GETEX k PX 300"), bulk("v"));
        assert_eq!(cmd(s, "PTTL k"), Reply::Int(300));
        assert_eq!(cmd(s, "GETEX k PERSIST"), bulk("v"));
        assert_eq!(cmd(s, "TTL k"), Reply::Int(-1));
        assert_eq!(
            cmd(s, "PSETEX k 0 v"),
            Reply::err("ERR invalid expire time in 'psetex' command")
        );
        assert_eq!(
            cmd(s, "SET k v EX 9223372036854775807"),
            Reply::err("ERR invalid expire time in 'set' command")
        );
    }

    #[test]
    fn reads_do_not_write() {
        let s = &mut mem();
        cmd(s, "SET k v PX 10");
        let writes = s.writes;
        s.now += 10;
        for c in [
            "GET k", "MGET k", "EXISTS k", "TTL k", "STRLEN k", "TYPE k", "GETEX k", "DBSIZE",
        ] {
            cmd(s, c);
        }
        assert_eq!(s.writes, writes);
        // The expired row is still there, for a sweep to take.
        let rows: i64 = s
            .conn
            .query_row("SELECT count(*) FROM bucketdb_kv", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1);
    }

    #[test]
    fn integers() {
        assert_eq!(int(b"0"), Some(0));
        assert_eq!(int(b"-12"), Some(-12));
        assert_eq!(int(b"-9223372036854775808"), Some(i64::MIN));
        for bad in [
            &b""[..],
            b"-",
            b"+1",
            b"01",
            b"-0",
            b" 1",
            b"1.0",
            b"9223372036854775808",
        ] {
            assert_eq!(int(bad), None, "{bad:?}");
        }
    }
}
