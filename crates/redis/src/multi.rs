//! MULTI, EXEC, DISCARD, WATCH: commands queued, then run together as one
//! transaction, one commit.
//!
//! WATCH keeps a fingerprint of each key (its type, value, expiry and a hash's
//! fields); EXEC compares them inside its own transaction, before running
//! anything, and answers a null array when one changed. Unlike Redis, a key
//! changed and then changed back reads as unchanged.

use crate::cmd::{self, Error, Store};
use crate::hash::{self, HASH};
use crate::resp::Reply;
use crate::server::{Cursors, load_cursor, save_cursor, unknown};
use rusqlite::Connection;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Mutex;

/// A queued command: its name, upper-cased, and its arguments.
pub type Queued = (String, Vec<Vec<u8>>);

/// A watched key: its database, the key, and its fingerprint then.
pub type Watched = (usize, Vec<u8>, u64);

#[derive(Debug, Default)]
pub struct State {
    /// Some while queueing, after MULTI.
    queue: Option<Vec<Queued>>,
    /// A command was refused while queueing: EXEC runs nothing.
    dirty: bool,
    pub watched: Vec<Watched>,
}

impl State {
    pub fn queueing(&self) -> bool {
        self.queue.is_some()
    }

    pub fn multi(&mut self) -> Reply {
        if self.queueing() {
            return Reply::err("ERR MULTI calls can not be nested");
        }
        self.queue = Some(Vec::new());
        self.dirty = false;
        Reply::ok()
    }

    /// Queues a command, or refuses it (and the transaction with it) when it is
    /// unknown or has a wrong number of arguments.
    pub fn queue(&mut self, name: &str, args: &[Vec<u8>]) -> Reply {
        let check = match name {
            "PING" if args.len() <= 1 => Some(Ok(())),
            "ECHO" if args.len() == 1 => Some(Ok(())),
            "PING" | "ECHO" => Some(Err(Reply::err(format!(
                "ERR wrong number of arguments for '{}' command",
                name.to_ascii_lowercase()
            )))),
            _ => cmd::check(name, args),
        };
        match check {
            Some(Ok(())) => {
                if let Some(q) = self.queue.as_mut() {
                    q.push((name.to_string(), args.to_vec()));
                }
                Reply::Simple("QUEUED".into())
            }
            Some(Err(e)) => {
                self.dirty = true;
                e
            }
            None => {
                self.dirty = true;
                unknown(name, args)
            }
        }
    }

    pub fn discard(&mut self) -> Reply {
        if self.queue.take().is_none() {
            return Reply::err("ERR DISCARD without MULTI");
        }
        self.dirty = false;
        self.watched.clear();
        Reply::ok()
    }

    pub fn unwatch(&mut self) -> Reply {
        self.watched.clear();
        Reply::ok()
    }

    /// For EXEC: the queue and the watched keys, the state left clear; or the
    /// reply when there is nothing to run.
    pub fn take(&mut self) -> Result<(Vec<Queued>, Vec<Watched>), Reply> {
        let Some(queue) = self.queue.take() else {
            return Err(Reply::err("ERR EXEC without MULTI"));
        };
        let watched = std::mem::take(&mut self.watched);
        if std::mem::take(&mut self.dirty) {
            return Err(Reply::err(
                "EXECABORT Transaction discarded because of previous errors.",
            ));
        }
        Ok((queue, watched))
    }
}

/// What a key holds, as a number: equal while it is unchanged.
///
/// # Errors
///
/// SQLite's.
pub fn fingerprint(c: &Connection, key: &[u8], now: i64) -> rusqlite::Result<u64> {
    let mut h = DefaultHasher::new();
    match cmd::entry(c, key, now)? {
        None => 0u8.hash(&mut h),
        Some(e) => {
            1u8.hash(&mut h);
            (e.kind, &e.value, e.expires_at).hash(&mut h);
            if e.kind == HASH {
                hash::fields(c, key, None, usize::MAX)?.hash(&mut h);
            }
        }
    }
    Ok(h.finish())
}

/// The queued commands run on c, the transaction EXEC opened. None when a
/// watched key changed. A command that fails leaves the others' writes, as in Redis.
///
/// # Errors
///
/// SQLite's, when the transaction itself breaks.
pub fn exec(
    c: &Connection,
    now: i64,
    queue: &[Queued],
    watched: &[Watched],
    db: usize,
    read_only: bool,
    cursors: &Mutex<Cursors>,
) -> Result<Option<Vec<Reply>>, Error> {
    for (_, key, fp) in watched.iter().filter(|w| w.0 == db) {
        if fingerprint(c, key, now)? != *fp {
            return Ok(None);
        }
    }
    let mut tx = Tx {
        c,
        now,
        read_only,
        cursors,
        db,
    };
    let mut out = Vec::with_capacity(queue.len());
    for (name, args) in queue {
        c.execute_batch("SAVEPOINT exec_command")?;
        let r = match (name.as_str(), args.first()) {
            ("PING", None) => Reply::Simple("PONG".into()),
            ("PING" | "ECHO", Some(msg)) => Reply::Bulk(msg.clone()),
            _ => cmd::run(&mut tx, name, args).unwrap_or_else(|| unknown(name, args)),
        };
        if matches!(r, Reply::Error(_)) {
            c.execute_batch("ROLLBACK TO exec_command")?;
        }
        c.execute_batch("RELEASE exec_command")?;
        out.push(r);
    }
    Ok(Some(out))
}

/// The commands of an EXEC, on its transaction: reads and writes both run on it.
struct Tx<'a> {
    c: &'a Connection,
    now: i64,
    read_only: bool,
    cursors: &'a Mutex<Cursors>,
    db: usize,
}

impl Store for Tx<'_> {
    fn read<T>(&mut self, f: &mut dyn FnMut(&Connection, i64) -> Result<T, Error>) -> Result<T, Error> {
        f(self.c, self.now)
    }

    fn write<T>(&mut self, f: &mut dyn FnMut(&Connection, i64) -> Result<T, Error>) -> Result<T, Error> {
        if self.read_only {
            return Err(Error::Reply(
                "READONLY You can't write against a read only replica.".into(),
            ));
        }
        f(self.c, self.now)
    }

    fn save_cursor(&mut self, last: Vec<u8>) -> u64 {
        save_cursor(self.cursors, self.db, last)
    }

    fn load_cursor(&mut self, cursor: u64) -> Option<Vec<u8>> {
        load_cursor(self.cursors, self.db, cursor)
    }
}
