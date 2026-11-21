//! Group commit: write commands from many connections, one commit.
//!
//! Each database has one writer connection. A write command takes it and runs in
//! a savepoint of its own inside the open transaction; then, if no other command
//! is queued for the connection, it commits: one segment and one manifest PUT for
//! all the commands in it. The commands that arrive while a commit is out to the
//! bucket queue for the connection, so the next commit takes them all: the busier
//! the database, the more each commit carries, at the latency of one commit.
//!
//! A command runs on its own connection's thread and hears back only once its
//! commit is in the bucket. When the commit loses to another sidecar, nothing of
//! it was answered, so every command in it runs again.

use crate::cmd::{Error, sql_error};
use rusqlite::Connection;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

/// How many times a write runs again after losing its commit to another sidecar.
pub const RETRIES: usize = 8;

/// The most commands one commit carries.
const MAX_COMMANDS: usize = 1024;

/// The writer of one database.
#[derive(Debug)]
pub struct Writer {
    group: Mutex<Group>,
    /// Commands waiting for the connection: while there are, the open
    /// transaction waits for them.
    queued: AtomicUsize,
    /// The most argument bytes one commit carries (a command larger than that
    /// commits alone), so commands together stay under BUCKETDB_MAX_TXN_MB.
    max_bytes: usize,
    commits: AtomicU64,
    writes: AtomicU64,
}

#[derive(Debug)]
struct Group {
    conn: Connection,
    open: Option<Open>,
}

/// The open transaction.
#[derive(Debug)]
struct Open {
    batch: Arc<Batch>,
    commands: usize,
    bytes: usize,
}

#[derive(Debug, Clone)]
enum Outcome {
    Committed,
    /// Lost to another sidecar, or undone: run again.
    Lost,
    Failed(String),
}

/// How a commit ended, for the commands in it.
#[derive(Debug, Default)]
struct Batch {
    outcome: Mutex<Option<Outcome>>,
    done: Condvar,
}

impl Batch {
    fn finish(&self, outcome: Outcome) {
        *lock(&self.outcome) = Some(outcome);
        self.done.notify_all();
    }

    fn wait(&self) -> Outcome {
        let mut o = lock(&self.outcome);
        loop {
            if let Some(outcome) = &*o {
                return outcome.clone();
            }
            o = self.done.wait(o).unwrap_or_else(PoisonError::into_inner);
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A commit that lost to another writer (SQLITE_BUSY_SNAPSHOT): run it again.
pub fn lost_race(e: &rusqlite::Error) -> bool {
    matches!(e, rusqlite::Error::SqliteFailure(f, _) if f.extended_code == 517)
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

impl Writer {
    pub fn new(conn: Connection, max_bytes: usize) -> Writer {
        Writer {
            group: Mutex::new(Group { conn, open: None }),
            queued: AtomicUsize::new(0),
            max_bytes: max_bytes.max(1),
            commits: AtomicU64::new(0),
            writes: AtomicU64::new(0),
        }
    }

    /// Commits so far, and the write commands they carried.
    pub fn stats(&self) -> (u64, u64) {
        (
            self.commits.load(Ordering::Relaxed),
            self.writes.load(Ordering::Relaxed),
        )
    }

    /// Runs f in the next commit and returns once that commit is in the bucket.
    /// bytes is the size of the command's arguments.
    ///
    /// # Errors
    ///
    /// What f returns, a commit that failed, or TRYAGAIN after RETRIES lost races.
    pub fn write<T>(&self, f: &mut dyn FnMut(&Connection, i64) -> Result<T, Error>, bytes: usize) -> Result<T, Error> {
        for _ in 0..RETRIES {
            let (batch, result) = self.join(f, bytes)?;
            match batch.wait() {
                Outcome::Committed => {
                    self.writes.fetch_add(1, Ordering::Relaxed);
                    return result;
                }
                Outcome::Lost => {}
                Outcome::Failed(msg) => return Err(Error::Reply(msg)),
            }
        }
        Err(Error::Reply(
            "TRYAGAIN another writer committed first; try again".into(),
        ))
    }

    /// Runs f in the open transaction (opening one), and commits it when nothing
    /// else is queued for it.
    fn join<T>(
        &self,
        f: &mut dyn FnMut(&Connection, i64) -> Result<T, Error>,
        bytes: usize,
    ) -> Result<(Arc<Batch>, Result<T, Error>), Error> {
        self.queued.fetch_add(1, Ordering::SeqCst);
        let mut g = lock(&self.group);
        self.queued.fetch_sub(1, Ordering::SeqCst);
        if g.open.as_ref().is_some_and(|o| o.bytes + bytes > self.max_bytes) {
            self.commit(&mut g);
        }
        if g.open.is_none() {
            // IMMEDIATE: the write lock first, so no other connection of this
            // process commits under it; only another sidecar can.
            g.conn.execute_batch("BEGIN IMMEDIATE")?;
            g.open = Some(Open {
                batch: Arc::new(Batch::default()),
                commands: 0,
                bytes: 0,
            });
        }
        let result = match g.apply(f) {
            Ok(r) => r,
            Err(e) => {
                // The transaction itself broke (SQLite may have rolled it back):
                // the commands before this one run again.
                g.end(Outcome::Lost);
                return Err(Error::Sql(e));
            }
        };
        let Some(open) = g.open.as_mut() else {
            return Err(Error::Reply("ERR no open transaction".into()));
        };
        open.commands += 1;
        open.bytes += bytes;
        let batch = open.batch.clone();
        let full = open.commands >= MAX_COMMANDS || open.bytes >= self.max_bytes;
        if full || self.queued.load(Ordering::SeqCst) == 0 {
            self.commit(&mut g);
        }
        Ok((batch, result))
    }

    fn commit(&self, g: &mut Group) {
        let outcome = match g.conn.execute_batch("COMMIT") {
            Ok(()) => Outcome::Committed,
            Err(e) if lost_race(&e) => Outcome::Lost,
            Err(e) => Outcome::Failed(sql_error(&e)),
        };
        if matches!(outcome, Outcome::Committed) {
            self.commits.fetch_add(1, Ordering::Relaxed);
        }
        g.end(outcome);
    }
}

impl Group {
    /// f in a savepoint: a command that fails leaves the others' writes as they were.
    fn apply<T>(
        &mut self,
        f: &mut dyn FnMut(&Connection, i64) -> Result<T, Error>,
    ) -> rusqlite::Result<Result<T, Error>> {
        self.conn.execute_batch("SAVEPOINT command")?;
        let r = f(&self.conn, now_ms());
        if r.is_err() {
            self.conn.execute_batch("ROLLBACK TO command")?;
        }
        self.conn.execute_batch("RELEASE command")?;
        Ok(r)
    }

    /// Ends the open transaction (rolled back unless committed) and tells its commands.
    fn end(&mut self, outcome: Outcome) {
        if !self.conn.is_autocommit() {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
        if let Some(open) = self.open.take() {
            open.batch.finish(outcome);
        }
    }
}
