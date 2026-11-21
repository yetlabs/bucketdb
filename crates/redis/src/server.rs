//! The server: a thread per connection, each with its own SQLite connections to
//! read, and one writer per database that all of them write through.

use crate::cmd::{self, Error, Store};
use crate::docs;
use crate::group::{RETRIES, Writer, lost_race, now_ms};
use crate::keys;
use crate::multi;
use crate::resp::{self, Limits, Reply};
use rusqlite::Connection;
use std::collections::{HashMap, VecDeque};
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

/// The version clients are told: the commands here are Redis 7.2's.
const REDIS_VERSION: &str = "7.2.0";

#[derive(Debug)]
pub struct Config {
    /// The databases, in SELECT's order: name and the path SQLite opens.
    pub dbs: Vec<(String, PathBuf)>,
    pub password: Option<String>,
    /// Connections served at once; one more is refused.
    pub max_connections: usize,
    /// A version of the past: a write is refused with READONLY.
    pub read_only: bool,
    /// Bytes one command's arguments may carry together.
    pub max_command_bytes: usize,
    /// The moment a version of the past is read at, in milliseconds since the
    /// epoch: keys expire as they had then. None: now.
    pub at: Option<i64>,
    /// How often the rows of expired keys are deleted. None: never.
    pub sweep: Option<Duration>,
}

/// SCAN cursors kept: past that many, the oldest go (a client that comes back
/// with one gets "invalid cursor").
const MAX_CURSORS: usize = 10_000;

/// Where SCANs stopped, by cursor: (database, last key). Any connection may go on
/// with a cursor another one got, as client pools do.
#[derive(Debug, Default)]
pub(crate) struct Cursors {
    next: u64,
    keys: HashMap<u64, (usize, Vec<u8>)>,
    order: VecDeque<u64>,
}

pub(crate) fn save_cursor(cursors: &Mutex<Cursors>, db: usize, last: Vec<u8>) -> u64 {
    let mut c = cursors.lock().unwrap_or_else(PoisonError::into_inner);
    c.next += 1;
    let id = c.next;
    c.keys.insert(id, (db, last));
    c.order.push_back(id);
    while c.order.len() > MAX_CURSORS {
        if let Some(old) = c.order.pop_front() {
            c.keys.remove(&old);
        }
    }
    id
}

pub(crate) fn load_cursor(cursors: &Mutex<Cursors>, db: usize, cursor: u64) -> Option<Vec<u8>> {
    let c = cursors.lock().unwrap_or_else(PoisonError::into_inner);
    c.keys.get(&cursor).filter(|(d, _)| *d == db).map(|(_, k)| k.clone())
}

/// What the connections share.
#[derive(Debug)]
struct Shared {
    writers: Vec<Writer>,
    cursors: Mutex<Cursors>,
    /// Connections open now.
    active: AtomicUsize,
}

static NEXT_ID: AtomicI64 = AtomicI64::new(1);

/// Holds one of max_connections while the session lives.
struct Slot<'a>(&'a AtomicUsize);

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Makes the keys' table in each database that has none, before the port is
/// announced. A version of the past is left as it is.
///
/// # Errors
///
/// When a database cannot be opened, or the table cannot be committed.
pub fn prepare(cfg: &Config) -> io::Result<()> {
    if cfg.read_only {
        return Ok(());
    }
    for (name, path) in &cfg.dbs {
        let conn = open(path, false).map_err(|e| io::Error::other(format!("{name}: {e}")))?;
        // Sidecars starting together race to make it: the losers find it made.
        let mut tries = 0;
        loop {
            match conn.execute_batch(&format!("BEGIN IMMEDIATE; {} COMMIT;", cmd::SCHEMA)) {
                Ok(()) => break,
                Err(e) if lost_race(&e) && tries < RETRIES => {
                    let _ = conn.execute_batch("ROLLBACK");
                    tries += 1;
                }
                Err(e) => {
                    let _ = conn.execute_batch("ROLLBACK");
                    return Err(io::Error::other(format!("{name}: the keys' table: {e}")));
                }
            }
        }
    }
    Ok(())
}

/// Accepts connections on listener, a thread each, until it fails.
///
/// # Errors
///
/// When a database cannot be opened, or accepting a connection fails.
pub fn serve(listener: TcpListener, cfg: Arc<Config>) -> io::Result<()> {
    let mut writers = Vec::new();
    if !cfg.read_only {
        for (name, path) in &cfg.dbs {
            let conn = open(path, false).map_err(|e| io::Error::other(format!("{name}: {e}")))?;
            // Half the transaction limit: room for the pages around the values.
            writers.push(Writer::new(conn, cfg.max_command_bytes / 2));
        }
    }
    let shared = Arc::new(Shared {
        writers,
        cursors: Mutex::new(Cursors::default()),
        active: AtomicUsize::new(0),
    });
    if let Some(every) = cfg.sweep {
        for (i, (name, path)) in cfg.dbs.iter().enumerate().take(shared.writers.len()) {
            let reader = open(path, true).map_err(|e| io::Error::other(format!("{name}: {e}")))?;
            let (shared, name) = (shared.clone(), name.clone());
            std::thread::spawn(move || sweeper(&shared.writers[i], &reader, &name, every));
        }
    }
    for stream in listener.incoming() {
        let stream = stream?;
        let cfg = cfg.clone();
        let shared = shared.clone();
        std::thread::spawn(move || {
            let peer = stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();
            let _slot = Slot(&shared.active);
            let full = shared.active.fetch_add(1, Ordering::Relaxed) >= cfg.max_connections;
            if let Err(e) = Session::run(stream, &cfg, &shared, full)
                && e.kind() != io::ErrorKind::UnexpectedEof
            {
                log::debug!("bucketdb: redis connection {peer}: {e}");
            }
        });
    }
    Ok(())
}

/// Deletes the rows of expired keys, every so often, through the writer: a read
/// finds whether there are any, so an idle database is never written. Every
/// sidecar sweeps; two at once lose a race and the loser finds nothing left.
fn sweeper(writer: &Writer, reader: &Connection, name: &str, every: Duration) {
    // Sidecars started together sweep apart.
    let jitter = now_ms().unsigned_abs() % every.as_millis().max(1) as u64;
    std::thread::sleep(Duration::from_millis(jitter));
    loop {
        loop {
            match keys::expired(reader, now_ms()) {
                Ok(true) => {}
                Ok(false) => break,
                Err(e) => {
                    log::error!("bucketdb: redis sweep {name}: {e}");
                    break;
                }
            }
            match writer.write(&mut |c, now| Ok(keys::sweep(c, now)?), 0) {
                Ok(n) => {
                    writer.add_expired(n as u64);
                    if n < keys::SWEEP_BATCH {
                        break;
                    }
                }
                Err(e) => {
                    log::error!("bucketdb: redis sweep {name}: {e:?}");
                    break;
                }
            }
        }
        std::thread::sleep(every);
    }
}

fn open(path: &std::path::Path, read_only: bool) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::from_secs(30))?;
    conn.set_prepared_statement_cache_capacity(64);
    if read_only {
        // A version from before the tables: empty ones of the connection's own.
        for (name, create) in cmd::TEMP_TABLES {
            let has: bool = conn.query_row(
                "SELECT EXISTS (SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name = ?1)",
                [name],
                |r| r.get(0),
            )?;
            if !has {
                conn.execute_batch(create)?;
            }
        }
        conn.execute_batch("PRAGMA query_only = ON;")?;
    }
    Ok(conn)
}

struct Session<'a> {
    cfg: &'a Config,
    writers: &'a [Writer],
    cursors: &'a Mutex<Cursors>,
    /// The size of the command being run, for its commit's share.
    bytes: usize,
    id: i64,
    db: usize,
    conns: Vec<Option<Connection>>,
    authed: bool,
    name: Option<Vec<u8>>,
    /// MULTI ... EXEC.
    tx: multi::State,
}

impl Session<'_> {
    fn run(stream: TcpStream, cfg: &Config, shared: &Shared, full: bool) -> io::Result<()> {
        let _ = stream.set_nodelay(true);
        let mut out = BufWriter::new(stream.try_clone()?);
        if full {
            Reply::err("ERR max number of clients reached").write(&mut out)?;
            return out.flush();
        }
        let mut input = BufReader::new(stream);
        let limits = Limits {
            max_args: 1 << 20,
            max_bytes: cfg.max_command_bytes,
        };
        let mut s = Session {
            cfg,
            writers: &shared.writers,
            cursors: &shared.cursors,
            bytes: 0,
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            db: 0,
            conns: cfg.dbs.iter().map(|_| None).collect(),
            authed: cfg.password.is_none(),
            name: None,
            tx: multi::State::default(),
        };
        loop {
            let args = match resp::read_command(&mut input, limits) {
                Ok(Some(args)) => args,
                Ok(None) => return out.flush(),
                Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                    Reply::err(format!("ERR Protocol error: {e}")).write(&mut out)?;
                    return out.flush();
                }
                Err(e) => return Err(e),
            };
            if args.is_empty() {
                continue;
            }
            let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
            s.bytes = args.iter().map(Vec::len).sum();
            let reply = s.dispatch(&name, &args[1..]);
            reply.write(&mut out)?;
            if name == "QUIT" {
                return out.flush();
            }
            // Pipelined commands are answered together.
            if input.buffer().is_empty() {
                out.flush()?;
            }
        }
    }

    fn dispatch(&mut self, name: &str, args: &[Vec<u8>]) -> Reply {
        if !self.authed && !matches!(name, "AUTH" | "HELLO" | "QUIT") {
            return Reply::err("NOAUTH Authentication required.");
        }
        if self.tx.queueing() && !matches!(name, "EXEC" | "DISCARD" | "MULTI" | "WATCH" | "QUIT") {
            return self.tx.queue(name, args);
        }
        match name {
            "MULTI" => self.tx.multi(),
            "DISCARD" => self.tx.discard(),
            "WATCH" if args.is_empty() => arity(name),
            "WATCH" => self.watch(args),
            "UNWATCH" => self.tx.unwatch(),
            "EXEC" => self.exec(),
            "PING" => match args {
                [] => Reply::Simple("PONG".into()),
                [msg] => Reply::Bulk(msg.clone()),
                _ => arity(name),
            },
            "ECHO" => match args {
                [msg] => Reply::Bulk(msg.clone()),
                _ => arity(name),
            },
            "QUIT" => Reply::ok(),
            "SELECT" => match args {
                [n] => match std::str::from_utf8(n).ok().and_then(|n| n.parse::<usize>().ok()) {
                    Some(n) if n < self.cfg.dbs.len() => {
                        self.db = n;
                        Reply::ok()
                    }
                    Some(_) => Reply::err("ERR DB index is out of range"),
                    None => Reply::err("ERR value is not an integer or out of range"),
                },
                _ => arity(name),
            },
            "AUTH" => match args {
                [pass] | [_, pass] => self.auth(pass),
                _ => arity(name),
            },
            "HELLO" => self.hello(args),
            "CLIENT" => self.client(args),
            // Clients ask for the command table — redis-cli its docs, for its help, hints and Tab: those it speaks.
            "COMMAND" => match args.first().map(|a| String::from_utf8_lossy(a).to_ascii_uppercase()) {
                Some(sub) if sub == "COUNT" => docs::count(),
                Some(sub) if sub == "DOCS" => docs::docs(&args[1..]),
                _ => Reply::Array(vec![]),
            },
            "INFO" => {
                let (commits, writes, expired) = self.writers.get(self.db).map_or((0, 0, 0), Writer::stats);
                Reply::Bulk(
                    format!(
                        "# Server\r\nredis_version:{REDIS_VERSION}\r\nredis_mode:standalone\r\n\
                         bucketdb_version:{}\r\n\r\n# Stats\r\nbucketdb_commits:{commits}\r\n\
                         bucketdb_writes:{writes}\r\nexpired_keys:{expired}\r\n",
                        env!("CARGO_PKG_VERSION")
                    )
                    .into_bytes(),
                )
            }
            _ => cmd::run(self, name, args).unwrap_or_else(|| unknown(name, args)),
        }
    }

    /// WATCH key...: what each key holds now, to compare at EXEC.
    fn watch(&mut self, keys: &[Vec<u8>]) -> Reply {
        if self.tx.queueing() {
            return Reply::err("ERR WATCH inside MULTI is not allowed");
        }
        for key in keys {
            match self.read(&mut |c, now| Ok(multi::fingerprint(c, key, now)?)) {
                Ok(fp) => self.tx.watched.push((self.db, key.clone(), fp)),
                Err(e) => return cmd::reply(Err(e)),
            }
        }
        Reply::ok()
    }

    /// EXEC: the queued commands in one transaction, one commit, unless a watched
    /// key changed (a null reply) or a command was refused when queued.
    fn exec(&mut self) -> Reply {
        let (queue, watched) = match self.tx.take() {
            Ok(t) => t,
            Err(e) => return e,
        };
        // Keys watched in another database: compared first, by themselves.
        let db = self.db;
        for (wdb, key, fp) in watched.iter().filter(|w| w.0 != db) {
            self.db = *wdb;
            let now = self.read(&mut |c, now| Ok(multi::fingerprint(c, key, now)?));
            self.db = db;
            if now.map_or(true, |n| n != *fp) {
                return Reply::NilArray;
            }
        }
        let (cursors, read_only) = (self.cursors, self.cfg.read_only);
        let bytes = queue.iter().map(|(_, a)| a.iter().map(Vec::len).sum::<usize>()).sum();
        let mut run = |c: &Connection, now: i64| multi::exec(c, now, &queue, &watched, db, read_only, cursors);
        let result = if read_only {
            self.read(&mut run)
        } else {
            match self.writers.get(db) {
                Some(w) => w.write(&mut run, bytes),
                None => Err(Error::Reply("ERR no writer for this database".into())),
            }
        };
        match result {
            Ok(Some(replies)) => Reply::Array(replies),
            Ok(None) => Reply::NilArray,
            Err(e) => cmd::reply(Err(e)),
        }
    }

    fn auth(&mut self, pass: &[u8]) -> Reply {
        match &self.cfg.password {
            None => Reply::err(
                "ERR AUTH <password> called without any password configured for the default user. \
                 Are you sure your configuration is correct?",
            ),
            Some(p) if p.as_bytes() == pass => {
                self.authed = true;
                Reply::ok()
            }
            Some(_) => Reply::err("WRONGPASS invalid username-password pair or user is disabled."),
        }
    }

    /// HELLO [protover [AUTH user pass] [SETNAME name]]: RESP2 only. Clients that
    /// ask for 3 take the error and go on in 2.
    fn hello(&mut self, args: &[Vec<u8>]) -> Reply {
        if let Some(v) = args.first() {
            match v.as_slice() {
                b"2" => {}
                b"3" => return Reply::err("NOPROTO sorry, this protocol version is not supported."),
                _ => return Reply::err("ERR Protocol version is not an integer or out of range"),
            }
        }
        let mut i = 1;
        while i < args.len() {
            match String::from_utf8_lossy(&args[i]).to_ascii_uppercase().as_str() {
                "AUTH" if i + 2 < args.len() => {
                    if let Reply::Error(e) = self.auth(&args[i + 2]) {
                        return Reply::Error(e);
                    }
                    i += 3;
                }
                "SETNAME" if i + 1 < args.len() => {
                    self.name = Some(args[i + 1].clone());
                    i += 2;
                }
                other => return Reply::err(format!("ERR Syntax error in HELLO option '{other}'")),
            }
        }
        if !self.authed {
            return Reply::err(
                "NOAUTH HELLO must be called with the client already authenticated, otherwise the \
                 HELLO <proto> AUTH <user> <pass> option can be used",
            );
        }
        let s = |v: &str| Reply::Bulk(v.as_bytes().to_vec());
        Reply::Array(vec![
            s("server"),
            s("redis"),
            s("version"),
            s(REDIS_VERSION),
            s("proto"),
            Reply::Int(2),
            s("id"),
            Reply::Int(self.id),
            s("mode"),
            s("standalone"),
            s("role"),
            s("master"),
            s("modules"),
            Reply::Array(vec![]),
        ])
    }

    fn client(&mut self, args: &[Vec<u8>]) -> Reply {
        let Some(sub) = args.first() else {
            return arity("CLIENT");
        };
        let sub = String::from_utf8_lossy(sub).to_ascii_uppercase();
        match (sub.as_str(), args.len()) {
            ("SETNAME", 2) => {
                self.name = Some(args[1].clone());
                Reply::ok()
            }
            ("GETNAME", 1) => Reply::bulk_or_nil(self.name.clone()),
            ("ID", 1) => Reply::Int(self.id),
            ("SETINFO", 3) => Reply::ok(),
            ("SETNAME" | "GETNAME" | "ID" | "SETINFO", _) => Reply::err(format!(
                "ERR wrong number of arguments for 'client|{}' command",
                sub.to_ascii_lowercase()
            )),
            _ => Reply::err(format!(
                "ERR unknown subcommand '{}'. Try CLIENT HELP.",
                sub.to_ascii_lowercase()
            )),
        }
    }

    fn conn(&mut self) -> Result<&Connection, Error> {
        let slot = &mut self.conns[self.db];
        if slot.is_none() {
            *slot = Some(open(&self.cfg.dbs[self.db].1, self.cfg.read_only)?);
        }
        slot.as_ref().ok_or_else(|| Error::Reply("ERR no database".into()))
    }
}

pub(crate) fn unknown(name: &str, args: &[Vec<u8>]) -> Reply {
    let mut shown = String::new();
    for a in args.iter().take(3) {
        shown.push('\'');
        shown.push_str(&String::from_utf8_lossy(a));
        shown.push_str("' ");
    }
    Reply::err(format!(
        "ERR unknown command '{}', with args beginning with: {shown}",
        name.to_ascii_lowercase()
    ))
}

fn arity(name: &str) -> Reply {
    Reply::err(format!(
        "ERR wrong number of arguments for '{}' command",
        name.to_ascii_lowercase()
    ))
}

impl Store for Session<'_> {
    fn read<T>(&mut self, f: &mut dyn FnMut(&Connection, i64) -> Result<T, Error>) -> Result<T, Error> {
        let now = self.cfg.at.unwrap_or_else(now_ms);
        let c = self.conn()?;
        // One snapshot for all the command reads.
        c.execute_batch("BEGIN")?;
        let r = f(c, now);
        let _ = c.execute_batch(if r.is_ok() { "COMMIT" } else { "ROLLBACK" });
        r
    }

    fn save_cursor(&mut self, last: Vec<u8>) -> u64 {
        save_cursor(self.cursors, self.db, last)
    }

    fn load_cursor(&mut self, cursor: u64) -> Option<Vec<u8>> {
        load_cursor(self.cursors, self.db, cursor)
    }

    fn write<T>(&mut self, f: &mut dyn FnMut(&Connection, i64) -> Result<T, Error>) -> Result<T, Error> {
        if self.cfg.read_only {
            return Err(Error::Reply(
                "READONLY You can't write against a read only replica.".into(),
            ));
        }
        let Some(w) = self.writers.get(self.db) else {
            return Err(Error::Reply("ERR no writer for this database".into()));
        };
        w.write(f, self.bytes)
    }
}
