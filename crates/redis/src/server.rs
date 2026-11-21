//! The server: a thread per connection, each with its own SQLite connections.

use crate::cmd::{self, Error, Store};
use crate::resp::{self, Limits, Reply};
use rusqlite::Connection;
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The version clients are told: the commands here are Redis 7.2's.
const REDIS_VERSION: &str = "7.2.0";

/// How many times a write runs again after losing its commit to another sidecar.
const RETRIES: usize = 8;

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
}

static ACTIVE: AtomicUsize = AtomicUsize::new(0);
static NEXT_ID: AtomicI64 = AtomicI64::new(1);

/// Holds one of max_connections while the session lives.
struct Slot;

impl Drop for Slot {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::Relaxed);
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
/// When accepting a connection fails.
pub fn serve(listener: TcpListener, cfg: Arc<Config>) -> io::Result<()> {
    for stream in listener.incoming() {
        let stream = stream?;
        let cfg = cfg.clone();
        std::thread::spawn(move || {
            let peer = stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();
            let _slot = Slot;
            let full = ACTIVE.fetch_add(1, Ordering::Relaxed) >= cfg.max_connections;
            if let Err(e) = Session::run(stream, &cfg, full)
                && e.kind() != io::ErrorKind::UnexpectedEof
            {
                log::debug!("bucketdb: redis connection {peer}: {e}");
            }
        });
    }
    Ok(())
}

fn open(path: &std::path::Path, read_only: bool) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::from_secs(30))?;
    conn.set_prepared_statement_cache_capacity(64);
    if read_only {
        // A version from before the table: an empty one of the connection's own.
        let has: bool = conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name = 'bucketdb_kv')",
            [],
            |r| r.get(0),
        )?;
        if !has {
            conn.execute_batch(cmd::TEMP_SCHEMA)?;
        }
        conn.execute_batch("PRAGMA query_only = ON;")?;
    }
    Ok(conn)
}

/// A commit that lost to another writer (SQLITE_BUSY_SNAPSHOT): run it again.
fn lost_race(e: &rusqlite::Error) -> bool {
    matches!(e, rusqlite::Error::SqliteFailure(f, _) if f.extended_code == 517)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

struct Session<'a> {
    cfg: &'a Config,
    id: i64,
    db: usize,
    conns: Vec<Option<Connection>>,
    authed: bool,
    name: Option<Vec<u8>>,
}

impl Session<'_> {
    fn run(stream: TcpStream, cfg: &Config, full: bool) -> io::Result<()> {
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
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            db: 0,
            conns: cfg.dbs.iter().map(|_| None).collect(),
            authed: cfg.password.is_none(),
            name: None,
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
        match name {
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
            // Clients (redis-cli) ask for the command table; an empty one is enough.
            "COMMAND" => match args.first().map(|a| String::from_utf8_lossy(a).to_ascii_uppercase()) {
                Some(sub) if sub == "COUNT" => Reply::Int(0),
                _ => Reply::Array(vec![]),
            },
            "INFO" => Reply::Bulk(
                format!(
                    "# Server\r\nredis_version:{REDIS_VERSION}\r\nredis_mode:standalone\r\n\
                     bucketdb_version:{}\r\n",
                    env!("CARGO_PKG_VERSION")
                )
                .into_bytes(),
            ),
            _ => cmd::run(self, name, args).unwrap_or_else(|| {
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
            }),
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

fn arity(name: &str) -> Reply {
    Reply::err(format!(
        "ERR wrong number of arguments for '{}' command",
        name.to_ascii_lowercase()
    ))
}

impl Store for Session<'_> {
    fn read<T>(&mut self, f: &mut dyn FnMut(&Connection, i64) -> Result<T, Error>) -> Result<T, Error> {
        let c = self.conn()?;
        // One snapshot for all the command reads.
        c.execute_batch("BEGIN")?;
        let r = f(c, now_ms());
        let _ = c.execute_batch(if r.is_ok() { "COMMIT" } else { "ROLLBACK" });
        r
    }

    fn write<T>(&mut self, f: &mut dyn FnMut(&Connection, i64) -> Result<T, Error>) -> Result<T, Error> {
        if self.cfg.read_only {
            return Err(Error::Reply(
                "READONLY You can't write against a read only replica.".into(),
            ));
        }
        let c = self.conn()?;
        for _ in 0..RETRIES {
            // IMMEDIATE: the write lock first, so no other connection of this
            // process commits under it; only another sidecar can.
            c.execute_batch("BEGIN IMMEDIATE")?;
            let r = f(c, now_ms()).and_then(|v| c.execute_batch("COMMIT").map(|()| v).map_err(Error::Sql));
            match r {
                Ok(v) => return Ok(v),
                Err(e) => {
                    // A failed COMMIT leaves the transaction open.
                    if !c.is_autocommit() {
                        let _ = c.execute_batch("ROLLBACK");
                    }
                    match e {
                        Error::Sql(e) if lost_race(&e) => {}
                        e => return Err(e),
                    }
                }
            }
        }
        Err(Error::Reply(
            "TRYAGAIN another writer committed first; try again".into(),
        ))
    }
}
