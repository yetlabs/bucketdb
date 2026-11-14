//! The Postgres wire protocol, version 3: startup (trust or a cleartext password),
//! the simple query protocol, and the extended one (Parse, Bind, Describe, Execute,
//! Sync, Close, Flush) that drivers such as pgx use, with binary parameters and
//! results for the common types.
//!
//! One SQLite connection per client connection, on the database's placeholder file,
//! which the VFS serves from the bucket.

use crate::sql::{self, Plan, Tag};
use crate::{funcs, types};
use rusqlite::{Connection, types::Value};
use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

#[derive(Debug)]
pub struct Config {
    /// Database name -> placeholder path.
    pub dbs: HashMap<String, PathBuf>,
    pub password: Option<String>,
    /// Connections served at once; one more is refused with 53300.
    pub max_connections: usize,
}

static NEXT_PID: AtomicU32 = AtomicU32::new(1);
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

/// Holds one of max_connections while the session lives.
struct Slot;

impl Drop for Slot {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::Relaxed);
    }
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
                log::debug!("bucketdb: connection {peer}: {e}");
            }
        });
    }
    Ok(())
}

/// A Postgres error: SQLSTATE and message.
#[derive(Debug)]
pub struct PgError {
    code: &'static str,
    msg: String,
}

impl PgError {
    fn new(code: &'static str, msg: impl Into<String>) -> PgError {
        PgError { code, msg: msg.into() }
    }
}

fn sqlite_error(e: &rusqlite::Error) -> PgError {
    let msg = e.to_string();
    if let rusqlite::Error::SqliteFailure(f, _) = e {
        let code = match f.extended_code {
            517 => "40001", // SQLITE_BUSY_SNAPSHOT: another writer committed first
            1555 | 2067 => "23505",
            1299 => "23502",
            787 => "23503",
            275 => "23514",
            5 | 6 => "55P03",
            13 => "54000", // SQLITE_FULL: the transaction passed BUCKETDB_MAX_TXN_MB
            _ => "",
        };
        if code == "40001" {
            return PgError::new(
                code,
                "could not serialize access: another writer committed first; run the transaction again",
            );
        }
        if code == "54000" {
            return PgError::new(
                code,
                "transaction too large: it writes more than BUCKETDB_MAX_TXN_MB; split it",
            );
        }
        if !code.is_empty() {
            return PgError::new(code, msg);
        }
    }
    let code = if msg.contains("no such table") {
        "42P01"
    } else if msg.contains("no such column") {
        "42703"
    } else if msg.contains("syntax error") {
        "42601"
    } else if msg.contains("already exists") {
        "42P07"
    } else {
        "XX000"
    };
    PgError::new(code, msg)
}

#[derive(Debug)]
struct Prepared {
    plan: Plan,
    param_oids: Vec<u32>,
    cols: Vec<(String, u32)>,
}

#[derive(Debug)]
struct Portal {
    prepared: Arc<Prepared>,
    params: Vec<Value>,
    formats: Vec<i16>,
    /// Rows still to send, after an Execute with a row limit, and the final tag.
    pending: Option<(std::collections::VecDeque<Vec<u8>>, String)>,
}

struct Session {
    conn: Connection,
    stmts: HashMap<String, Arc<Prepared>>,
    portals: HashMap<String, Portal>,
    settings: HashMap<String, String>,
    /// Inside BEGIN ... COMMIT/ROLLBACK.
    explicit: bool,
    /// An error happened in the explicit transaction: only its end is taken.
    failed: bool,
    decls: HashMap<String, Vec<(String, Option<String>)>>,
    decls_version: i64,
    /// When set, rows go here instead of to the client (a portal with a row limit),
    /// and the command tag to collected_tag.
    collect: Option<std::collections::VecDeque<Vec<u8>>>,
    collected_tag: String,
    out: BufWriter<TcpStream>,
}

// --- framing -----------------------------------------------------------------------

fn read_i32(r: &mut impl Read) -> io::Result<i32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(i32::from_be_bytes(b))
}

struct Body<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Body<'a> {
    fn bad() -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, "malformed message")
    }
    fn cstr(&mut self) -> io::Result<&'a str> {
        let end = self.b[self.i..].iter().position(|&c| c == 0).ok_or_else(Self::bad)?;
        let s = std::str::from_utf8(&self.b[self.i..self.i + end]).map_err(|_| Self::bad())?;
        self.i += end + 1;
        Ok(s)
    }
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let s = self.b.get(self.i..self.i + n).ok_or_else(Self::bad)?;
        self.i += n;
        Ok(s)
    }
    fn i16(&mut self) -> io::Result<i16> {
        let s = self.take(2)?;
        Ok(i16::from_be_bytes([s[0], s[1]]))
    }
    fn i32(&mut self) -> io::Result<i32> {
        let s = self.take(4)?;
        Ok(i32::from_be_bytes([s[0], s[1], s[2], s[3]]))
    }
    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
}

#[derive(Default)]
struct Msg(Vec<u8>);

impl Msg {
    fn str(mut self, s: &str) -> Msg {
        self.0.extend_from_slice(s.as_bytes());
        self.0.push(0);
        self
    }
    fn i16(mut self, n: i16) -> Msg {
        self.0.extend_from_slice(&n.to_be_bytes());
        self
    }
    fn i32(mut self, n: i32) -> Msg {
        self.0.extend_from_slice(&n.to_be_bytes());
        self
    }
    fn u8(mut self, n: u8) -> Msg {
        self.0.push(n);
        self
    }
}

fn send(out: &mut impl Write, tag: u8, body: &[u8]) -> io::Result<()> {
    out.write_all(&[tag])?;
    out.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
    out.write_all(body)
}

fn send_error(out: &mut impl Write, severity: &str, e: &PgError) -> io::Result<()> {
    let m = Msg::default()
        .u8(b'S')
        .str(severity)
        .u8(b'V')
        .str(severity)
        .u8(b'C')
        .str(e.code)
        .u8(b'M')
        .str(&e.msg)
        .u8(0);
    send(out, b'E', &m.0)
}

fn type_len(oid: u32) -> i16 {
    match oid {
        types::BOOL => 1,
        types::INT2 => 2,
        types::INT4 | types::FLOAT4 | types::DATE => 4,
        types::INT8 | types::FLOAT8 | types::TIMESTAMP | types::TIMESTAMPTZ => 8,
        types::UUID => 16,
        _ => -1, // text, bytea, numeric, json: variable
    }
}

fn format_of(formats: &[i16], i: usize) -> i16 {
    match formats.len() {
        0 => 0,
        1 => formats[0],
        _ => formats.get(i).copied().unwrap_or(0),
    }
}

fn row_description(cols: &[(String, u32)], formats: &[i16]) -> Vec<u8> {
    let mut m = Msg::default().i16(cols.len() as i16);
    for (i, (name, oid)) in cols.iter().enumerate() {
        m = m
            .str(name)
            .i32(0)
            .i16(0)
            .i32(*oid as i32)
            .i16(type_len(*oid))
            .i32(-1)
            .i16(format_of(formats, i));
    }
    m.0
}

// --- session -----------------------------------------------------------------------

impl Session {
    fn run(stream: TcpStream, cfg: &Config, full: bool) -> io::Result<()> {
        stream.set_nodelay(true)?;
        let mut input = BufReader::new(stream.try_clone()?);
        let mut out = BufWriter::with_capacity(64 << 10, stream);

        // Startup: refuse SSL and GSS, read the v3 startup packet.
        let params = loop {
            let len = read_i32(&mut input)?;
            if !(8..=10_000).contains(&len) {
                return Err(Body::bad());
            }
            let mut b = vec![0u8; len as usize - 4];
            input.read_exact(&mut b)?;
            let code = i32::from_be_bytes([b[0], b[1], b[2], b[3]]);
            match code {
                80_877_103 | 80_877_104 => {
                    out.write_all(b"N")?;
                    out.flush()?;
                }
                196_608 => {
                    let mut body = Body { b: &b, i: 4 };
                    let mut p = HashMap::new();
                    loop {
                        let k = body.cstr()?;
                        if k.is_empty() {
                            break;
                        }
                        p.insert(k.to_string(), body.cstr()?.to_string());
                    }
                    break p;
                }
                _ => return Ok(()), // cancel requests and others: nothing to do
            }
        };
        if full {
            log::warn!(
                "bucketdb: refused a connection past BUCKETDB_MAX_CONNECTIONS ({})",
                cfg.max_connections
            );
            send_error(
                &mut out,
                "FATAL",
                &PgError::new("53300", "sorry, too many clients already"),
            )?;
            return out.flush();
        }
        let user = params.get("user").cloned().unwrap_or_else(|| "postgres".into());
        let db = params.get("database").cloned().unwrap_or_else(|| user.clone());
        let Some(path) = cfg.dbs.get(&db) else {
            send_error(
                &mut out,
                "FATAL",
                &PgError::new("3D000", format!("database \"{db}\" does not exist")),
            )?;
            return out.flush();
        };

        if let Some(pw) = &cfg.password {
            send(&mut out, b'R', &Msg::default().i32(3).0)?;
            out.flush()?;
            let mut tag = [0u8; 1];
            input.read_exact(&mut tag)?;
            let len = read_i32(&mut input)?;
            let mut b = vec![0u8; (len.max(4) - 4) as usize];
            input.read_exact(&mut b)?;
            let given = Body { b: &b, i: 0 }.cstr().unwrap_or_default().to_string();
            if tag[0] != b'p' || given != *pw {
                send_error(
                    &mut out,
                    "FATAL",
                    &PgError::new("28P01", format!("password authentication failed for user \"{user}\"")),
                )?;
                return out.flush();
            }
        }

        let conn = match open(path, &db) {
            Ok(c) => c,
            Err(e) => {
                send_error(&mut out, "FATAL", &sqlite_error(&e))?;
                return out.flush();
            }
        };
        send(&mut out, b'R', &Msg::default().i32(0).0)?;
        for (k, v) in [
            ("server_version", "16.0"),
            ("server_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO, MDY"),
            ("IntervalStyle", "postgres"),
            ("integer_datetimes", "on"),
            ("standard_conforming_strings", "on"),
            ("TimeZone", "UTC"),
            ("is_superuser", "on"),
            (
                "application_name",
                params.get("application_name").map_or("", String::as_str),
            ),
            ("session_authorization", &user),
        ] {
            send(&mut out, b'S', &Msg::default().str(k).str(v).0)?;
        }
        let pid = NEXT_PID.fetch_add(1, Ordering::Relaxed);
        send(&mut out, b'K', &Msg::default().i32(pid as i32).i32(0x5a45_524f).0)?;
        let mut s = Session {
            conn,
            stmts: HashMap::new(),
            portals: HashMap::new(),
            settings: HashMap::new(),
            explicit: false,
            failed: false,
            decls: HashMap::new(),
            decls_version: -1,
            collect: None,
            collected_tag: String::new(),
            out,
        };
        s.ready()?;
        s.out.flush()?;
        s.messages(&mut input)
    }

    fn ready(&mut self) -> io::Result<()> {
        let status = if !self.explicit {
            b'I'
        } else if self.failed {
            b'E'
        } else {
            b'T'
        };
        send(&mut self.out, b'Z', &[status])
    }

    fn messages(&mut self, input: &mut impl Read) -> io::Result<()> {
        // After an error in the extended protocol, messages are skipped until Sync.
        let mut skipping = false;
        loop {
            let mut tag = [0u8; 1];
            input.read_exact(&mut tag)?;
            let len = read_i32(input)?;
            if len < 4 {
                return Err(Body::bad());
            }
            let mut b = vec![0u8; len as usize - 4];
            input.read_exact(&mut b)?;
            let mut body = Body { b: &b, i: 0 };
            match tag[0] {
                b'Q' => {
                    let q = body.cstr()?.to_string();
                    self.simple(&q)?;
                    self.ready()?;
                    self.out.flush()?;
                }
                b'S' => {
                    skipping = false;
                    self.ready()?;
                    self.out.flush()?;
                }
                b'H' => self.out.flush()?,
                b'X' => return Ok(()),
                _ if skipping => {}
                b'P' | b'B' | b'D' | b'E' | b'C' => {
                    if let Err(e) = self.extended(tag[0], &mut body) {
                        self.error(&e)?;
                        skipping = true;
                    }
                }
                other => {
                    self.error(&PgError::new(
                        "0A000",
                        format!("message {:?} is not supported", other as char),
                    ))?;
                    skipping = true;
                }
            }
        }
    }

    fn error(&mut self, e: &PgError) -> io::Result<()> {
        if self.explicit {
            self.failed = true;
        }
        send_error(&mut self.out, "ERROR", e)
    }

    fn simple(&mut self, q: &str) -> io::Result<()> {
        let plans = match sql::parse(q) {
            Ok(p) => p,
            Err(e) => match raw_plan(q) {
                Some(p) => {
                    log::debug!("bucketdb: running as SQLite SQL ({e}): {q}");
                    vec![p]
                }
                None => return self.error(&PgError::new("42601", e)),
            },
        };
        for plan in plans {
            let prepared = match self.prepare(plan, &[]) {
                Ok(p) => p,
                Err(e) => return self.error(&e),
            };
            if let Err(e) = self.exec(&prepared, &[], &[], true) {
                return self.error(&e);
            }
        }
        Ok(())
    }

    fn extended(&mut self, tag: u8, body: &mut Body<'_>) -> Result<(), PgError> {
        let io = |e: io::Error| PgError::new("08P01", e.to_string());
        match tag {
            b'P' => {
                let name = body.cstr().map_err(io)?.to_string();
                let q = body.cstr().map_err(io)?.to_string();
                let n = body.i16().map_err(io)?;
                let mut oids = Vec::new();
                for _ in 0..n {
                    oids.push(body.i32().map_err(io)? as u32);
                }
                let mut plans = match sql::parse(&q) {
                    Ok(p) => p,
                    Err(e) => vec![raw_plan(&q).ok_or_else(|| PgError::new("42601", e))?],
                };
                if plans.len() > 1 {
                    return Err(PgError::new(
                        "42601",
                        "cannot insert multiple commands into a prepared statement",
                    ));
                }
                let prepared = self.prepare(plans.remove(0), &oids)?;
                self.stmts.insert(name, prepared);
                send(&mut self.out, b'1', &[]).map_err(io)
            }
            b'B' => {
                let portal = body.cstr().map_err(io)?.to_string();
                let stmt = body.cstr().map_err(io)?.to_string();
                let prepared =
                    self.stmts.get(&stmt).cloned().ok_or_else(|| {
                        PgError::new("26000", format!("prepared statement \"{stmt}\" does not exist"))
                    })?;
                let nf = body.i16().map_err(io)?;
                let mut pf = Vec::new();
                for _ in 0..nf {
                    pf.push(body.i16().map_err(io)?);
                }
                let np = body.i16().map_err(io)? as usize;
                let mut params = Vec::with_capacity(np);
                for i in 0..np {
                    let len = body.i32().map_err(io)?;
                    let raw = if len < 0 {
                        None
                    } else {
                        Some(body.take(len as usize).map_err(io)?)
                    };
                    let oid = prepared.param_oids.get(i).copied().unwrap_or(types::TEXT);
                    let v = types::param(raw, oid, format_of(&pf, i) == 1)
                        .map_err(|m| PgError::new("22P03", format!("parameter ${}: {m}", i + 1)))?;
                    params.push(v);
                }
                let nr = body.i16().map_err(io)?;
                let mut formats = Vec::new();
                for _ in 0..nr {
                    formats.push(body.i16().map_err(io)?);
                }
                self.portals.insert(
                    portal,
                    Portal {
                        prepared,
                        params,
                        formats,
                        pending: None,
                    },
                );
                send(&mut self.out, b'2', &[]).map_err(io)
            }
            b'D' => {
                let kind = body.u8().map_err(io)?;
                let name = body.cstr().map_err(io)?;
                if kind == b'S' {
                    let p = self.stmts.get(name).cloned().ok_or_else(|| {
                        PgError::new("26000", format!("prepared statement \"{name}\" does not exist"))
                    })?;
                    let mut m = Msg::default().i16(p.param_oids.len() as i16);
                    for oid in &p.param_oids {
                        m = m.i32(*oid as i32);
                    }
                    send(&mut self.out, b't', &m.0).map_err(io)?;
                    self.describe_rows(&p.cols, &[]).map_err(io)
                } else {
                    let p = self
                        .portals
                        .get(name)
                        .ok_or_else(|| PgError::new("34000", format!("portal \"{name}\" does not exist")))?;
                    let (cols, formats) = (p.prepared.cols.clone(), p.formats.clone());
                    self.describe_rows(&cols, &formats).map_err(io)
                }
            }
            b'E' => {
                let name = body.cstr().map_err(io)?.to_string();
                let max_rows = body.i32().map_err(io)?;
                let p = self
                    .portals
                    .get_mut(&name)
                    .ok_or_else(|| PgError::new("34000", format!("portal \"{name}\" does not exist")))?;
                let (prepared, params, formats) = (p.prepared.clone(), p.params.clone(), p.formats.clone());
                let returns_rows = !prepared.cols.is_empty() && matches!(prepared.plan, Plan::Sql { .. });
                if p.pending.is_none() && (max_rows <= 0 || !returns_rows) {
                    return self.exec(&prepared, &params, &formats, false);
                }
                if p.pending.is_none() {
                    // A row limit: run it whole, then hand the rows out in parts.
                    self.collect = Some(std::collections::VecDeque::new());
                    let r = self.exec(&prepared, &params, &formats, false);
                    let rows = self.collect.take().unwrap_or_default();
                    r?;
                    let tag = std::mem::take(&mut self.collected_tag);
                    if let Some(p) = self.portals.get_mut(&name) {
                        p.pending = Some((rows, tag));
                    }
                }
                let Some(p) = self.portals.get_mut(&name) else {
                    return Ok(());
                };
                let Some((rows, tag)) = p.pending.as_mut() else {
                    return Ok(());
                };
                let n = if max_rows <= 0 {
                    rows.len()
                } else {
                    (max_rows as usize).min(rows.len())
                };
                let part: Vec<Vec<u8>> = rows.drain(..n).collect();
                let done = rows.is_empty();
                let tag = tag.clone();
                if done {
                    p.pending = None;
                }
                for row in part {
                    send(&mut self.out, b'D', &row).map_err(io)?;
                }
                if done {
                    self.command(&tag)
                } else {
                    send(&mut self.out, b's', &[]).map_err(io)
                }
            }
            b'C' => {
                let kind = body.u8().map_err(io)?;
                let name = body.cstr().map_err(io)?;
                if kind == b'S' {
                    self.stmts.remove(name);
                } else {
                    self.portals.remove(name);
                }
                send(&mut self.out, b'3', &[]).map_err(io)
            }
            _ => Ok(()),
        }
    }

    fn describe_rows(&mut self, cols: &[(String, u32)], formats: &[i16]) -> io::Result<()> {
        if cols.is_empty() {
            send(&mut self.out, b'n', &[])
        } else {
            send(&mut self.out, b'T', &row_description(cols, formats))
        }
    }

    /// Checks a plan against the database: its columns, its parameter types.
    fn prepare(&mut self, plan: Plan, given: &[u32]) -> Result<Arc<Prepared>, PgError> {
        let (cols, param_oids) = match &plan {
            Plan::Sql {
                sql,
                params,
                param_columns,
                ..
            } => {
                let stmt = self.conn.prepare_cached(sql).map_err(|e| sqlite_error(&e))?;
                let cols = stmt
                    .columns()
                    .iter()
                    .map(|c| {
                        let oid = match c.decl_type() {
                            None if types::catalog_bool(c.name()) => types::BOOL,
                            decl => types::oid_of_decl(decl),
                        };
                        (c.name().to_string(), oid)
                    })
                    .collect();
                drop(stmt);
                let mut oids = Vec::with_capacity(*params);
                for (i, column) in param_columns.iter().enumerate().take(*params) {
                    let oid = match given.get(i) {
                        Some(&o) if o != 0 => o,
                        _ => match column {
                            Some(col) if col == sql::INTEGER_PARAM => types::INT8,
                            Some(col) => self.decl_oid(sql, col),
                            None => types::TEXT,
                        },
                    };
                    oids.push(oid);
                }
                (cols, oids)
            }
            Plan::Show { name } => (vec![(name.clone(), types::TEXT)], vec![]),
            _ => (vec![], vec![]),
        };
        Ok(Arc::new(Prepared { plan, param_oids, cols }))
    }

    /// The OID of column col in one of the tables sql names.
    fn decl_oid(&mut self, sql: &str, col: &str) -> u32 {
        let version: i64 = self
            .conn
            .query_row("PRAGMA schema_version", [], |r| r.get(0))
            .unwrap_or(0);
        if version != self.decls_version {
            self.decls.clear();
            self.decls_version = version;
        }
        for table in sql::tables(sql) {
            if !self.decls.contains_key(&table) {
                let cols = self
                    .conn
                    .prepare(&format!(
                        "SELECT name, type FROM pragma_table_info('{}')",
                        table.replace('\'', "''")
                    ))
                    .and_then(|mut s| {
                        s.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)))?
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .unwrap_or_default();
                self.decls.insert(table.clone(), cols);
            }
            if let Some((_, decl)) = self.decls[&table].iter().find(|(n, _)| n.eq_ignore_ascii_case(col)) {
                return types::oid_of_decl(decl.as_deref());
            }
        }
        types::TEXT
    }

    fn command(&mut self, tag: &str) -> Result<(), PgError> {
        send(&mut self.out, b'C', &Msg::default().str(tag).0).map_err(|e| PgError::new("08P01", e.to_string()))
    }

    fn exec(&mut self, p: &Prepared, params: &[Value], formats: &[i16], describe: bool) -> Result<(), PgError> {
        let to_savepoint = matches!(
            p.plan,
            Plan::Sql {
                tag: Tag::Other("ROLLBACK"),
                ..
            }
        );
        if self.failed && !matches!(p.plan, Plan::Commit | Plan::Rollback) && !to_savepoint {
            return Err(PgError::new(
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block",
            ));
        }
        let result = self.exec_inner(p, params, formats, describe);
        if result.is_ok() && to_savepoint {
            self.failed = false; // ROLLBACK TO SAVEPOINT ends the aborted state
        }
        // An error outside BEGIN leaves no transaction behind (SQLite may keep one open
        // after a failed autocommit commit).
        if result.is_err() && !self.explicit && !self.conn.is_autocommit() {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
        result
    }

    fn exec_inner(&mut self, p: &Prepared, params: &[Value], formats: &[i16], describe: bool) -> Result<(), PgError> {
        let io = |e: io::Error| PgError::new("08P01", e.to_string());
        match &p.plan {
            Plan::Empty => send(&mut self.out, b'I', &[]).map_err(io),
            Plan::Begin => {
                if !self.explicit {
                    self.conn.execute_batch("BEGIN").map_err(|e| sqlite_error(&e))?;
                    self.explicit = true;
                }
                self.command("BEGIN")
            }
            Plan::Commit => {
                if !self.explicit {
                    return self.command("COMMIT");
                }
                self.explicit = false;
                if self.failed {
                    self.failed = false;
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return self.command("ROLLBACK");
                }
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    // A lost commit race leaves SQLite's transaction open: end it.
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(sqlite_error(&e));
                }
                self.command("COMMIT")
            }
            Plan::Rollback => {
                if self.explicit && !self.conn.is_autocommit() {
                    let _ = self.conn.execute_batch("ROLLBACK");
                }
                self.explicit = false;
                self.failed = false;
                self.command("ROLLBACK")
            }
            Plan::Set { name, value } => {
                self.settings.insert(name.to_ascii_lowercase(), value.clone());
                self.command("SET")
            }
            Plan::Show { name } => {
                let key = name.to_ascii_lowercase();
                let value = self.settings.get(&key).cloned().unwrap_or_else(|| {
                    match key.as_str() {
                        "server_version" => "16.0",
                        "server_encoding" | "client_encoding" => "UTF8",
                        "timezone" => "UTC",
                        "datestyle" => "ISO, MDY",
                        "standard_conforming_strings" | "integer_datetimes" => "on",
                        "transaction_isolation" | "default_transaction_isolation" => "serializable",
                        "search_path" => "\"$user\", public",
                        _ => "",
                    }
                    .to_string()
                });
                if describe {
                    send(&mut self.out, b'T', &row_description(&p.cols, formats)).map_err(io)?;
                }
                let m = Msg::default().i16(1).i32(value.len() as i32);
                let mut row = m.0;
                row.extend_from_slice(value.as_bytes());
                send(&mut self.out, b'D', &row).map_err(io)?;
                self.command("SHOW")
            }
            Plan::Noop { tag } => self.command(tag),
            Plan::Sql { sql, tag, .. } => {
                let mut stmt = self.conn.prepare_cached(sql).map_err(|e| sqlite_error(&e))?;
                for (i, v) in params.iter().enumerate() {
                    let name = format!("${}", i + 1);
                    if let Some(idx) = stmt.parameter_index(&name).map_err(|e| sqlite_error(&e))? {
                        stmt.raw_bind_parameter(idx, v).map_err(|e| sqlite_error(&e))?;
                    }
                }
                let ncols = stmt.column_count();
                if ncols == 0 {
                    stmt.raw_execute().map_err(|e| sqlite_error(&e))?;
                    drop(stmt);
                    let n = self.conn.changes();
                    return self.command(&tag_text(*tag, n));
                }
                let cols = p.cols.clone();
                if describe {
                    send(&mut self.out, b'T', &row_description(&cols, formats)).map_err(io)?;
                }
                let mut rows = stmt.raw_query();
                let mut n = 0u64;
                let mut buf = Vec::with_capacity(256);
                while let Some(row) = rows.next().map_err(|e| sqlite_error(&e))? {
                    buf.clear();
                    buf.extend_from_slice(&(ncols as i16).to_be_bytes());
                    for i in 0..ncols {
                        let v = row.get_ref(i).map_err(|e| sqlite_error(&e))?;
                        let oid = cols.get(i).map_or(types::TEXT, |c| c.1);
                        let cell = if format_of(formats, i) == 1 {
                            types::to_binary(v, oid).map_err(|m| PgError::new("22P02", m))?
                        } else {
                            types::to_text(v, oid)
                        };
                        match cell {
                            None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
                            Some(c) => {
                                buf.extend_from_slice(&(c.len() as i32).to_be_bytes());
                                buf.extend_from_slice(&c);
                            }
                        }
                    }
                    match self.collect.as_mut() {
                        Some(c) => c.push_back(buf.clone()),
                        None => send(&mut self.out, b'D', &buf).map_err(io)?,
                    }
                    n += 1;
                }
                drop(rows);
                drop(stmt);
                if self.collect.is_some() {
                    self.collected_tag = tag_text(*tag, n);
                    return Ok(());
                }
                self.command(&tag_text(*tag, n))
            }
        }
    }
}

fn tag_text(tag: Tag, n: u64) -> String {
    match tag {
        Tag::Select => format!("SELECT {n}"),
        Tag::Insert => format!("INSERT 0 {n}"),
        Tag::Update => format!("UPDATE {n}"),
        Tag::Delete => format!("DELETE {n}"),
        Tag::Other(t) => t.to_string(),
    }
}

/// SQL the parser does not take runs as SQLite SQL, as it was written.
fn raw_plan(q: &str) -> Option<Plan> {
    let first = q.split_whitespace().next()?.to_ascii_uppercase();
    let tag = match first.as_str() {
        "SELECT" | "WITH" | "PRAGMA" | "VALUES" => Tag::Select,
        "INSERT" | "REPLACE" => Tag::Insert,
        "UPDATE" => Tag::Update,
        "DELETE" => Tag::Delete,
        _ => Tag::Other("OK"),
    };
    Some(Plan::Sql {
        sql: q.to_string(),
        tag,
        params: 0,
        param_columns: vec![],
    })
}

fn open(path: &std::path::Path, db: &str) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA busy_timeout = 30000; PRAGMA case_sensitive_like = ON; PRAGMA foreign_keys = ON;")?;
    conn.set_prepared_statement_cache_capacity(256);
    funcs::register(&conn, db)?;
    conn.execute_batch(funcs::CATALOG)?;
    Ok(conn)
}

#[cfg(test)]
mod tests {
    #[test]
    fn rewrites() {
        let cases = [
            (
                "SELECT x::text FROM public.t WHERE n = $1",
                "SELECT CAST(x AS TEXT) AS \"x\" FROM t WHERE n = $1",
            ),
            (
                "SELECT * FROM t WHERE a ILIKE $1",
                "SELECT * FROM t WHERE lower(a) LIKE lower($1)",
            ),
            (
                "SELECT * FROM t WHERE id = 1 FOR UPDATE",
                "SELECT * FROM t WHERE id = 1",
            ),
        ];
        for (input, want) in cases {
            match super::sql::parse(input).unwrap().remove(0) {
                super::Plan::Sql { sql, .. } => assert_eq!(sql, want),
                other => panic!("{other:?}"),
            }
        }
    }
}
