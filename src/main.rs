//! bucketdb: a Postgres database that lives in an S3 bucket, served by a sidecar.
//!
//! The command line over the libraries: [`bucketdb_postgres`] speaks the Postgres
//! protocol and runs the SQL on SQLite, [`bucketdb_redis`] speaks Redis's over a
//! table of the same database, [`bucketdb_storage`] keeps the SQLite file in a
//! bucket. A database is always written `s3://bucket/path`;
//! anything else is a local file.

mod lambda;

use bucketdb_postgres as postgres;
use bucketdb_redis as redis;
use bucketdb_storage::{Remote, env_num, lambda_memory_mb, past, store, sync, vfs};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const USAGE: &str = "usage:
  bucketdb import [--replace] <file.sqlite> s3://bucket/db
                                                   upload a SQLite file as the database
                                                   (--replace: over the one there)
  bucketdb export s3://bucket/db <file.sqlite>     download it, and pin that version
  bucketdb export --at WHEN s3://bucket/db <file>  download a version of the past
  bucketdb versions [--limit N] s3://bucket/db     its versions the bucket keeps, as JSON
  bucketdb restore --at WHEN s3://bucket/db        make a version of the past the current one
                                                   (WHEN: a version id, or a moment, RFC 3339;
                                                   the bucket's versioning on)
  bucketdb upsert <base.sqlite> <updated.sqlite> s3://bucket/db
                                                   commit the pages updated changed;
                                                   base must be what the bucket holds
  bucketdb serve [--host H] [--port P] [--redis-port P] [--dir D] [--create] [--at WHEN]
                 s3://bucket/db...                 Postgres on 5432 and Redis on 6379
                                                   (--redis-port 0: no Redis; --create: an
                                                   empty one where none is; --at: a version
                                                   of the past, read only)
  bucketdb create s3://bucket/db                   an empty database
  bucketdb compact s3://bucket/db

S3: BUCKETDB_S3_ENDPOINT (AWS when unset), BUCKETDB_S3_REGION,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
Ports: POSTGRES_PORT (5432), REDIS_PORT (6379; SELECT n is the nth database)
Auth: BUCKETDB_PASSWORD (any user; trust when unset), for both
Limits: BUCKETDB_MAX_CONNECTIONS (64), BUCKETDB_MAX_TXN_MB (256)
Lambda: run with no arguments as /opt/extensions/bucketdb; serves BUCKETDB_DATABASES
        (s3://bucket/a,s3://bucket/b) on 127.0.0.1:5432 and :6379; cache, transaction limit, disk cache in /tmp
        and connection limit follow the function's memory unless set
Tuning: BUCKETDB_CACHE_MB (64), BUCKETDB_READAHEAD (16 pages),
        BUCKETDB_STALENESS_MS (1000), BUCKETDB_CHUNK_PAGES (256), BUCKETDB_LOG (info),
        BUCKETDB_DISK_CACHE_DIR, BUCKETDB_DISK_CACHE_MB (1024),
        BUCKETDB_COMPACT_SEGMENTS (100), BUCKETDB_GRACE_SECONDS (600),
        BUCKETDB_REDIS_SWEEP_SECONDS (60; 0: expired keys' rows are kept)";

fn main() {
    Logger::init();
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() && lambda::active().is_some() {
        // Lambda runs extensions with no arguments: serve BUCKETDB_DATABASES.
        // SQLite's temp files (big sorts, spilled transactions) go to /tmp, Lambda's
        // only writable place; set before any thread starts.
        if std::env::var_os("TMPDIR").is_none() {
            // SAFETY: single-threaded still.
            unsafe { std::env::set_var("TMPDIR", "/tmp") };
        }
        args.push("serve".into());
        let dbs = std::env::var("BUCKETDB_DATABASES").unwrap_or_default();
        args.extend(
            dbs.split(',')
                .map(str::trim)
                .filter(|d| !d.is_empty())
                .map(String::from),
        );
    }
    let result = match args.first().map(String::as_str) {
        Some("import") if args.len() == 3 => import(&args[1], &args[2], false),
        Some("import") if args.len() == 4 && args[1] == "--replace" => import(&args[2], &args[3], true),
        Some("export") if args.len() == 3 => export(&args[1], &args[2]),
        Some("export") if args.len() == 5 && args[1] == "--at" => export_at(&args[2], &args[3], &args[4]),
        Some("versions") if args.len() == 2 => versions(&args[1], "0"),
        Some("versions") if args.len() == 4 && args[1] == "--limit" => versions(&args[3], &args[2]),
        Some("restore") if args.len() == 4 && args[1] == "--at" => restore(&args[2], &args[3]),
        Some("upsert") if args.len() == 4 => upsert(&args[1], &args[2], &args[3]),
        Some("serve") => serve(&args[1..]),
        Some("compact") if args.len() == 2 => compact(&args[1]),
        Some("create") if args.len() == 2 => create(&args[1]),
        Some("version" | "--version") => {
            println!("bucketdb {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };
    if let Err(e) = result {
        log::error!("{e}");
        std::process::exit(1);
    }
}

/// A local file argument, refused when it looks like a bucket URL.
fn local(path: &str) -> Result<&str> {
    if path.starts_with("s3://") {
        return Err(format!("{path}: expected a local file here").into());
    }
    Ok(path)
}

fn chunk_pages() -> u64 {
    env_num("BUCKETDB_CHUNK_PAGES", 256)
}

fn threads() -> u64 {
    env_num("BUCKETDB_UPLOAD_THREADS", 16)
}

fn export_at(when: &str, url: &str, file: &str) -> Result<()> {
    let (remote, file) = (Remote::parse(url)?, local(file)?);
    let s3 = remote.client()?;
    let snap = past::at(&s3, &remote.db, when)?;
    past::export_at(&s3, &snap, file, threads())?;
    log::info!("exported v{} of {remote}, as of {when}", snap.manifest.version);
    Ok(())
}

/// Prints the versions the bucket keeps, the newest first, as JSON.
fn versions(url: &str, limit: &str) -> Result<()> {
    let remote = Remote::parse(url)?;
    let s3 = remote.client()?;
    let list = past::versions(&s3, &remote.db, limit.parse()?)?;
    println!("{}", serde_json::to_string(&list)?);
    Ok(())
}

fn restore(when: &str, url: &str) -> Result<()> {
    let remote = Remote::parse(url)?;
    let s3 = remote.client()?;
    let version = past::restore(&s3, &remote.db, when, chunk_pages(), threads())?;
    log::info!("{remote}: v{version}, of {when}, is the current version again");
    s3.log_stats();
    Ok(())
}

fn import(file: &str, url: &str, replace: bool) -> Result<()> {
    let (file, remote) = (local(file)?, Remote::parse(url)?);
    let s3 = remote.client()?;
    sync::import(&s3, &remote.db, file, replace, chunk_pages(), threads())?;
    s3.log_stats();
    Ok(())
}

fn create(url: &str) -> Result<()> {
    let remote = Remote::parse(url)?;
    let s3 = remote.client()?;
    sync::create(&s3, &remote.db, chunk_pages())?;
    log::info!("created {remote}");
    Ok(())
}

/// Folds a database's segments into its chunks now.
fn compact(url: &str) -> Result<()> {
    let remote = Remote::parse(url)?;
    let s3 = remote.client()?;
    let rdb = store::RemoteDb::open(&remote.db, s3.clone(), store::Options::from_env())?;
    match rdb.compact()? {
        Some(snap) => log::info!("{remote}: v{}, no segments left", snap.manifest.version),
        None => log::info!("{remote}: nothing to compact (or another compaction won)"),
    }
    s3.log_stats();
    Ok(())
}

fn export(url: &str, file: &str) -> Result<()> {
    let (remote, file) = (Remote::parse(url)?, local(file)?);
    let s3 = remote.client()?;
    sync::export(&s3, &remote.db, file, threads())?;
    s3.log_stats();
    Ok(())
}

fn upsert(base: &str, updated: &str, url: &str) -> Result<()> {
    let (base, updated, remote) = (local(base)?, local(updated)?, Remote::parse(url)?);
    let s3 = remote.client()?;
    sync::upsert(&s3, &remote.db, base, updated)?;
    s3.log_stats();
    Ok(())
}

fn serve(args: &[String]) -> Result<()> {
    let mut host = "127.0.0.1".to_string();
    let mut port = env_num("POSTGRES_PORT", 5432) as u16;
    let mut redis_port = env_num("REDIS_PORT", 6379) as u16;
    let lambda = lambda::active();
    let default_dir = if lambda.is_some() {
        "/tmp/bucketdb/db"
    } else {
        ".bucketdb"
    };
    let mut dir = std::env::var("BUCKETDB_DIR").unwrap_or_else(|_| default_dir.into());
    let mut remotes = Vec::new();
    let mut create = false;
    let mut at: Option<String> = None;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--host" => host.clone_from(it.next().ok_or("--host needs a value")?),
            "--port" => port = it.next().ok_or("--port needs a value")?.parse()?,
            "--redis-port" => redis_port = it.next().ok_or("--redis-port needs a value")?.parse()?,
            "--dir" => dir.clone_from(it.next().ok_or("--dir needs a value")?),
            "--create" => create = true,
            "--at" => at = Some(it.next().ok_or("--at needs a version or a moment")?.clone()),
            url => remotes.push(Remote::parse(url)?),
        }
    }
    if remotes.is_empty() {
        return Err(USAGE.into());
    }
    let names: Vec<String> = remotes.iter().map(|r| r.name.clone()).collect();
    for (i, name) in names.iter().enumerate() {
        if names[..i].contains(name) {
            return Err(format!("two databases end in {name:?}: clients could not tell them apart").into());
        }
    }
    // One client per bucket.
    let mut clients: Vec<(String, Arc<bucketdb_storage::s3::Client>)> = Vec::new();
    let paths = vfs::placeholders(Path::new(&dir), &names)?;
    let mut served = Vec::new();
    let mut rdbs = Vec::new();
    // The moment a version of the past is seen at, for the keys' TTLs.
    let mut at_ms = None;
    for (remote, path) in remotes.iter().zip(&paths) {
        let s3 = if let Some((_, c)) = clients.iter().find(|(b, _)| *b == remote.bucket) {
            c.clone()
        } else {
            let c = remote.client()?;
            clients.push((remote.bucket.clone(), c.clone()));
            c
        };
        if create && at.is_none() {
            sync::ensure(&s3, &remote.db, chunk_pages())?;
        }
        // A version of the past: read only, as it was.
        let rdb = Arc::new(match &at {
            Some(when) => {
                let (snap, moment) = past::at_moment(&s3, &remote.db, when)?;
                at_ms.get_or_insert(moment);
                log::info!("{}: v{}, as of {when}, read only", remote.name, snap.manifest.version);
                store::RemoteDb::open_pinned(&remote.db, s3, store::Options::from_env(), snap)?
            }
            None => store::RemoteDb::open(&remote.db, s3, store::Options::from_env())?,
        });
        log::info!("{}: {remote}", remote.name);
        served.push((path.to_string_lossy().into_owned(), rdb.clone()));
        rdbs.push(rdb);
    }
    vfs::register(served)?;

    let addr = format!("{host}:{port}");
    let listener = listen(&addr)?;
    let password = std::env::var("BUCKETDB_PASSWORD").ok().filter(|p| !p.is_empty());
    // ~2 MB of SQLite cache and a thread each; in Lambda, one per 32 MB.
    let max_connections = env_num(
        "BUCKETDB_MAX_CONNECTIONS",
        lambda_memory_mb().map_or(64, |m| (m / 32).clamp(8, 64)),
    )
    .max(1) as usize;
    // Redis: the same databases, SELECT n the nth; open (and its table made)
    // before the function's init may go on.
    let mut redis_addr = None;
    if redis_port != 0 {
        let addr = format!("{host}:{redis_port}");
        let listener = listen(&addr)?;
        let cfg = redis::Config {
            dbs: names.iter().cloned().zip(paths.iter().cloned()).collect(),
            password: password.clone(),
            max_connections,
            read_only: at.is_some(),
            max_command_bytes: store::Options::from_env().max_txn_bytes() as usize,
            at: at_ms,
            sweep: Some(env_num("BUCKETDB_REDIS_SWEEP_SECONDS", 60))
                .filter(|s| *s > 0)
                .map(Duration::from_secs),
        };
        redis::prepare(&cfg)?;
        let cfg = Arc::new(cfg);
        std::thread::spawn(move || {
            if let Err(e) = redis::serve(listener, cfg) {
                log::error!("bucketdb: redis: {e}");
                std::process::exit(1);
            }
        });
        redis_addr = Some(addr);
    }
    let cfg = Arc::new(postgres::Config {
        dbs: names.iter().cloned().zip(paths.iter().cloned()).collect(),
        password,
        max_connections,
        read_only: at.is_some(),
    });
    let redis_on = redis_addr.map_or(String::new(), |a| format!(", redis on {a}"));
    log::info!(
        "bucketdb {}: postgres on {addr}{redis_on} -> {}",
        env!("CARGO_PKG_VERSION"),
        names.join(", ")
    );
    // Compaction: any sidecar may fold; the manifest's If-Match keeps one winner.
    for rdb in &rdbs {
        let rdb = rdb.clone();
        std::thread::spawn(move || {
            loop {
                rdb.wait_for_compaction(Duration::from_secs(5));
                if (rdb.wants_compaction() || rdb.snapshot().is_ok_and(|_| rdb.wants_compaction()))
                    && let Err(e) = rdb.compact()
                {
                    log::error!("bucketdb: compact {}: {e}", rdb.name);
                }
            }
        });
    }
    std::thread::spawn(move || {
        let mut last = 0;
        loop {
            std::thread::sleep(Duration::from_secs(10));
            let now: u64 = clients.iter().map(|(_, c)| c.requests()).sum();
            if now != last {
                last = now;
                for (_, c) in &clients {
                    c.log_stats();
                }
                for d in &rdbs {
                    d.log_stats();
                }
            }
        }
    });
    if let Some(api) = lambda {
        // The port is open: the function's init may go on.
        std::thread::spawn(move || {
            let code = match lambda::run(&api) {
                Ok(()) => 0,
                Err(e) => {
                    log::error!("bucketdb: {e}");
                    1
                }
            };
            std::process::exit(code);
        });
    }
    postgres::serve(listener, cfg)?;
    Ok(())
}

/// Listens on addr. A restart may find the port still held a moment by the
/// process before.
fn listen(addr: &str) -> Result<std::net::TcpListener> {
    let mut tries = 0;
    loop {
        match std::net::TcpListener::bind(addr) {
            Ok(l) => return Ok(l),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse && tries < 40 => {
                tries += 1;
                std::thread::sleep(Duration::from_millis(250));
            }
            Err(e) => return Err(format!("listen {addr}: {e}").into()),
        }
    }
}

struct Logger {
    level: log::LevelFilter,
}

impl Logger {
    fn init() {
        let level = match std::env::var("BUCKETDB_LOG").as_deref() {
            Ok("debug") => log::LevelFilter::Debug,
            Ok("warn") => log::LevelFilter::Warn,
            Ok("error") => log::LevelFilter::Error,
            _ => log::LevelFilter::Info,
        };
        let _ = log::set_boxed_logger(Box::new(Logger { level }));
        log::set_max_level(level);
    }
}

impl log::Log for Logger {
    fn enabled(&self, m: &log::Metadata) -> bool {
        // Debug is bucketdb's own; the libraries' stays out.
        m.level() <= self.level && (m.level() <= log::Level::Info || m.target().starts_with("bucketdb"))
    }

    fn log(&self, r: &log::Record) {
        if self.enabled(r.metadata()) {
            eprintln!(
                "{} {} {}",
                chrono::Utc::now().format("%H:%M:%S%.3f"),
                r.level(),
                r.args()
            );
        }
    }

    fn flush(&self) {}
}
