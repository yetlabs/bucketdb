//! The server over TCP, on a plain SQLite file (no bucket): the protocol,
//! connection commands, pipelining, concurrent writers, the sweep and SCAN.

#![allow(clippy::unwrap_used)]

use bucketdb_redis::{Config, prepare, serve};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A new directory for a test's databases: tests run in parallel.
fn new_dir() -> std::path::PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "bucketdb-redis-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(dir: &std::path::Path, dbs: usize) -> Config {
    Config {
        dbs: (0..dbs)
            .map(|i| (format!("db{i}"), dir.join(format!("db{i}.sqlite"))))
            .collect(),
        password: None,
        max_connections: 16,
        read_only: false,
        max_command_bytes: 1 << 20,
        at: None,
        sweep: None,
    }
}

fn serve_on(cfg: Config) -> String {
    prepare(&cfg).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || serve(listener, Arc::new(cfg)));
    addr
}

fn start(password: Option<&str>, dbs: usize, read_only: bool) -> String {
    let mut cfg = config(&new_dir(), dbs);
    cfg.password = password.map(Into::into);
    cfg.read_only = read_only;
    serve_on(cfg)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

struct Client {
    w: TcpStream,
    r: BufReader<TcpStream>,
}

impl Client {
    fn new(addr: &str) -> Client {
        let w = TcpStream::connect(addr).unwrap();
        let r = BufReader::new(w.try_clone().unwrap());
        Client { w, r }
    }

    fn send(&mut self, args: &[&str]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend(format!("${}\r\n{a}\r\n", a.len()).bytes());
        }
        self.w.write_all(&buf).unwrap();
    }

    /// A reply, written back compactly: +OK, -ERR..., :1, "v", nil, [a, b].
    fn reply(&mut self) -> String {
        let mut line = String::new();
        self.r.read_line(&mut line).unwrap();
        let line = line.trim_end();
        let (t, rest) = line.split_at(1);
        match t {
            "$" if rest == "-1" => "nil".into(),
            "$" => {
                let mut b = vec![0; rest.parse::<usize>().unwrap() + 2];
                self.r.read_exact(&mut b).unwrap();
                format!("{:?}", String::from_utf8_lossy(&b[..b.len() - 2]))
            }
            "*" => {
                let items: Vec<String> = (0..rest.parse::<usize>().unwrap()).map(|_| self.reply()).collect();
                format!("[{}]", items.join(", "))
            }
            _ => line.into(),
        }
    }

    fn cmd(&mut self, args: &[&str]) -> String {
        self.send(args);
        self.reply()
    }
}

#[test]
fn commands_over_tcp() {
    let addr = start(None, 2, false);
    let c = &mut Client::new(&addr);
    assert_eq!(c.cmd(&["PING"]), "+PONG");
    assert_eq!(
        c.cmd(&["HELLO", "3"]),
        "-NOPROTO sorry, this protocol version is not supported."
    );
    assert!(c.cmd(&["HELLO", "2"]).starts_with("[\"server\", \"redis\""));
    assert_eq!(c.cmd(&["CLIENT", "SETINFO", "lib-name", "x"]), "+OK");
    assert_eq!(c.cmd(&["SET", "k", "v", "EX", "60"]), "+OK");
    assert_eq!(c.cmd(&["get", "k"]), "\"v\"");
    assert_eq!(c.cmd(&["TTL", "k"]), ":60");
    // Pipelined: all sent, then all read.
    c.send(&["INCR", "n"]);
    c.send(&["INCR", "n"]);
    c.send(&["MGET", "k", "n", "x"]);
    assert_eq!(c.reply(), ":1");
    assert_eq!(c.reply(), ":2");
    assert_eq!(c.reply(), "[\"v\", \"2\", nil]");
    // Each database its own keys.
    assert_eq!(c.cmd(&["SELECT", "1"]), "+OK");
    assert_eq!(c.cmd(&["GET", "k"]), "nil");
    assert_eq!(c.cmd(&["SELECT", "2"]), "-ERR DB index is out of range");
    assert_eq!(c.cmd(&["SELECT", "0"]), "+OK");
    assert_eq!(c.cmd(&["GET", "k"]), "\"v\"");
    assert_eq!(
        c.cmd(&["NOPE", "a"]),
        "-ERR unknown command 'nope', with args beginning with: 'a'"
    );
    // Inline, as typed into a terminal.
    c.w.write_all(b"PING hi\r\n").unwrap();
    assert_eq!(c.reply(), "\"hi\"");
    assert_eq!(c.cmd(&["QUIT"]), "+OK");
}

#[test]
fn protocol_error_closes() {
    let addr = start(None, 1, false);
    let c = &mut Client::new(&addr);
    c.w.write_all(b"*1\r\n+PING\r\n").unwrap();
    assert_eq!(c.reply(), "-ERR Protocol error: expected '$', got '+'");
    let mut rest = Vec::new();
    c.r.read_to_end(&mut rest).unwrap();
    assert_eq!(rest, b"");
}

#[test]
fn auth() {
    let addr = start(Some("secret"), 1, false);
    let c = &mut Client::new(&addr);
    assert_eq!(c.cmd(&["GET", "k"]), "-NOAUTH Authentication required.");
    assert_eq!(
        c.cmd(&["AUTH", "nope"]),
        "-WRONGPASS invalid username-password pair or user is disabled."
    );
    assert_eq!(c.cmd(&["AUTH", "default", "secret"]), "+OK");
    assert_eq!(c.cmd(&["GET", "k"]), "nil");
    let c = &mut Client::new(&addr);
    assert!(c.cmd(&["HELLO", "2", "AUTH", "default", "secret"]).starts_with('['));
    assert_eq!(c.cmd(&["PING"]), "+PONG");
}

#[test]
fn read_only() {
    let addr = start(None, 1, true);
    let c = &mut Client::new(&addr);
    // No table in this version: it reads as empty.
    assert_eq!(c.cmd(&["GET", "k"]), "nil");
    assert_eq!(c.cmd(&["DBSIZE"]), ":0");
    assert_eq!(
        c.cmd(&["SET", "k", "v"]),
        "-READONLY You can't write against a read only replica."
    );
}

#[test]
fn concurrent_writers() {
    let addr = start(None, 1, false);
    assert_eq!(Client::new(&addr).cmd(&["SET", "s", "abc"]), "+OK");
    let threads: Vec<_> = (0..8)
        .map(|i| {
            let addr = addr.clone();
            std::thread::spawn(move || {
                let c = &mut Client::new(&addr);
                for _ in 0..50 {
                    assert!(c.cmd(&["INCR", "n"]).starts_with(':'));
                    // A command that fails, in the same commits as the others.
                    if i == 0 {
                        assert_eq!(c.cmd(&["INCR", "s"]), "-ERR value is not an integer or out of range");
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let c = &mut Client::new(&addr);
    assert_eq!(c.cmd(&["GET", "n"]), "\"400\"");
    assert_eq!(c.cmd(&["GET", "s"]), "\"abc\"");
    // Commands that arrived together went out together.
    let info = c.cmd(&["INFO"]);
    let stat = |k: &str| -> u64 {
        let line = info.split("\\r\\n").find(|l| l.starts_with(k)).unwrap();
        line[k.len() + 1..].parse().unwrap()
    };
    let (commits, writes) = (stat("bucketdb_commits"), stat("bucketdb_writes"));
    assert_eq!(writes, 1 + 400 + 50);
    assert!(commits < writes, "{commits} commits for {writes} writes");
}

#[test]
fn sweeps_expired_rows() {
    let mut cfg = config(&new_dir(), 1);
    cfg.sweep = Some(std::time::Duration::from_millis(20));
    let addr = serve_on(cfg);
    let c = &mut Client::new(&addr);
    assert_eq!(c.cmd(&["SET", "a", "1", "PX", "1"]), "+OK");
    assert_eq!(c.cmd(&["SET", "b", "2", "PX", "1"]), "+OK");
    assert_eq!(c.cmd(&["SET", "c", "3"]), "+OK");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if c.cmd(&["INFO"]).contains("expired_keys:2") {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "no sweep: {}", c.cmd(&["INFO"]));
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_eq!(c.cmd(&["DBSIZE"]), ":1");
    assert_eq!(c.cmd(&["GET", "c"]), "\"3\"");
}

#[test]
fn reads_the_past_at_its_moment() {
    let dir = new_dir();
    let writer = serve_on(config(&dir, 1));
    let t = now_ms() + 60_000;
    let c = &mut Client::new(&writer);
    assert_eq!(c.cmd(&["SET", "k", "v", "PXAT", &t.to_string()]), "+OK");
    // Read only, a moment before the key expires and one after.
    for (at, want) in [(t - 1, "\"v\""), (t, "nil")] {
        let mut cfg = config(&dir, 1);
        cfg.read_only = true;
        cfg.at = Some(at);
        let c = &mut Client::new(&serve_on(cfg));
        assert_eq!(c.cmd(&["GET", "k"]), want);
    }
    let mut cfg = config(&dir, 1);
    cfg.read_only = true;
    cfg.at = Some(t - 1000);
    let c = &mut Client::new(&serve_on(cfg));
    assert_eq!(c.cmd(&["PTTL", "k"]), ":1000");
}

#[test]
fn scan_cursors_go_across_connections() {
    let addr = start(None, 2, false);
    let a = &mut Client::new(&addr);
    for i in 0..5 {
        assert_eq!(a.cmd(&["SET", &format!("k{i}"), "v"]), "+OK");
    }
    a.send(&["SCAN", "0", "COUNT", "3"]);
    assert_eq!(a.r.by_ref().lines().next().unwrap().unwrap(), "*2");
    let cursor = a.reply().trim_matches('"').to_string();
    assert_eq!(a.reply(), "[\"k0\", \"k1\", \"k2\"]");
    // Another connection of the pool goes on with it.
    let b = &mut Client::new(&addr);
    assert_eq!(b.cmd(&["SCAN", &cursor, "COUNT", "3"]), "[\"0\", [\"k3\", \"k4\"]]");
    // Not in another database.
    assert_eq!(b.cmd(&["SELECT", "1"]), "+OK");
    assert_eq!(b.cmd(&["SCAN", &cursor]), "-ERR invalid cursor");
}
