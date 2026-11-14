//! Against a real S3 (MinIO locally): set BUCKETDB_TEST_BUCKET, the credentials and,
//! for MinIO, BUCKETDB_S3_ENDPOINT (`make test-s3` does, from .data/env.sh). Without
//! a bucket each test says so and passes. Every test works under its own prefix.

// The helpers below the tests fail the test they serve, as an assert would.
#![allow(clippy::unwrap_used)]

use bucketdb_storage::{Remote, store, sync, vfs};
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

struct Case {
    remote: Remote,
    s3: Arc<bucketdb_storage::s3::Client>,
    dir: PathBuf,
}

impl Case {
    fn new(name: &str) -> Option<Case> {
        let Ok(bucket) = std::env::var("BUCKETDB_TEST_BUCKET") else {
            eprintln!("skipped: BUCKETDB_TEST_BUCKET is not set");
            return None;
        };
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let remote = Remote::parse(&format!("s3://{bucket}/test/{nanos}/{name}")).unwrap();
        let s3 = remote.client().unwrap();
        let dir = std::env::temp_dir().join(format!("bucketdb-test-{nanos}-{name}"));
        std::fs::create_dir_all(&dir).unwrap();
        Some(Case { remote, s3, dir })
    }

    fn file(&self, name: &str) -> String {
        self.dir.join(name).to_string_lossy().into_owned()
    }

    fn import(&self, file: &str, replace: bool) -> std::io::Result<()> {
        sync::import(&self.s3, &self.remote.db, file, replace, 4, 4)
    }

    fn manifest(&self) -> store::Manifest {
        store::fetch_manifest(&self.s3, &self.remote.db)
            .unwrap()
            .unwrap()
            .manifest
    }
}

impl Drop for Case {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A SQLite file with rows rows, in rollback mode.
fn sqlite(path: &str, rows: u32) {
    let c = Connection::open(path).unwrap();
    c.execute_batch(
        "PRAGMA journal_mode = DELETE; PRAGMA page_size = 4096;
         CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);",
    )
    .unwrap();
    let tx = c.unchecked_transaction().unwrap();
    for i in 0..rows {
        tx.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            (i, format!("row {i:06} {}", "x".repeat(40))),
        )
        .unwrap();
    }
    tx.commit().unwrap();
}

fn same_bytes(a: &str, b: &str) -> bool {
    std::fs::read(a).unwrap() == std::fs::read(b).unwrap()
}

#[test]
fn import_then_export_gives_the_file_back() {
    let Some(t) = Case::new("roundtrip") else { return };
    let (src, out) = (t.file("src.sqlite"), t.file("out.sqlite"));
    sqlite(&src, 5_000);
    t.import(&src, false).unwrap();
    assert_eq!(t.manifest().synced, Some(sync::digest(&src).unwrap()));
    sync::export(&t.s3, &t.remote.db, &out, 4).unwrap();
    assert!(same_bytes(&src, &out));
    // A second import over it is refused.
    assert!(t.import(&src, false).is_err());
}

#[test]
fn upsert_sends_only_the_changed_pages() {
    let Some(t) = Case::new("upsert") else { return };
    let (base, updated, out) = (t.file("base.sqlite"), t.file("updated.sqlite"), t.file("out.sqlite"));
    sqlite(&base, 20_000);
    t.import(&base, false).unwrap();
    std::fs::copy(&base, &updated).unwrap();
    let c = Connection::open(&updated).unwrap();
    c.execute("UPDATE t SET v = 'changed' WHERE id IN (10, 19000)", [])
        .unwrap();
    c.execute("INSERT INTO t VALUES (50000, 'new')", []).unwrap();
    drop(c);

    let before = t.manifest();
    sync::upsert(&t.s3, &t.remote.db, &base, &updated).unwrap();
    let after = t.manifest();
    assert_eq!(after.version, before.version + 1);
    let sent: usize = after.segments.iter().map(|s| s.pages.len()).sum();
    assert!(sent > 0 && sent < 10, "{sent} pages for three rows");
    assert_eq!(after.synced, Some(sync::digest(&updated).unwrap()));

    sync::export(&t.s3, &t.remote.db, &out, 4).unwrap();
    assert!(same_bytes(&updated, &out));
    // The bucket is no longer base: an upsert from it is refused.
    assert!(sync::upsert(&t.s3, &t.remote.db, &base, &updated).is_err());
}

#[test]
fn replace_swaps_the_whole_file() {
    let Some(t) = Case::new("replace") else { return };
    let (a, b, out) = (t.file("a.sqlite"), t.file("b.sqlite"), t.file("out.sqlite"));
    sqlite(&a, 1_000);
    sqlite(&b, 3_000);
    t.import(&a, false).unwrap();
    t.import(&b, true).unwrap();
    let m = t.manifest();
    assert_eq!(m.version, 2);
    assert_eq!(m.synced, Some(sync::digest(&b).unwrap()));
    sync::export(&t.s3, &t.remote.db, &out, 4).unwrap();
    assert!(same_bytes(&b, &out));
}

/// The VFS registers once per process, so one test drives SQLite through it.
#[test]
fn sqlite_commits_through_the_vfs() {
    let Some(t) = Case::new("vfs") else { return };
    let src = t.file("src.sqlite");
    sqlite(&src, 100);
    t.import(&src, false).unwrap();

    let rdb = Arc::new(store::RemoteDb::open(&t.remote.db, t.s3.clone(), store::Options::from_env()).unwrap());
    let paths = vfs::placeholders(&t.dir.join("db"), std::slice::from_ref(&t.remote.name)).unwrap();
    vfs::register(vec![(paths[0].to_string_lossy().into_owned(), rdb)]).unwrap();

    let c = Connection::open(Path::new(&paths[0])).unwrap();
    let n: i64 = c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(n, 100);
    c.execute("INSERT INTO t VALUES (1000, 'through the bucket')", [])
        .unwrap();

    // The commit is a new version with a segment, and the bucket no longer equals
    // the imported file.
    let m = t.manifest();
    assert_eq!(m.version, 2);
    assert_eq!(m.segments.len(), 1);
    assert_eq!(m.synced, None);
    assert!(sync::upsert(&t.s3, &t.remote.db, &src, &src).is_err());

    // A second connection, as another client would, sees the row.
    let c2 = Connection::open(Path::new(&paths[0])).unwrap();
    let v: String = c2
        .query_row("SELECT v FROM t WHERE id = 1000", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, "through the bucket");
}
