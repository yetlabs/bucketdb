//! A database in the bucket and a SQLite file on disk: `import` uploads a file as a
//! database's base (or replaces it), `create` starts an empty one, and the two below
//! keep a file and the bucket in step by pages.
//!
//! `export` writes the bucket's current version to a file and pins it: the manifest
//! records the file's SHA-256 (`synced`). `upsert` takes that file and a copy of it
//! changed with SQLite (the same page layout, only some pages differ) and commits the
//! differing pages as one version, without reading anything from the bucket. Any
//! commit through a sidecar clears `synced`, so upsert never overwrites a change it
//! has not seen.

use crate::s3::{Client, Cond, Get, Put};
use crate::store::{self, Manifest, Segment, Snapshot};
use ring::digest;
use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Bytes read from disk at a time.
const BLOCK: usize = 8 << 20;
/// Pages per segment upsert writes (the commit names several when there are more).
const SEGMENT_BYTES: usize = 64 << 20;

/// A SQLite file's page size, from its header.
///
/// # Errors
///
/// When the file cannot be read or is not a SQLite database.
pub fn page_size(f: &File, name: &str) -> io::Result<u64> {
    let mut header = [0u8; 100];
    f.read_exact_at(&mut header, 0)?;
    if &header[..16] != b"SQLite format 3\0" {
        return Err(io::Error::other(format!("{name} is not a SQLite database")));
    }
    Ok(match u16::from_be_bytes([header[16], header[17]]) {
        1 => 65536,
        n => u64::from(n),
    })
}

/// Refuses a file whose WAL still holds pages the main file lacks.
///
/// # Errors
///
/// When `<file>-wal` is not empty.
pub fn no_wal(file: &str) -> io::Result<()> {
    if std::fs::metadata(format!("{file}-wal")).is_ok_and(|m| m.len() > 0) {
        return Err(io::Error::other(format!(
            "{file}-wal is there: checkpoint the database first"
        )));
    }
    Ok(())
}

/// In the bucket a database is in rollback-journal mode, whatever the file said
/// (header bytes 18 and 19 are 2 in WAL mode): it needs no shared memory.
pub fn rollback_mode(page1: &mut [u8]) {
    page1[18] = 1;
    page1[19] = 1;
}

/// SHA-256 of a SQLite file as the bucket holds it (rollback mode in its header).
///
/// # Errors
///
/// When the file cannot be read.
pub fn digest(path: &str) -> io::Result<String> {
    let f = File::open(path)?;
    let len = f.metadata()?.len();
    let mut hash = digest::Context::new(&digest::SHA256);
    let mut buf = vec![0u8; BLOCK];
    let mut at = 0;
    while at < len {
        let n = (len - at).min(BLOCK as u64) as usize;
        f.read_exact_at(&mut buf[..n], at)?;
        if at == 0 && n > 19 {
            rollback_mode(&mut buf);
        }
        hash.update(&buf[..n]);
        at += n as u64;
    }
    Ok(hex::encode(hash.finish()))
}

fn current(s3: &Client, db: &str) -> io::Result<Snapshot> {
    store::fetch_manifest(s3, db)?.ok_or_else(|| io::Error::other(format!("s3://{}/{db} does not exist", s3.bucket())))
}

/// Writes the bucket's current version of db to file, and pins it (see the module).
///
/// # Errors
///
/// On S3 or disk errors, or when a commit lands while it downloads.
pub fn export(s3: &Client, db: &str, file: &str, threads: u64) -> io::Result<()> {
    let snap = current(s3, db)?;
    let m = &snap.manifest;
    let ps = snap.page_size();
    let cp = u64::from(m.chunk_pages);
    let started = Instant::now();
    let out = File::create(file)?;
    out.set_len(m.page_count * ps)?;
    let base_chunks = m.base_pages.min(m.page_count).div_ceil(cp);
    log::info!(
        "export s3://{}/{db} v{} -> {file}: {} pages, {base_chunks} chunks, {} segments",
        s3.bucket(),
        m.version,
        m.page_count,
        m.segments.len()
    );
    // The chunks, in parallel; then the segments in order, newest last, over them.
    let next = AtomicU64::new(0);
    std::thread::scope(|scope| -> io::Result<()> {
        let workers: Vec<_> = (0..threads.max(1))
            .map(|_| {
                scope.spawn(|| -> io::Result<()> {
                    loop {
                        let c = next.fetch_add(1, Ordering::Relaxed);
                        if c >= base_chunks {
                            return Ok(());
                        }
                        let key = snap.chunk_key(c);
                        let Get::Found { body, .. } = s3.get(&key, None, None)? else {
                            return Err(io::Error::other(format!("{key}: missing")));
                        };
                        // Pages past page_count (the file shrank) are not part of it.
                        let room = (m.page_count - c * cp) * ps;
                        let n = (body.len() as u64).min(room) as usize;
                        out.write_all_at(&body[..n], c * cp * ps)?;
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().map_err(|_| io::Error::other("export thread panicked"))??;
        }
        Ok(())
    })?;
    for seg in &m.segments {
        let Get::Found { body, .. } = s3.get(&seg.key, None, None)? else {
            return Err(io::Error::other(format!("{}: missing", seg.key)));
        };
        for (i, &p) in seg.pages.iter().enumerate() {
            if p <= m.page_count {
                let at = i * ps as usize;
                out.write_all_at(&body[at..at + ps as usize], (p - 1) * ps)?;
            }
        }
    }
    out.sync_all()?;
    drop(out);
    let sum = digest(file)?;
    if m.synced.as_deref() != Some(sum.as_str()) {
        let mut pinned = m.clone();
        pinned.synced = Some(sum);
        let json = serde_json::to_vec(&pinned).map_err(io::Error::other)?;
        if let Put::PreconditionFailed = s3.put(&store::manifest_key(db), &json, Cond::IfMatch(&snap.etag))? {
            return Err(io::Error::other(format!(
                "{file} holds v{}, but s3://{}/{db} changed meanwhile: export again",
                m.version,
                s3.bucket()
            )));
        }
    }
    log::info!("exported v{} in {:.1}s", m.version, started.elapsed().as_secs_f64());
    Ok(())
}

/// Commits the pages in which `updated` differs from `base`, which must be what the
/// bucket holds (see the module).
///
/// # Errors
///
/// When base is not the bucket's current version, the files are not SQLite with the
/// same page size, a commit lands meanwhile, or on S3 and disk errors.
pub fn upsert(s3: &Client, db: &str, base: &str, updated: &str) -> io::Result<()> {
    no_wal(base)?;
    no_wal(updated)?;
    let snap = current(s3, db)?;
    let m = &snap.manifest;
    let url = format!("s3://{}/{db}", s3.bucket());
    let (bf, uf) = (File::open(base)?, File::open(updated)?);
    let ps = page_size(&bf, base)?;
    if page_size(&uf, updated)? != ps || ps != snap.page_size() {
        return Err(io::Error::other(format!(
            "{base}, {updated} and {url} differ in page size"
        )));
    }
    let started = Instant::now();
    match &m.synced {
        Some(sum) if *sum == digest(base)? => {}
        Some(_) => {
            return Err(io::Error::other(format!(
                "{base} is not what {url} holds (v{}): bucketdb export it first",
                m.version
            )));
        }
        None => {
            return Err(io::Error::other(format!(
                "{url} v{} is not pinned to a local file (a sidecar committed since): bucketdb export it first",
                m.version
            )));
        }
    }

    let base_len = bf.metadata()?.len();
    let new_len = uf.metadata()?.len();
    let page_count = new_len / ps;
    let version = m.version + 1;
    let mut segments: Vec<Segment> = Vec::new();
    let mut body: Vec<u8> = Vec::new();
    let mut pages: Vec<u64> = Vec::new();
    let mut flush = |body: &mut Vec<u8>, pages: &mut Vec<u64>| -> io::Result<()> {
        if pages.is_empty() {
            return Ok(());
        }
        let key = format!(
            "{db}/seg/{version:012}-upsert-{}-{}",
            std::process::id(),
            segments.len()
        );
        s3.put(&key, body, Cond::None)?;
        segments.push(Segment {
            version,
            key,
            pages: std::mem::take(pages),
        });
        body.clear();
        Ok(())
    };
    let (mut a, mut b) = (vec![0u8; BLOCK], vec![0u8; BLOCK]);
    let mut at = 0;
    while at < page_count * ps {
        let n = (page_count * ps - at).min(BLOCK as u64) as usize;
        uf.read_exact_at(&mut b[..n], at)?;
        // Past the end of base, every page is new.
        let have = base_len.saturating_sub(at).min(n as u64) as usize;
        bf.read_exact_at(&mut a[..have], at)?;
        if at == 0 {
            rollback_mode(&mut a);
            rollback_mode(&mut b);
        }
        for off in (0..n).step_by(ps as usize) {
            let page = &b[off..off + ps as usize];
            if off >= have || a[off..off + ps as usize] != *page {
                pages.push((at + off as u64) / ps + 1);
                body.extend_from_slice(page);
                if body.len() >= SEGMENT_BYTES {
                    flush(&mut body, &mut pages)?;
                }
            }
        }
        at += n as u64;
    }
    flush(&mut body, &mut pages)?;
    drop(flush);

    let changed: usize = segments.iter().map(|s| s.pages.len()).sum();
    if changed == 0 && page_count == m.page_count {
        log::info!("{url}: {updated} equals {base}, nothing to send");
        return Ok(());
    }
    let mut manifest: Manifest = m.clone();
    manifest.version = version;
    manifest.page_count = page_count;
    manifest.segments.extend(segments.iter().cloned());
    manifest.synced = Some(digest(updated)?);
    let json = serde_json::to_vec(&manifest).map_err(io::Error::other)?;
    if let Put::PreconditionFailed = s3.put(&store::manifest_key(db), &json, Cond::IfMatch(&snap.etag))? {
        for seg in &segments {
            let _ = s3.delete(&seg.key);
        }
        return Err(io::Error::other(format!(
            "{url} changed while uploading: export it and run again"
        )));
    }
    let total = page_count.max(1);
    log::info!(
        "{url}: v{version}, {changed} of {page_count} pages ({:.1}%, {:.1} MB) in {} segments, {:.1}s",
        changed as f64 * 100.0 / total as f64,
        (changed as u64 * ps) as f64 / 1e6,
        segments.len(),
        started.elapsed().as_secs_f64()
    );
    if changed as u64 * 2 > total {
        log::warn!("most pages differ: was {updated} made from a copy of {base}? A file built anew has another layout");
    }
    Ok(())
}

/// Uploads a SQLite file as the base of a new database or, with replace, as the next
/// version of the one there: readers move to it at their next transaction, and the
/// old objects are deleted after the grace period.
///
/// # Errors
///
/// When the file is not SQLite or has a WAL, the database exists (without replace),
/// the page size differs from the one replaced, or on S3 and disk errors.
pub fn import(s3: &Client, db: &str, file: &str, replace: bool, chunk_pages: u64, threads: u64) -> io::Result<()> {
    no_wal(file)?;
    let f = File::open(file)?;
    let len = f.metadata()?.len();
    let ps = page_size(&f, file)?;
    let pages = len / ps;
    let chunk_pages = chunk_pages.max(1);
    let chunks = pages.div_ceil(chunk_pages);
    let url = format!("s3://{}/{db}", s3.bucket());
    s3.create_bucket()?;
    if !replace && store::fetch_manifest(s3, db)?.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{url} already exists: upsert or import --replace updates it"),
        ));
    }
    let base = format!("{db}/base/{}", chrono::Utc::now().format("%Y%m%dT%H%M%S"));
    log::info!("import {file} -> {url}: {pages} pages of {ps} B in {chunks} chunks of {chunk_pages}");

    let started = Instant::now();
    let next = AtomicU64::new(0);
    let done = AtomicU64::new(0);
    std::thread::scope(|scope| -> io::Result<()> {
        let workers: Vec<_> = (0..threads.max(1))
            .map(|_| {
                scope.spawn(|| -> io::Result<()> {
                    let mut buf = vec![0u8; (chunk_pages * ps) as usize];
                    loop {
                        let c = next.fetch_add(1, Ordering::Relaxed);
                        if c >= chunks {
                            return Ok(());
                        }
                        let start = c * chunk_pages * ps;
                        let n = ((pages * ps) - start).min(chunk_pages * ps) as usize;
                        f.read_exact_at(&mut buf[..n], start)?;
                        if c == 0 {
                            rollback_mode(&mut buf);
                        }
                        s3.put(&format!("{base}/{c:08}"), &buf[..n], Cond::None)?;
                        let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                        if d.is_multiple_of((chunks / 20).max(1)) {
                            let mb = (d * chunk_pages * ps) as f64 / 1e6;
                            log::info!(
                                "  {d}/{chunks} chunks, {mb:.0} MB, {:.0} MB/s",
                                mb / started.elapsed().as_secs_f64()
                            );
                        }
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().map_err(|_| io::Error::other("upload thread panicked"))??;
        }
        Ok(())
    })?;
    let manifest = Manifest {
        version: 1,
        synced: Some(digest(file)?),
        page_size: ps as u32,
        page_count: pages,
        chunk_pages: chunk_pages as u32,
        base_pages: pages,
        base: base.clone(),
        ..Default::default()
    };
    if replace {
        replace_manifest(s3, db, manifest, &base, chunks)?;
    } else {
        store::create(s3, db, &manifest)?;
    }
    log::info!("uploaded in {:.1}s", started.elapsed().as_secs_f64());
    Ok(())
}

/// Swaps the manifest to a freshly uploaded base, over whatever version is there.
fn replace_manifest(s3: &Client, db: &str, mut manifest: Manifest, base: &str, chunks: u64) -> io::Result<()> {
    let url = format!("s3://{}/{db}", s3.bucket());
    for _ in 0..8 {
        let Some(old) = store::fetch_manifest(s3, db)? else {
            return store::create(s3, db, &manifest);
        };
        if old.manifest.page_size != manifest.page_size {
            delete_chunks(s3, base, chunks);
            return Err(io::Error::other(format!(
                "{url} has pages of {} B, the file of {} B: open sidecars could not follow; import under another path",
                old.manifest.page_size, manifest.page_size
            )));
        }
        manifest.version = old.manifest.version + 1;
        let json = serde_json::to_vec(&manifest).map_err(io::Error::other)?;
        if let Put::Done { .. } = s3.put(&store::manifest_key(db), &json, Cond::IfMatch(&old.etag))? {
            let now = store::now_unix();
            let retired: Vec<store::Retired> = store::objects(&old)
                .into_iter()
                .map(|key| store::Retired { key, at: now })
                .collect();
            if let Err(e) = store::collect_garbage(s3, db, store::Options::from_env().grace, &retired) {
                log::warn!("{url}: gc: {e}");
            }
            log::info!("{url}: v{} replaces v{}", manifest.version, old.manifest.version);
            return Ok(());
        }
    }
    delete_chunks(s3, base, chunks);
    Err(io::Error::other(format!("{url} kept changing; nothing replaced")))
}

fn delete_chunks(s3: &Client, base: &str, chunks: u64) {
    for c in 0..chunks {
        let _ = s3.delete(&format!("{base}/{c:08}"));
    }
}

/// An empty database: no pages; the first commit writes page 1.
///
/// # Errors
///
/// When it exists, or on S3 errors.
pub fn create(s3: &Client, db: &str, chunk_pages: u64) -> io::Result<()> {
    s3.create_bucket()?;
    store::create(s3, db, &empty(db, chunk_pages))
}

/// An empty database, unless the bucket holds one already: what a sidecar given
/// a bucket of its own starts on. The bucket must exist; it is not made here, as
/// whoever gave it may not let a sidecar make buckets. Two sidecars starting at
/// once both find it there: one writes the manifest, the other's If-None-Match
/// fails, and neither minds.
///
/// # Errors
///
/// On S3 errors.
pub fn ensure(s3: &Client, db: &str, chunk_pages: u64) -> io::Result<()> {
    if store::fetch_manifest(s3, db)?.is_some() {
        return Ok(());
    }
    match s3.put(
        &store::manifest_key(db),
        &serde_json::to_vec(&empty(db, chunk_pages)).map_err(io::Error::other)?,
        Cond::IfNoneMatch,
    )? {
        Put::Done { .. } | Put::PreconditionFailed => Ok(()),
    }
}

fn empty(db: &str, chunk_pages: u64) -> Manifest {
    Manifest {
        version: 1,
        page_size: 4096,
        chunk_pages: chunk_pages.max(1) as u32,
        base: format!("{db}/base/empty"),
        chunks: BTreeMap::new(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::{digest, page_size, rollback_mode};
    use std::io::Write;

    fn file(bytes: &[u8]) -> (std::path::PathBuf, std::fs::File) {
        let p = std::env::temp_dir().join(format!("bucketdb-sync-{}-{}", std::process::id(), bytes.len()));
        std::fs::File::create(&p).unwrap().write_all(bytes).unwrap();
        let f = std::fs::File::open(&p).unwrap();
        (p, f)
    }

    #[test]
    fn digest_ignores_the_journal_mode() {
        let mut wal = vec![0u8; 8192];
        wal[..16].copy_from_slice(b"SQLite format 3\0");
        wal[16..18].copy_from_slice(&4096u16.to_be_bytes());
        wal[18] = 2;
        wal[19] = 2;
        let mut rollback = wal.clone();
        rollback_mode(&mut rollback);
        rollback.push(0); // a different length, so a different file name
        let (a, fa) = file(&wal);
        let (b, _) = file(&rollback[..8192]);
        assert_eq!(page_size(&fa, "a").unwrap(), 4096);
        assert_eq!(
            digest(&a.to_string_lossy()).unwrap(),
            digest(&b.to_string_lossy()).unwrap()
        );
        let _ = (std::fs::remove_file(a), std::fs::remove_file(b));
    }

    #[test]
    fn page_size_refuses_other_files() {
        let (p, f) = file(&[7u8; 200]);
        assert!(page_size(&f, "x").is_err());
        let _ = std::fs::remove_file(p);
    }
}
