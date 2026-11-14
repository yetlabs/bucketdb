//! A database in a bucket: a manifest naming its pages, a base cut in chunks, and one
//! segment per commit holding the pages it changed.
//!
//! ```text
//! <db>/manifest.json            version, page size and count, base, segments
//! <db>/base/<id>/<chunk:08>     chunk_pages pages of the file
//! <db>/seg/<version:012>-<n>    the pages one commit wrote, in its list's order
//! ```
//!
//! A page is in the newest segment that lists it, else in its chunk of the base. A
//! chunk lives under the base's prefix unless a compaction rewrote it, which the
//! manifest's `chunks` map records. Objects are never changed once written, so a
//! page is cached by where it lives and no cache entry is ever stale.
//!
//! A commit writes its segment, then replaces the manifest with If-Match on the ETag
//! its transaction started from. When another commit got there first, the
//! transaction is replayed over it if neither touched what the other wrote: the pages
//! this one read or wrote against the pages the others wrote (page 1, whose counters
//! every commit bumps, is merged). Otherwise it is told to run again.
//!
//! Compaction folds the segments into the chunks they touch, written anew, and drops
//! them from the manifest. What it replaced is listed as retired and deleted by a
//! later compaction, once no transaction can still be reading it.

use crate::s3::{self, Client, Cond, Get, Put};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,
    pub page_size: u32,
    pub page_count: u64,
    pub chunk_pages: u32,
    /// Pages the chunks hold; a later page no segment lists reads as zeros.
    pub base_pages: u64,
    /// Prefix of the chunks as imported.
    pub base: String,
    /// Chunks a compaction rewrote: chunk number -> prefix.
    #[serde(default)]
    pub chunks: BTreeMap<u64, String>,
    pub segments: Vec<Segment>,
    /// The newest version a compaction folded into the chunks.
    #[serde(default)]
    pub folded: u64,
    /// The pages of the commits the latest compaction folded (versions past
    /// recent_after), so a transaction that started before it can still be replayed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent: Vec<Segment>,
    #[serde(default)]
    pub recent_after: u64,
    /// SHA-256 of the local SQLite file this version equals byte for byte (with its
    /// header in rollback mode), set by import and upsert and cleared by any commit:
    /// upsert sends only the pages that differ from that file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synced: Option<String>,
    /// Objects no longer named (older manifests kept them here; now in gc.json).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retired: Vec<Retired>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Segment {
    /// The version whose commit wrote it.
    #[serde(default)]
    pub version: u64,
    pub key: String,
    /// The page numbers it holds (1-based, as SQLite counts), in its order.
    pub pages: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Retired {
    pub key: String,
    /// Unix seconds when it stopped being named.
    pub at: i64,
}

/// Where a page's bytes are: an object and an offset in it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Loc {
    obj: Arc<str>,
    off: u64,
}

/// One version of the database, as a transaction sees it from start to end.
#[derive(Debug)]
pub struct Snapshot {
    pub manifest: Manifest,
    pub etag: String,
    /// page -> (segment key, position), the newest segment's.
    in_segments: HashMap<u64, (Arc<str>, u32)>,
}

impl Snapshot {
    fn new(mut manifest: Manifest, etag: String) -> Snapshot {
        // Segments written before they carried a version have it in their key.
        for seg in &mut manifest.segments {
            if seg.version == 0 {
                seg.version = seg
                    .key
                    .rsplit('/')
                    .next()
                    .and_then(|k| k.split('-').next())
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
            }
        }
        let mut in_segments = HashMap::new();
        for seg in &manifest.segments {
            let key: Arc<str> = seg.key.as_str().into();
            for (i, &p) in seg.pages.iter().enumerate() {
                in_segments.insert(p, (key.clone(), i as u32));
            }
        }
        Snapshot {
            manifest,
            etag,
            in_segments,
        }
    }

    /// A version of the past (past.rs): a manifest the bucket kept, read as it was.
    pub fn pinned(manifest: Manifest, version_id: String) -> Snapshot {
        Snapshot::new(manifest, version_id)
    }

    pub fn page_size(&self) -> u64 {
        u64::from(self.manifest.page_size)
    }

    pub fn file_size(&self) -> u64 {
        self.manifest.page_count * self.page_size()
    }

    pub fn chunk_key(&self, chunk: u64) -> String {
        let prefix = self.manifest.chunks.get(&chunk).unwrap_or(&self.manifest.base);
        format!("{prefix}/{chunk:08}")
    }
}

#[derive(Debug, Clone)]
pub struct Options {
    pub cache_bytes: usize,
    /// Pages fetched together from the base, around the one asked for.
    pub readahead: u64,
    /// How old a manifest a new transaction may start from.
    pub staleness: Duration,
    /// A second cache tier on local disk (Lambda's /tmp), when set.
    pub disk_cache: Option<(PathBuf, u64)>,
    /// Compact once the manifest lists this many segments.
    pub compact_segments: usize,
    /// How long a retired object is kept for transactions still reading it.
    pub grace: Duration,
    /// The most a transaction may write before commit (its pages wait in memory).
    pub max_txn_bytes: u64,
}

impl Options {
    /// From the environment. In Lambda the defaults follow the function: a quarter of
    /// its memory for the page cache, another quarter for a transaction's writes, and
    /// half of /tmp's free space for the disk cache.
    pub fn from_env() -> Options {
        let num = |k: &str, d: u64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
        let lambda_mb = crate::lambda_memory_mb();
        let quarter = lambda_mb.map(|m| (m / 4).max(16));
        let disk_dir = std::env::var("BUCKETDB_DISK_CACHE_DIR")
            .ok()
            .or_else(|| lambda_mb.map(|_| "/tmp/bucketdb/cache".into()))
            .filter(|d| !d.is_empty() && d != "off");
        let disk_cache = disk_dir.map(|d| {
            let free_mb = lambda_mb.and_then(|_| free_mb("/tmp")).map(|f| f / 2);
            (
                PathBuf::from(d),
                num("BUCKETDB_DISK_CACHE_MB", free_mb.unwrap_or(1024)) << 20,
            )
        });
        Options {
            cache_bytes: (num("BUCKETDB_CACHE_MB", quarter.unwrap_or(64)) as usize) << 20,
            readahead: num("BUCKETDB_READAHEAD", 16).max(1),
            staleness: Duration::from_millis(num("BUCKETDB_STALENESS_MS", 1000)),
            disk_cache,
            compact_segments: num("BUCKETDB_COMPACT_SEGMENTS", 100).max(1) as usize,
            grace: Duration::from_secs(num("BUCKETDB_GRACE_SECONDS", 600)),
            max_txn_bytes: num("BUCKETDB_MAX_TXN_MB", quarter.unwrap_or(256)) << 20,
        }
    }

    pub fn max_txn_bytes(&self) -> u64 {
        self.max_txn_bytes
    }
}

/// Free space on the file system holding path, in MB.
fn free_mb(path: &str) -> Option<u64> {
    let c = std::ffi::CString::new(path).ok()?;
    // SAFETY: statvfs is plain old data, valid when zeroed.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: c is a valid C string and st a struct statvfs fills.
    if unsafe { libc::statvfs(c.as_ptr(), &raw mut st) } != 0 {
        return None;
    }
    Some((st.f_bavail as u64 * st.f_frsize as u64) >> 20)
}

#[derive(Debug)]
struct Current {
    snap: Arc<Snapshot>,
    checked: Instant,
}

/// A database a sidecar serves: shared by every connection to it.
#[derive(Debug)]
pub struct RemoteDb {
    pub name: String,
    pub s3: Arc<Client>,
    pub opts: Options,
    current: Mutex<Current>,
    cache: Cache,
    disk: Option<DiskCache>,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub commits: AtomicU64,
    pub rebased: AtomicU64,
    pub conflicts: AtomicU64,
    pub compactions: AtomicU64,
    seq: AtomicU64,
    /// Set when a commit leaves enough segments to fold; the compactor waits on it.
    wake: (Mutex<bool>, std::sync::Condvar),
    /// A version of the past, served as it was: read only, never newer, its objects
    /// read from the bucket's kept versions once deleted (past.rs).
    pub pinned: bool,
}

pub fn manifest_key(db: &str) -> String {
    format!("{db}/manifest.json")
}

/// The objects compactions replaced, waiting out the grace period: written only by
/// a compaction that won, so commits never carry them.
fn gc_key(db: &str) -> String {
    format!("{db}/gc.json")
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub fn now_unix() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Bytes of page 1 every commit rewrites: the file change counter (24..28) and the
/// version-valid-for number (92..96).
pub const COUNTERS: [std::ops::Range<usize>; 2] = [24..28, 92..96];
/// Header fields a concurrent commit may change under another: the page count (28..32)
/// and the freelist (32..40). The schema cookie and the rest of page 1 may not.
const SIZE_AND_FREELIST: std::ops::Range<usize> = 28..40;

/// Whether a and b differ outside the given ranges.
fn differs_outside(a: &[u8], b: &[u8], skip: &[std::ops::Range<usize>]) -> bool {
    a.iter()
        .zip(b)
        .enumerate()
        .any(|(i, (x, y))| x != y && !skip.iter().any(|r| r.contains(&i)))
}

pub fn page1_equal(a: &[u8], b: &[u8]) -> bool {
    !differs_outside(a, b, &COUNTERS)
}

impl RemoteDb {
    /// See the module documentation.
    ///
    /// # Errors
    ///
    /// When the database does not exist, or on S3 errors.
    pub fn open(name: &str, s3: Arc<Client>, opts: Options) -> io::Result<RemoteDb> {
        let snap = fetch_manifest(&s3, name)?
            .ok_or_else(|| io::Error::other(format!("no database {name:?} in bucket {:?}", s3.bucket())))?;
        RemoteDb::with(name, s3, opts, snap, false)
    }

    /// A version of the past, served read only as it was (see [`crate::past::at`]).
    ///
    /// # Errors
    ///
    /// When the disk cache cannot be made.
    pub fn open_pinned(name: &str, s3: Arc<Client>, opts: Options, snap: Snapshot) -> io::Result<RemoteDb> {
        RemoteDb::with(name, s3, opts, snap, true)
    }

    fn with(name: &str, s3: Arc<Client>, opts: Options, snap: Snapshot, pinned: bool) -> io::Result<RemoteDb> {
        let disk = match &opts.disk_cache {
            Some((dir, cap)) => Some(DiskCache::new(dir.join(name), *cap)?),
            None => None,
        };
        Ok(RemoteDb {
            name: name.to_string(),
            s3,
            cache: Cache::new(opts.cache_bytes),
            disk,
            opts,
            current: Mutex::new(Current {
                snap: Arc::new(snap),
                checked: Instant::now(),
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            commits: AtomicU64::new(0),
            rebased: AtomicU64::new(0),
            conflicts: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
            seq: AtomicU64::new(0),
            wake: (Mutex::new(false), std::sync::Condvar::new()),
            pinned,
        })
    }

    /// The version a new transaction starts from: the newest known, checked with the
    /// bucket when older than the staleness allowed (a 304 when nothing changed).
    ///
    /// # Errors
    ///
    /// On S3 errors while checking for a newer manifest.
    pub fn snapshot(&self) -> io::Result<Arc<Snapshot>> {
        let (snap, fresh) = {
            let cur = lock(&self.current);
            (cur.snap.clone(), cur.checked.elapsed() <= self.opts.staleness)
        };
        if fresh || self.pinned {
            return Ok(snap);
        }
        self.refresh(&snap)
    }

    /// Asks the bucket for a manifest newer than seen.
    ///
    /// # Errors
    ///
    /// On S3 errors.
    pub fn refresh(&self, seen: &Arc<Snapshot>) -> io::Result<Arc<Snapshot>> {
        let got = self.s3.get(&manifest_key(&self.name), None, Some(&seen.etag))?;
        let mut cur = lock(&self.current);
        cur.checked = Instant::now();
        if let Get::Found { body, etag } = got {
            let manifest: Manifest = serde_json::from_slice(&body).map_err(io::Error::other)?;
            if manifest.version > cur.snap.manifest.version {
                cur.snap = Arc::new(Snapshot::new(manifest, etag));
            }
        }
        Ok(cur.snap.clone())
    }

    fn adopt(&self, snap: &Arc<Snapshot>) {
        let mut cur = lock(&self.current);
        if snap.manifest.version > cur.snap.manifest.version {
            cur.snap = snap.clone();
            cur.checked = Instant::now();
        }
    }

    /// Makes the next snapshot ask the bucket.
    fn stale(&self) {
        let mut cur = lock(&self.current);
        cur.checked = Instant::now()
            .checked_sub(self.opts.staleness + Duration::from_secs(1))
            .unwrap_or(cur.checked);
    }

    fn cached(&self, loc: &Loc, len: usize) -> Option<Arc<[u8]>> {
        if let Some(p) = self.cache.get(loc) {
            return Some(p);
        }
        let p: Arc<[u8]> = self.disk.as_ref()?.get(loc, len)?.into();
        self.cache.put(loc.clone(), p.clone());
        Some(p)
    }

    /// Keeps a run of pages of one object, starting at off, in memory and on disk.
    fn keep(&self, obj: &Arc<str>, off: u64, bytes: &[u8], ps: usize) {
        if let Some(d) = &self.disk {
            d.put(obj, off, bytes, ps);
        }
        for (i, page) in bytes.chunks(ps).enumerate() {
            self.cache.put(
                Loc {
                    obj: obj.clone(),
                    off: off + (i * ps) as u64,
                },
                page.into(),
            );
        }
    }

    /// Copies page pgno of snap into out (page_size bytes).
    ///
    /// # Errors
    ///
    /// When the object holding the page is missing, or on S3 errors.
    pub fn read_page(&self, snap: &Snapshot, pgno: u64, out: &mut [u8]) -> io::Result<()> {
        let ps = snap.page_size();
        if pgno == 0 || pgno > snap.manifest.page_count {
            out.fill(0);
            return Ok(());
        }
        let seg = snap.in_segments.get(&pgno);
        if seg.is_none() && pgno > snap.manifest.base_pages {
            out.fill(0);
            return Ok(());
        }
        let cp = u64::from(snap.manifest.chunk_pages);
        let idx = pgno - 1;
        let loc = match seg {
            Some((key, pos)) => Loc {
                obj: key.clone(),
                off: u64::from(*pos) * ps,
            },
            None => Loc {
                obj: snap.chunk_key(idx / cp).into(),
                off: (idx % cp) * ps,
            },
        };
        if let Some(page) = self.cached(&loc, ps as usize) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            out.copy_from_slice(&page);
            return Ok(());
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        if seg.is_some() {
            let body = self.fetch(&loc.obj, loc.off, ps)?;
            out.copy_from_slice(&body[..ps as usize]);
            self.keep(&loc.obj, loc.off, &body[..ps as usize], ps as usize);
            return Ok(());
        }
        // From a chunk: a run of pages around it, within the chunk.
        let in_chunk = idx % cp;
        let first = in_chunk - in_chunk % self.opts.readahead.min(cp);
        let chunk_first_idx = idx - in_chunk;
        let last = (first + self.opts.readahead)
            .min(cp)
            .min(snap.manifest.base_pages - chunk_first_idx);
        let body = self.fetch(&loc.obj, first * ps, (last - first) * ps)?;
        let at = ((in_chunk - first) * ps) as usize;
        out.copy_from_slice(&body[at..at + ps as usize]);
        self.keep(&loc.obj, first * ps, &body, ps as usize);
        Ok(())
    }

    fn fetch(&self, key: &str, off: u64, len: u64) -> io::Result<Vec<u8>> {
        if self.pinned {
            let body = crate::past::get_any(&self.s3, key, Some((off, len)))?;
            if (body.len() as u64) < len {
                return Err(io::Error::other(format!(
                    "{key}: {} bytes at {off}, wanted {len}",
                    body.len()
                )));
            }
            return Ok(body);
        }
        match self.s3.get(key, Some((off, len)), None)? {
            Get::Found { body, .. } if body.len() as u64 >= len => Ok(body),
            Get::Found { body, .. } => Err(io::Error::other(format!(
                "{key}: {} bytes at {off}, wanted {len}",
                body.len()
            ))),
            _ => Err(io::Error::other(format!("{key}: missing"))),
        }
    }

    fn write_segment(&self, version: u64, pages: &BTreeMap<u64, Vec<u8>>) -> io::Result<Segment> {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        let key = format!("{}/seg/{version:012}-{}-{n}", self.name, std::process::id());
        let mut body = Vec::with_capacity(pages.values().map(Vec::len).sum());
        for p in pages.values() {
            body.extend_from_slice(p);
        }
        self.s3.put(&key, &body, Cond::None)?;
        Ok(Segment {
            version,
            key,
            pages: pages.keys().copied().collect(),
        })
    }

    fn put_manifest(&self, manifest: &Manifest, over: &str) -> io::Result<Option<Arc<Snapshot>>> {
        let json = serde_json::to_vec(manifest).map_err(io::Error::other)?;
        Ok(
            match self.s3.put(&manifest_key(&self.name), &json, Cond::IfMatch(over))? {
                Put::PreconditionFailed => None,
                Put::Done { etag } => Some(Arc::new(Snapshot::new(manifest.clone(), etag))),
            },
        )
    }

    /// Commits a transaction's pages over base. reads are the pages it read from base.
    /// Ok(None) when it lost to a commit it cannot be replayed over.
    ///
    /// # Errors
    ///
    /// On S3 errors; a lost race is `Ok(None)`, not an error.
    pub fn commit(
        &self,
        base: &Arc<Snapshot>,
        dirty: &BTreeMap<u64, Vec<u8>>,
        reads: Option<&HashSet<u64>>,
        page_count: u64,
    ) -> io::Result<Option<Arc<Snapshot>>> {
        if self.pinned {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "a version of the past is read only",
            ));
        }
        let pages: BTreeMap<u64, Vec<u8>> = dirty
            .iter()
            .filter(|(p, _)| **p <= page_count)
            .map(|(p, b)| (*p, b.clone()))
            .collect();
        let mut over = base.clone();
        let mut pages = pages;
        let mut page_count = page_count;
        for attempt in 0..16 {
            let seg = self.write_segment(over.manifest.version + 1, &pages)?;
            let mut manifest = over.manifest.clone();
            manifest.version += 1;
            manifest.page_count = page_count;
            manifest.segments.push(seg.clone());
            manifest.synced = None;
            if let Some(snap) = self.put_manifest(&manifest, &over.etag)? {
                self.commits.fetch_add(1, Ordering::Relaxed);
                if attempt > 0 {
                    self.rebased.fetch_add(1, Ordering::Relaxed);
                }
                let obj: Arc<str> = seg.key.as_str().into();
                let ps = base.page_size() as usize;
                let body: Vec<u8> = pages.values().flatten().copied().collect();
                self.keep(&obj, 0, &body, ps);
                self.adopt(&snap);
                if snap.manifest.segments.len() >= self.opts.compact_segments {
                    *lock(&self.wake.0) = true;
                    self.wake.1.notify_one();
                }
                return Ok(Some(snap));
            }
            let _ = self.s3.delete(&seg.key);
            // Lost the race: replay over the newest version, if nothing overlaps (and
            // the pages the transaction read are known).
            let Some(reads) = reads else {
                self.conflicts.fetch_add(1, Ordering::Relaxed);
                self.stale();
                return Ok(None);
            };
            let newest = match fetch_manifest(&self.s3, &self.name)? {
                Some(s) => Arc::new(s),
                None => return Err(io::Error::other("the manifest is gone")),
            };
            if let Some((p, n)) = self.replay(base, &newest, &pages, reads, page_count)? {
                pages = p;
                page_count = n;
                over = newest;
            } else {
                self.conflicts.fetch_add(1, Ordering::Relaxed);
                self.adopt(&newest);
                self.stale();
                return Ok(None);
            }
        }
        self.conflicts.fetch_add(1, Ordering::Relaxed);
        self.stale();
        Ok(None)
    }

    /// The transaction's pages rebased from base onto newest, or None when the
    /// commits since base wrote a page it read or wrote, changed the schema, or were
    /// folded by a compaction.
    #[allow(clippy::type_complexity)]
    fn replay(
        &self,
        base: &Snapshot,
        newest: &Snapshot,
        pages: &BTreeMap<u64, Vec<u8>>,
        reads: &HashSet<u64>,
        page_count: u64,
    ) -> io::Result<Option<(BTreeMap<u64, Vec<u8>>, u64)>> {
        let (b, n) = (&base.manifest, &newest.manifest);
        // A compaction changes where pages are, not what they hold: it is no conflict
        // when it folded only versions the transaction had already seen. The commits
        // since then are the newest's segments past base's version.
        if n.base != b.base || (n.folded > b.version && n.recent_after > b.version) {
            return Ok(None); // a compaction folded commits this one never saw listed
        }
        let theirs: HashSet<u64> = n
            .segments
            .iter()
            .chain(n.recent.iter())
            .filter(|s| s.version > b.version)
            .flat_map(|s| s.pages.iter().copied())
            .collect();
        let touched = reads.iter().chain(pages.keys()).filter(|&&p| p != 1);
        if touched.into_iter().any(|p| theirs.contains(p)) {
            return Ok(None);
        }
        let ours_grew = page_count != b.page_count;
        if ours_grew && n.page_count != b.page_count {
            return Ok(None); // both changed the file's size
        }
        let count = if ours_grew { page_count } else { n.page_count };
        let ps = base.page_size() as usize;
        let mut out = pages.clone();
        if theirs.contains(&1) || pages.contains_key(&1) {
            let mut base1 = vec![0u8; ps];
            let mut their1 = vec![0u8; ps];
            self.read_page(base, 1, &mut base1)?;
            self.read_page(newest, 1, &mut their1)?;
            let our1 = pages.get(&1).cloned().unwrap_or_else(|| base1.clone());
            let ours_changed = differs_outside(&base1, &our1, &COUNTERS);
            let mut skip = COUNTERS.to_vec();
            skip.push(SIZE_AND_FREELIST);
            if differs_outside(&base1, &their1, &skip) {
                return Ok(None); // their schema or the root page changed under us
            }
            if ours_changed && differs_outside(&base1, &their1, &COUNTERS) {
                return Ok(None); // both changed the header beyond the counters
            }
            let mut merged = if ours_changed { our1 } else { their1.clone() };
            let counter = u32::from_be_bytes([their1[24], their1[25], their1[26], their1[27]]).wrapping_add(1);
            for r in COUNTERS {
                merged[r].copy_from_slice(&counter.to_be_bytes());
            }
            merged[28..32].copy_from_slice(&(count as u32).to_be_bytes());
            out.insert(1, merged);
        }
        Ok(Some((out, count)))
    }

    /// Whether the manifest lists enough segments to fold.
    pub fn wants_compaction(&self) -> bool {
        !self.pinned && lock(&self.current).snap.manifest.segments.len() >= self.opts.compact_segments
    }

    /// Waits until a commit asks for a compaction, or the timeout passes (another
    /// sidecar's commits are seen only by looking).
    pub fn wait_for_compaction(&self, timeout: Duration) {
        let mut woken = lock(&self.wake.0);
        if !*woken {
            woken = self
                .wake
                .1
                .wait_timeout(woken, timeout)
                .map_or_else(|e| e.into_inner().0, |(g, _)| g);
        }
        *woken = false;
    }

    /// Folds the segments into rewritten chunks and deletes what was retired long
    /// enough ago. Ok(None) when there was nothing to fold, or the race was lost.
    ///
    /// # Errors
    ///
    /// On S3 errors; a lost race is `Ok(None)`.
    pub fn compact(&self) -> io::Result<Option<Arc<Snapshot>>> {
        let started = Instant::now();
        let current = lock(&self.current).snap.clone();
        let snap = self.refresh(&current)?;
        let m = &snap.manifest;
        if m.segments.is_empty() {
            return Ok(None);
        }
        let ps = snap.page_size();
        let cp = u64::from(m.chunk_pages);
        let live: BTreeSet<u64> = snap
            .in_segments
            .keys()
            .copied()
            .filter(|&p| p <= m.page_count)
            .collect();
        let mut chunks: BTreeSet<u64> = live.iter().map(|p| (p - 1) / cp).collect();
        // Chunks past the old base the file grew into, even where no segment wrote.
        if m.page_count > m.base_pages {
            chunks.extend(m.base_pages / cp..m.page_count.div_ceil(cp));
        }
        // Unique per process and attempt: a compaction that loses deletes only its own.
        let prefix = format!(
            "{}/base/{}-v{}-{}-{}",
            self.name,
            chrono::Utc::now().format("%Y%m%dT%H%M%S"),
            m.version,
            std::process::id(),
            self.seq.fetch_add(1, Ordering::Relaxed)
        );

        // Each folded segment is read whole, once.
        let mut segs: HashMap<Arc<str>, Vec<u8>> = HashMap::new();
        for (key, _) in snap.in_segments.values() {
            if !segs.contains_key(key) {
                let Get::Found { body, .. } = self.s3.get(key, None, None)? else {
                    return Err(io::Error::other(format!("{key}: missing")));
                };
                segs.insert(key.clone(), body);
            }
        }
        let next = AtomicU64::new(0);
        let list: Vec<u64> = chunks.iter().copied().collect();
        std::thread::scope(|scope| -> io::Result<()> {
            let mut handles = Vec::new();
            for _ in 0..8.min(list.len()) {
                handles.push(scope.spawn(|| -> io::Result<()> {
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed) as usize;
                        let Some(&chunk) = list.get(i) else { return Ok(()) };
                        let first_idx = chunk * cp;
                        let n = cp.min(m.page_count - first_idx);
                        let mut buf = vec![0u8; (n * ps) as usize];
                        if first_idx < m.base_pages {
                            let have = n.min(m.base_pages - first_idx);
                            let body = self.fetch(&snap.chunk_key(chunk), 0, have * ps)?;
                            buf[..(have * ps) as usize].copy_from_slice(&body[..(have * ps) as usize]);
                        }
                        for j in 0..n {
                            if let Some((key, pos)) = snap.in_segments.get(&(first_idx + j + 1)) {
                                let s = &segs[key];
                                let at = u64::from(*pos) * ps;
                                let dst = (j * ps) as usize;
                                buf[dst..dst + ps as usize].copy_from_slice(&s[at as usize..(at + ps) as usize]);
                            }
                        }
                        self.s3.put(&format!("{prefix}/{chunk:08}"), &buf, Cond::None)?;
                    }
                }));
            }
            for h in handles {
                h.join().map_err(|_| io::Error::other("compaction thread panicked"))??;
            }
            Ok(())
        })?;

        let now = now_unix();
        let mut retired: Vec<Retired> = m.retired.clone();
        for seg in &m.segments {
            retired.push(Retired {
                key: seg.key.clone(),
                at: now,
            });
        }
        for &chunk in &chunks {
            if chunk * cp < m.base_pages {
                retired.push(Retired {
                    key: snap.chunk_key(chunk),
                    at: now,
                });
            }
        }
        let mut manifest = m.clone();
        manifest.version += 1;
        manifest.base_pages = m.page_count;
        for &chunk in &chunks {
            manifest.chunks.insert(chunk, prefix.clone());
        }
        manifest.chunks.retain(|&c, _| c * cp < m.page_count);
        manifest.folded = m
            .segments
            .iter()
            .map(|s| s.version)
            .max()
            .unwrap_or(m.folded)
            .max(m.folded);
        manifest.recent_after = m.folded;
        manifest.recent = m
            .segments
            .iter()
            .map(|s| Segment {
                version: s.version,
                key: String::new(),
                pages: s.pages.clone(),
            })
            .collect();
        manifest.segments.clear();
        manifest.retired = Vec::new();
        let mut over = snap.clone();
        // A commit that lands meanwhile only appends segments: they are kept, and they
        // still win over the rewritten chunks.
        for _ in 0..8 {
            if let Some(new) = self.put_manifest(&manifest, &over.etag)? {
                self.compactions.fetch_add(1, Ordering::Relaxed);
                self.adopt(&new);
                if let Err(e) = self.collect_garbage(retired) {
                    log::warn!("bucketdb: {} gc: {e}", self.name);
                }
                log::info!(
                    "bucketdb: {} compacted to v{}: {} segments into {} chunks in {:.1}s",
                    self.name,
                    new.manifest.version,
                    m.segments.len(),
                    chunks.len(),
                    started.elapsed().as_secs_f64()
                );
                return Ok(Some(new));
            }
            let newest = match fetch_manifest(&self.s3, &self.name)? {
                Some(s) => Arc::new(s),
                None => return Err(io::Error::other("the manifest is gone")),
            };
            let n = &newest.manifest;
            if n.base != m.base
                || n.chunks != m.chunks
                || n.segments.len() < m.segments.len()
                || n.segments[..m.segments.len()] != m.segments[..]
            {
                break; // another compaction won
            }
            manifest.version = n.version + 1;
            manifest.page_count = n.page_count;
            manifest.segments = n.segments[m.segments.len()..].to_vec();
            manifest.synced.clone_from(&n.synced);
            over = newest;
        }
        for &chunk in &chunks {
            let _ = self.s3.delete(&format!("{prefix}/{chunk:08}"));
        }
        Ok(None)
    }
}

impl RemoteDb {
    /// Logs its cache and commit counters.
    pub fn log_stats(&self) {
        log::info!(
            "{}: cache {} hits / {} misses, {} commits ({} replayed), {} conflicts, {} compactions",
            self.name,
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.commits.load(Ordering::Relaxed),
            self.rebased.load(Ordering::Relaxed),
            self.conflicts.load(Ordering::Relaxed),
            self.compactions.load(Ordering::Relaxed)
        );
    }

    /// Adds what a compaction retired to gc.json and deletes what has waited out the
    /// grace period. Runs only after the compaction's manifest won.
    fn collect_garbage(&self, add: Vec<Retired>) -> io::Result<()> {
        collect_garbage(&self.s3, &self.name, self.opts.grace, &add)
    }
}

/// Adds objects no manifest names any more to db's gc.json, and deletes those that
/// have waited out the grace period (a transaction may still be reading them).
///
/// # Errors
///
/// On S3 errors, or when gc.json keeps changing under it.
pub fn collect_garbage(s3: &Client, db: &str, grace: Duration, add: &[Retired]) -> io::Result<()> {
    let key = gc_key(db);
    for _ in 0..4 {
        let (mut list, etag): (Vec<Retired>, Option<String>) = match s3.get(&key, None, None)? {
            Get::Found { body, etag } => (serde_json::from_slice(&body).unwrap_or_default(), Some(etag)),
            _ => (Vec::new(), None),
        };
        list.extend(add.iter().cloned());
        let now = now_unix();
        let grace = grace.as_secs() as i64;
        let (expired, keep): (Vec<Retired>, Vec<Retired>) = list.into_iter().partition(|r| now - r.at >= grace);
        let json = serde_json::to_vec(&keep).map_err(io::Error::other)?;
        let cond = etag.as_deref().map_or(Cond::IfNoneMatch, Cond::IfMatch);
        if let Put::Done { .. } = s3.put(&key, &json, cond)? {
            for r in expired {
                let _ = s3.delete(&r.key);
            }
            return Ok(());
        }
    }
    Err(io::Error::other("gc.json kept changing"))
}

/// Every object a version names: its chunks and its segments.
pub fn objects(snap: &Snapshot) -> Vec<String> {
    let m = &snap.manifest;
    let cp = u64::from(m.chunk_pages).max(1);
    let mut keys: Vec<String> = (0..m.base_pages.div_ceil(cp)).map(|c| snap.chunk_key(c)).collect();
    keys.extend(m.segments.iter().map(|s| s.key.clone()));
    keys
}

/// The current manifest of db, or None when there is no such database.
///
/// # Errors
///
/// On S3 errors, or a manifest that does not parse.
pub fn fetch_manifest(s3: &Client, db: &str) -> io::Result<Option<Snapshot>> {
    match s3.get(&manifest_key(db), None, None)? {
        Get::Found { body, etag } => {
            let manifest: Manifest = serde_json::from_slice(&body).map_err(io::Error::other)?;
            Ok(Some(Snapshot::new(manifest, etag)))
        }
        Get::NotFound => Ok(None),
        Get::NotModified => Err(io::Error::other("304 without If-None-Match")),
    }
}

/// Writes the first manifest; fails when the database exists.
///
/// # Errors
///
/// When the database exists, or on S3 errors.
pub fn create(s3: &s3::Client, db: &str, manifest: &Manifest) -> io::Result<()> {
    let json = serde_json::to_vec(manifest).map_err(io::Error::other)?;
    match s3.put(&manifest_key(db), &json, Cond::IfNoneMatch)? {
        Put::Done { .. } => Ok(()),
        Put::PreconditionFailed => Err(io::Error::other(format!("database {db:?} already exists"))),
    }
}

/// Pages in memory, bounded in bytes: 16 shards, each with its own lock and a CLOCK
/// (second-chance) eviction, O(1) per page.
#[derive(Debug)]
struct Cache {
    shards: Vec<Mutex<Shard>>,
}

#[derive(Debug)]
struct Shard {
    cap: usize,
    used: usize,
    hand: usize,
    map: HashMap<Loc, usize>,
    slots: Vec<(Loc, Arc<[u8]>, bool)>,
}

impl Cache {
    fn new(cap: usize) -> Cache {
        let per = (cap / 16).max(1 << 16);
        let shards = (0..16)
            .map(|_| {
                Mutex::new(Shard {
                    cap: per,
                    used: 0,
                    hand: 0,
                    map: HashMap::new(),
                    slots: Vec::new(),
                })
            })
            .collect();
        Cache { shards }
    }

    fn shard(&self, loc: &Loc) -> MutexGuard<'_, Shard> {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        loc.hash(&mut h);
        lock(&self.shards[(h.finish() % 16) as usize])
    }

    fn get(&self, loc: &Loc) -> Option<Arc<[u8]>> {
        let mut s = self.shard(loc);
        let i = *s.map.get(loc)?;
        s.slots[i].2 = true;
        Some(s.slots[i].1.clone())
    }

    fn put(&self, loc: Loc, page: Arc<[u8]>) {
        let mut s = self.shard(&loc);
        if let Some(&i) = s.map.get(&loc) {
            s.slots[i].1 = page;
            s.slots[i].2 = true;
            return;
        }
        let len = page.len();
        if s.used + len <= s.cap || s.slots.is_empty() {
            let i = s.slots.len();
            s.slots.push((loc.clone(), page, false));
            s.map.insert(loc, i);
            s.used += len;
            return;
        }
        // CLOCK: the hand clears referenced bits until it finds a page that had none.
        loop {
            let h = s.hand % s.slots.len();
            s.hand = h + 1;
            if s.slots[h].2 {
                s.slots[h].2 = false;
                continue;
            }
            let old = std::mem::replace(&mut s.slots[h], (loc.clone(), page, false));
            s.map.remove(&old.0);
            s.map.insert(loc, h);
            s.used = s.used - old.1.len() + len;
            return;
        }
    }
}

/// Pages on local disk: one sparse file per bucket object, pages written at their
/// offsets, which ones are there kept in memory. The directory is emptied at start:
/// it is a cache of this process (as Lambda's /tmp is of its environment). Past the
/// limit, the objects written to first go whole.
#[derive(Debug)]
struct DiskCache {
    dir: PathBuf,
    cap: u64,
    state: Mutex<DiskState>,
}

#[derive(Debug, Default)]
struct DiskState {
    used: u64,
    have: HashMap<Arc<str>, HashSet<u64>>,
    order: std::collections::VecDeque<Arc<str>>,
}

impl DiskCache {
    fn new(dir: PathBuf, cap: u64) -> io::Result<DiskCache> {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        Ok(DiskCache {
            dir,
            cap,
            state: Mutex::new(DiskState::default()),
        })
    }

    fn path(&self, obj: &str) -> PathBuf {
        let name: String = obj.chars().map(|c| if c == '/' { '~' } else { c }).collect();
        self.dir.join(name)
    }

    fn get(&self, loc: &Loc, len: usize) -> Option<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        if !lock(&self.state).have.get(&loc.obj)?.contains(&loc.off) {
            return None;
        }
        let f = std::fs::File::open(self.path(&loc.obj)).ok()?;
        let mut buf = vec![0u8; len];
        f.read_exact_at(&mut buf, loc.off).ok()?;
        Some(buf)
    }

    /// Writes a run of pages of one object, starting at off.
    fn put(&self, obj: &Arc<str>, off: u64, bytes: &[u8], page: usize) {
        use std::os::unix::fs::FileExt;
        let ok = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(self.path(obj))
            .and_then(|f| f.write_all_at(bytes, off))
            .is_ok();
        if !ok {
            return;
        }
        let mut st = lock(&self.state);
        let fresh = !st.have.contains_key(obj);
        let set = st.have.entry(obj.clone()).or_default();
        let mut added = 0;
        for i in 0..bytes.len() / page {
            if set.insert(off + (i * page) as u64) {
                added += page as u64;
            }
        }
        st.used += added;
        if fresh {
            st.order.push_back(obj.clone());
        }
        while st.used > self.cap {
            let Some(victim) = st.order.pop_front() else { break };
            if let Some(pages) = st.have.remove(&victim) {
                st.used -= pages.len() as u64 * page as u64;
            }
            let _ = std::fs::remove_file(self.path(&victim));
        }
    }
}
