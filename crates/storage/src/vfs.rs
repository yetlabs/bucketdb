//! The SQLite VFS that puts a database's main file in a bucket.
//!
//! It becomes SQLite's default VFS and hands every file to the one it replaced,
//! except the main file of a database being served: that one reads its pages from
//! the bucket (through RemoteDb's cache) and commits a transaction as one batch.
//!
//! The VFS claims SQLITE_IOCAP_BATCH_ATOMIC, so SQLite (built with
//! SQLITE_ENABLE_BATCH_ATOMIC_WRITE) keeps the rollback journal in memory and writes
//! a commit's pages between BEGIN_ATOMIC_WRITE and COMMIT_ATOMIC_WRITE. Those writes
//! are held per connection; COMMIT_ATOMIC_WRITE sends them as a segment and swaps the
//! manifest. A lost race answers SQLITE_BUSY_SNAPSHOT: the transaction read a version
//! that is no longer the newest, and must run again.
//!
//! Locks are this process's: a transaction reads the snapshot taken when it got its
//! SHARED lock, so readers never wait for a writer, and one connection at a time may
//! hold RESERVED. Across processes there are no locks, only the manifest's If-Match.

use crate::store::{RemoteDb, Snapshot};
use rusqlite::ffi;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

struct Entry {
    db: Arc<RemoteDb>,
    /// The connection (file id) that holds RESERVED or more.
    writer: Mutex<Option<u64>>,
}

static REMOTES: OnceLock<HashMap<String, Arc<Entry>>> = OnceLock::new();
static PARENT: OnceLock<usize> = OnceLock::new();
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
/// Each transaction's own change counter (see RemoteFile::epoch). It moves by two: a
/// transaction that commits leaves SQLite holding the counter it was shown plus one,
/// and the next transaction's must differ from that too.
static EPOCH: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
const EPOCH_STEP: u32 = 2;

/// Whether transactions track the pages they read (BUCKETDB_REPLAY, on by default),
/// so a lost commit race can be replayed. It costs SQLite its page cache at every
/// transaction; off, SQLite keeps it and a lost race is always a 40001.
pub fn tracking() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("BUCKETDB_REPLAY").map_or(true, |v| v != "0" && v != "off"))
}

fn parent() -> *mut ffi::sqlite3_vfs {
    *PARENT.get().unwrap_or(&0) as *mut ffi::sqlite3_vfs
}

/// Registers the VFS as the default; paths are the canonical paths of the main files
/// (an empty placeholder each) that are to live in the bucket.
///
/// # Errors
///
/// When SQLite refuses the VFS, or it was registered already.
pub fn register(dbs: Vec<(String, Arc<RemoteDb>)>) -> Result<(), String> {
    let map = dbs
        .into_iter()
        .map(|(path, db)| {
            (
                path,
                Arc::new(Entry {
                    db,
                    writer: Mutex::new(None),
                }),
            )
        })
        .collect();
    REMOTES
        .set(map)
        .map_err(|_| "the bucketdb VFS is registered once".to_string())?;
    // SAFETY: SQLite is initialized before its VFS list is read; the parent VFS lives
    // as long as the process, and the struct handed to SQLite is leaked so it does too.
    unsafe {
        ffi::sqlite3_initialize();
        let p = ffi::sqlite3_vfs_find(std::ptr::null());
        if p.is_null() {
            return Err("no default SQLite VFS".into());
        }
        let _ = PARENT.set(p as usize);
        let mut vfs = *p;
        vfs.zName = CString::new("bucketdb").unwrap_or_default().into_raw();
        vfs.pNext = std::ptr::null_mut();
        vfs.szOsFile = vfs.szOsFile.max(size_of::<ZFile>() as c_int);
        vfs.xOpen = Some(x_open);
        vfs.xDelete = Some(x_delete);
        vfs.xAccess = Some(x_access);
        let rc = ffi::sqlite3_vfs_register(Box::into_raw(Box::new(vfs)), 1);
        if rc != ffi::SQLITE_OK {
            return Err(format!("sqlite3_vfs_register: {rc}"));
        }
    }
    Ok(())
}

fn remote(name: *const c_char) -> Option<Arc<Entry>> {
    if name.is_null() {
        return None;
    }
    // SAFETY: SQLite passes a NUL-terminated path.
    let path = unsafe { CStr::from_ptr(name) }.to_str().ok()?;
    REMOTES.get()?.get(path).cloned()
}

#[repr(C)]
struct ZFile {
    base: ffi::sqlite3_file,
    file: *mut RemoteFile,
}

struct RemoteFile {
    id: u64,
    entry: Arc<Entry>,
    snap: Option<Arc<Snapshot>>,
    level: c_int,
    /// Pages written in this transaction, not yet committed.
    dirty: BTreeMap<u64, Vec<u8>>,
    /// The size the transaction gave the file, when it changed it.
    size: Option<u64>,
    batch: bool,
    /// After a failed commit: the rollback's writes are dropped until unlock.
    poisoned: bool,
    /// Pages this transaction read from its snapshot: a commit that lost the race is
    /// replayed over the winner only if the winner wrote none of them.
    reads: HashSet<u64>,
    /// The change counter page 1 shows this transaction. A new value per transaction
    /// makes SQLite drop its page cache at every start, so every page a transaction
    /// uses comes through xRead and lands in reads.
    epoch: u32,
}

impl RemoteFile {
    fn snap(&mut self) -> std::io::Result<Arc<Snapshot>> {
        if let Some(s) = &self.snap {
            return Ok(s.clone());
        }
        self.entry.db.snapshot()
    }

    fn size(&mut self) -> std::io::Result<u64> {
        if let Some(n) = self.size {
            return Ok(n);
        }
        let snap = self.snap()?;
        let ps = snap.page_size();
        let dirty_end = self.dirty.keys().next_back().map_or(0, |p| p * ps);
        Ok(snap.file_size().max(dirty_end))
    }

    fn page(&mut self, pgno: u64, out: &mut [u8]) -> std::io::Result<()> {
        if let Some(p) = self.dirty.get(&pgno) {
            out.copy_from_slice(p);
            return Ok(());
        }
        let snap = self.snap()?;
        self.entry.db.read_page(&snap, pgno, out)?;
        self.reads.insert(pgno);
        if pgno == 1 && tracking() {
            for r in crate::store::COUNTERS {
                out[r].copy_from_slice(&self.epoch.to_be_bytes());
            }
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.dirty.clear();
        self.size = None;
        self.batch = false;
        self.poisoned = false;
    }

    /// Sends the held pages as a commit. With drop_unchanged, pages equal to the
    /// snapshot's go first: a rollback's playback writes the originals back and
    /// syncs, and that must not become a version.
    fn commit(&mut self, drop_unchanged: bool) -> c_int {
        let Some(snap) = self.snap.clone() else {
            if self.dirty.is_empty() && self.size.is_none() {
                return ffi::SQLITE_OK;
            }
            log::error!("bucketdb: commit without a snapshot");
            return ffi::SQLITE_IOERR_WRITE;
        };
        let ps = snap.page_size();
        if drop_unchanged {
            let mut same = Vec::new();
            let mut page = vec![0u8; ps as usize];
            for (&p, bytes) in &self.dirty {
                if p <= snap.manifest.page_count
                    && self.entry.db.read_page(&snap, p, &mut page).is_ok()
                    && (if p == 1 {
                        crate::store::page1_equal(&page, bytes)
                    } else {
                        page == *bytes
                    })
                {
                    same.push(p);
                }
            }
            for p in same {
                self.dirty.remove(&p);
            }
            if self.size == Some(snap.file_size()) {
                self.size = None;
            }
        }
        if self.dirty.is_empty() && self.size.is_none() {
            return ffi::SQLITE_OK;
        }
        let size = match self.size() {
            Ok(n) => n,
            Err(e) => {
                log::error!("bucketdb: {e}");
                return ffi::SQLITE_IOERR_WRITE;
            }
        };
        let page_count = size.div_ceil(ps);
        let started = std::time::Instant::now();
        let result = self.entry.db.commit(
            &snap,
            &self.dirty,
            if tracking() { Some(&self.reads) } else { None },
            page_count,
        );
        let pages = self.dirty.len();
        self.dirty.clear();
        self.size = None;
        match result {
            Ok(Some(new)) => {
                log::debug!(
                    "bucketdb: {} v{} committed, {pages} pages in {:?}",
                    self.entry.db.name,
                    new.manifest.version,
                    started.elapsed()
                );
                self.snap = Some(new);
                ffi::SQLITE_OK
            }
            Ok(None) => {
                log::info!(
                    "bucketdb: {} v{} was taken by another writer; the transaction must run again",
                    self.entry.db.name,
                    snap.manifest.version + 1
                );
                self.poisoned = true;
                ffi::SQLITE_BUSY_SNAPSHOT
            }
            Err(e) => {
                log::error!("bucketdb: commit {}: {e}", self.entry.db.name);
                self.poisoned = true;
                ffi::SQLITE_IOERR_WRITE
            }
        }
    }
}

/// The RemoteFile behind a sqlite3_file this VFS opened.
///
/// SAFETY: f was opened by x_open with METHODS, and SQLite calls one method of a file
/// at a time.
unsafe fn rf<'a>(f: *mut ffi::sqlite3_file) -> &'a mut RemoteFile {
    // SAFETY: as documented on the function.
    unsafe { &mut *(*f.cast::<ZFile>()).file }
}

static METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 1,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_check_reserved_lock),
    xFileControl: Some(x_file_control),
    xSectorSize: Some(x_sector_size),
    xDeviceCharacteristics: Some(x_device_characteristics),
    xShmMap: None,
    xShmLock: None,
    xShmBarrier: None,
    xShmUnmap: None,
    xFetch: None,
    xUnfetch: None,
};

unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    if flags & ffi::SQLITE_OPEN_MAIN_DB != 0
        && let Some(entry) = remote(name)
    {
        let boxed = Box::new(RemoteFile {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            entry,
            snap: None,
            level: ffi::SQLITE_LOCK_NONE,
            dirty: BTreeMap::new(),
            size: None,
            batch: false,
            poisoned: false,
            reads: HashSet::new(),
            epoch: 0,
        });
        // SAFETY: SQLite gives szOsFile bytes at file, at least a ZFile.
        unsafe {
            let z = file.cast::<ZFile>();
            (*z).file = Box::into_raw(boxed);
            (*z).base.pMethods = &raw const METHODS;
            if !out_flags.is_null() {
                *out_flags = flags;
            }
        }
        return ffi::SQLITE_OK;
    }
    let _ = vfs;
    let p = parent();
    // SAFETY: every other file is the parent VFS's, opened as it would have been.
    unsafe { ((*p).xOpen.unwrap_or_else(|| unreachable!()))(p, name, file, flags, out_flags) }
}

unsafe extern "C" fn x_delete(_vfs: *mut ffi::sqlite3_vfs, name: *const c_char, sync: c_int) -> c_int {
    if remote(name).is_some() {
        return ffi::SQLITE_OK;
    }
    let p = parent();
    // SAFETY: the parent's own call.
    unsafe { ((*p).xDelete.unwrap_or_else(|| unreachable!()))(p, name, sync) }
}

unsafe extern "C" fn x_access(
    _vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    if remote(name).is_some() {
        // SAFETY: SQLite passes where to write the answer.
        unsafe { *out = 1 };
        return ffi::SQLITE_OK;
    }
    let p = parent();
    // SAFETY: the parent's own call.
    unsafe { ((*p).xAccess.unwrap_or_else(|| unreachable!()))(p, name, flags, out) }
}

unsafe extern "C" fn x_close(f: *mut ffi::sqlite3_file) -> c_int {
    // SAFETY: opened by x_open; this is its last call.
    unsafe {
        let z = f.cast::<ZFile>();
        let file = Box::from_raw((*z).file);
        let mut w = file
            .entry
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *w == Some(file.id) {
            *w = None;
        }
        (*z).file = std::ptr::null_mut();
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_read(f: *mut ffi::sqlite3_file, buf: *mut c_void, amt: c_int, off: i64) -> c_int {
    // SAFETY: see rf; buf holds amt bytes.
    let (file, out) = unsafe { (rf(f), std::slice::from_raw_parts_mut(buf.cast::<u8>(), amt as usize)) };
    let want = out.len();
    let mut read = || -> std::io::Result<bool> {
        let size = file.size()?;
        let ps = file.snap()?.page_size();
        let off = off as u64;
        let end = (off + out.len() as u64).min(size);
        let mut page = vec![0u8; ps as usize];
        let mut pos = off;
        while pos < end {
            let pgno = pos / ps + 1;
            let in_page = (pos % ps) as usize;
            let n = ((ps as usize) - in_page).min((end - pos) as usize);
            file.page(pgno, &mut page)?;
            let at = (pos - off) as usize;
            out[at..at + n].copy_from_slice(&page[in_page..in_page + n]);
            pos += n as u64;
        }
        let got = end.saturating_sub(off) as usize;
        out[got.min(want)..].fill(0);
        Ok(got == want)
    };
    match read() {
        Ok(true) => ffi::SQLITE_OK,
        Ok(false) => ffi::SQLITE_IOERR_SHORT_READ,
        Err(e) => {
            log::error!("bucketdb: read {}: {e}", file.entry.db.name);
            ffi::SQLITE_IOERR_READ
        }
    }
}

unsafe extern "C" fn x_write(f: *mut ffi::sqlite3_file, buf: *const c_void, amt: c_int, off: i64) -> c_int {
    // SAFETY: see rf; buf holds amt bytes.
    let (file, data) = unsafe { (rf(f), std::slice::from_raw_parts(buf.cast::<u8>(), amt as usize)) };
    if file.poisoned {
        return ffi::SQLITE_OK;
    }
    let mut write = || -> std::io::Result<()> {
        let ps = file.snap()?.page_size();
        let size = file.size()?;
        let off = off as u64;
        let mut pos = off;
        let end = off + data.len() as u64;
        while pos < end {
            let pgno = pos / ps + 1;
            let in_page = (pos % ps) as usize;
            let n = ((ps as usize) - in_page).min((end - pos) as usize);
            let at = (pos - off) as usize;
            let mut page = if n == ps as usize {
                Vec::new()
            } else {
                let mut p = vec![0u8; ps as usize];
                if (pgno - 1) * ps < size {
                    file.page(pgno, &mut p)?;
                }
                p
            };
            if page.is_empty() {
                page.extend_from_slice(&data[at..at + n]);
            } else {
                page[in_page..in_page + n].copy_from_slice(&data[at..at + n]);
            }
            if !file.dirty.contains_key(&pgno)
                && (file.dirty.len() as u64 + 1) * ps > file.entry.db.opts.max_txn_bytes()
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::StorageFull,
                    "transaction too large",
                ));
            }
            file.dirty.insert(pgno, page);
            pos += n as u64;
        }
        if end > size {
            file.size = Some(end);
        }
        Ok(())
    };
    match write() {
        Ok(()) => ffi::SQLITE_OK,
        Err(e) if e.kind() == std::io::ErrorKind::StorageFull => {
            log::warn!(
                "bucketdb: {}: a transaction passed BUCKETDB_MAX_TXN_MB",
                file.entry.db.name
            );
            ffi::SQLITE_FULL
        }
        Err(e) => {
            log::error!("bucketdb: write {}: {e}", file.entry.db.name);
            ffi::SQLITE_IOERR_WRITE
        }
    }
}

unsafe extern "C" fn x_truncate(f: *mut ffi::sqlite3_file, size: i64) -> c_int {
    // SAFETY: see rf.
    let file = unsafe { rf(f) };
    if file.poisoned {
        return ffi::SQLITE_OK;
    }
    let Ok(ps) = file.snap().map(|s| s.page_size()) else {
        return ffi::SQLITE_IOERR_TRUNCATE;
    };
    let size = size as u64;
    let keep = size.div_ceil(ps);
    file.dirty.retain(|&p, _| p <= keep);
    file.size = Some(size);
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_sync(f: *mut ffi::sqlite3_file, _flags: c_int) -> c_int {
    // SAFETY: see rf.
    let file = unsafe { rf(f) };
    // A batch commits at COMMIT_ATOMIC_WRITE. A transaction too big for the cache
    // spills and takes a real (local) rollback journal: then the sync of the main
    // file is its commit, or the end of its rollback's playback.
    if file.batch || file.poisoned || (file.dirty.is_empty() && file.size.is_none()) {
        return ffi::SQLITE_OK;
    }
    file.commit(true)
}

unsafe extern "C" fn x_file_size(f: *mut ffi::sqlite3_file, out: *mut i64) -> c_int {
    // SAFETY: see rf; out is SQLite's.
    let file = unsafe { rf(f) };
    match file.size() {
        Ok(n) => {
            // SAFETY: as above.
            unsafe { *out = n as i64 };
            ffi::SQLITE_OK
        }
        Err(e) => {
            log::error!("bucketdb: size {}: {e}", file.entry.db.name);
            ffi::SQLITE_IOERR_FSTAT
        }
    }
}

unsafe extern "C" fn x_lock(f: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    // SAFETY: see rf.
    let file = unsafe { rf(f) };
    if level <= file.level {
        return ffi::SQLITE_OK;
    }
    if file.level == ffi::SQLITE_LOCK_NONE {
        file.reads.clear();
        file.epoch = EPOCH.fetch_add(EPOCH_STEP, Ordering::Relaxed);
        match file.entry.db.snapshot() {
            Ok(s) => file.snap = Some(s),
            Err(e) => {
                log::error!("bucketdb: manifest {}: {e}", file.entry.db.name);
                return ffi::SQLITE_IOERR_LOCK;
            }
        }
    }
    if level >= ffi::SQLITE_LOCK_RESERVED {
        let mut w = file
            .entry
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match *w {
            Some(id) if id != file.id => return ffi::SQLITE_BUSY,
            _ => *w = Some(file.id),
        }
    }
    file.level = level;
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_unlock(f: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    // SAFETY: see rf.
    let file = unsafe { rf(f) };
    if level < ffi::SQLITE_LOCK_RESERVED && file.level >= ffi::SQLITE_LOCK_RESERVED {
        let mut w = file
            .entry
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *w == Some(file.id) {
            *w = None;
        }
        drop(w);
        file.reset();
    }
    if level == ffi::SQLITE_LOCK_NONE {
        file.snap = None;
    }
    file.level = level.min(file.level);
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_check_reserved_lock(f: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    // SAFETY: see rf; out is SQLite's.
    let file = unsafe { rf(f) };
    let held = file
        .entry
        .writer
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_some();
    // SAFETY: as above.
    unsafe { *out = c_int::from(held) };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_file_control(f: *mut ffi::sqlite3_file, op: c_int, _arg: *mut c_void) -> c_int {
    // SAFETY: see rf.
    let file = unsafe { rf(f) };
    match op {
        ffi::SQLITE_FCNTL_BEGIN_ATOMIC_WRITE => {
            file.batch = true;
            ffi::SQLITE_OK
        }
        ffi::SQLITE_FCNTL_COMMIT_ATOMIC_WRITE => {
            let rc = file.commit(false);
            file.batch = false;
            rc
        }
        ffi::SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE => {
            file.dirty.clear();
            file.size = None;
            file.batch = false;
            ffi::SQLITE_OK
        }
        _ => ffi::SQLITE_NOTFOUND,
    }
}

unsafe extern "C" fn x_sector_size(_f: *mut ffi::sqlite3_file) -> c_int {
    4096
}

unsafe extern "C" fn x_device_characteristics(_f: *mut ffi::sqlite3_file) -> c_int {
    ffi::SQLITE_IOCAP_BATCH_ATOMIC
        | ffi::SQLITE_IOCAP_POWERSAFE_OVERWRITE
        | ffi::SQLITE_IOCAP_SAFE_APPEND
        | ffi::SQLITE_IOCAP_SEQUENTIAL
        | ffi::SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN
}

/// An empty file per database in dir, which SQLite opens and the VFS serves from the
/// bucket; returns their canonical paths, the ones [`register`] takes.
///
/// # Errors
///
/// When one of them is a real file, or on disk errors.
pub fn placeholders(dir: &std::path::Path, names: &[String]) -> std::io::Result<Vec<std::path::PathBuf>> {
    std::fs::create_dir_all(dir)?;
    let mut out = Vec::new();
    for name in names {
        let p = dir.join(format!("{name}.db"));
        match std::fs::metadata(&p) {
            Ok(m) if m.len() > 0 => {
                return Err(std::io::Error::other(format!(
                    "{} is a real file; bucketdb's databases are placeholders",
                    p.display()
                )));
            }
            Ok(_) => {}
            Err(_) => {
                std::fs::File::create(&p)?;
            }
        }
        // A journal left by a crash would play stale pages over the bucket's newer
        // version; the bucket is the truth, and a commit there is all or nothing.
        let _ = std::fs::remove_file(dir.join(format!("{name}.db-journal")));
        out.push(std::fs::canonicalize(&p)?);
    }
    Ok(out)
}
