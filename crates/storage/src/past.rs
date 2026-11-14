//! A database's past, kept by its bucket: with versioning on, S3 keeps every
//! manifest a commit replaced and every object a compaction deleted, so each commit
//! is a version to look at again, or to go back to, for as long as the bucket's
//! lifecycle keeps them.
//!
//! [`versions`] lists them, [`at`] finds the one of a moment, and [`get_any`] reads
//! an object a past version names even once it is gone. [`export_at`] writes one to
//! a file; [`restore`] makes one the current version again, as `import --replace`
//! does: a new base, so a transaction under way when it lands is a conflict, run
//! again over what was restored.

use crate::s3::{Client, Get, Version};
use crate::store::{self, Manifest, Snapshot};
use crate::sync;
use serde::Serialize;
use std::io;

/// A version of a database, as its manifest was then.
#[derive(Debug, Clone, Serialize)]
pub struct Past {
    /// The manifest's version id in the bucket: what names this version exactly.
    pub id: String,
    /// When it was committed, RFC 3339.
    pub at: String,
    /// The manifest's own version number.
    pub version: u64,
    /// The database's size then, in bytes.
    pub bytes: u64,
    /// Whether it is the current version.
    pub latest: bool,
}

/// The manifest's versions, the newest first, at most limit (0: all), deletions left out.
///
/// # Errors
///
/// On S3 errors, or when the bucket keeps no versions.
pub fn versions(s3: &Client, db: &str, limit: usize) -> io::Result<Vec<Past>> {
    let key = store::manifest_key(db);
    let mut list: Vec<Version> = s3
        .versions(&key)?
        .into_iter()
        .filter(|v| v.key == key && !v.deleted)
        .collect();
    if list.is_empty() {
        return Err(io::Error::other(format!(
            "s3://{}/{db} has no versions: is the bucket's versioning on?",
            s3.bucket()
        )));
    }
    list.sort_by(|a, b| b.last_modified.cmp(&a.last_modified));
    if limit > 0 {
        list.truncate(limit);
    }
    let mut out = Vec::with_capacity(list.len());
    for v in list {
        let m = manifest_of(s3, &key, &v)?;
        out.push(Past {
            id: v.version_id,
            at: v.last_modified,
            version: m.version,
            bytes: m.page_count * u64::from(m.page_size),
            latest: v.latest,
        });
    }
    Ok(out)
}

fn manifest_of(s3: &Client, key: &str, v: &Version) -> io::Result<Manifest> {
    match s3.get_version(key, &v.version_id, None)? {
        Get::Found { body, .. } => serde_json::from_slice(&body).map_err(io::Error::other),
        _ => Err(io::Error::other(format!("{key}: version {} is gone", v.version_id))),
    }
}

/// The version of db at when: a version id, as [`versions`] lists them, or a moment
/// (RFC 3339), the newest committed by then.
///
/// # Errors
///
/// When there is none, or on S3 errors.
pub fn at(s3: &Client, db: &str, when: &str) -> io::Result<Snapshot> {
    let key = store::manifest_key(db);
    let all: Vec<Version> = s3
        .versions(&key)?
        .into_iter()
        .filter(|v| v.key == key && !v.deleted)
        .collect();
    let chosen = if let Some(v) = all.iter().find(|v| v.version_id == when) {
        v.clone()
    } else {
        let moment = chrono::DateTime::parse_from_rfc3339(when)
            .map_err(|_| io::Error::other(format!("{when}: neither a version id nor a moment (RFC 3339)")))?;
        all.iter()
            .filter(|v| chrono::DateTime::parse_from_rfc3339(&v.last_modified).is_ok_and(|t| t <= moment))
            .max_by(|a, b| a.last_modified.cmp(&b.last_modified))
            .cloned()
            .ok_or_else(|| {
                io::Error::other(format!(
                    "s3://{}/{db} keeps no version from {when} or before",
                    s3.bucket()
                ))
            })?
    };
    let manifest = manifest_of(s3, &key, &chosen)?;
    Ok(Snapshot::pinned(manifest, chosen.version_id))
}

/// An object a version names — or, deleted since, its version kept by the bucket.
/// Objects never change, so any version of a key is the one it named.
///
/// # Errors
///
/// When the bucket kept no version of it, or on S3 errors.
pub fn get_any(s3: &Client, key: &str, range: Option<(u64, u64)>) -> io::Result<Vec<u8>> {
    if let Get::Found { body, .. } = s3.get(key, range, None)? {
        return Ok(body);
    }
    let kept = s3
        .versions(key)?
        .into_iter()
        .filter(|v| v.key == key && !v.deleted)
        .max_by(|a, b| a.last_modified.cmp(&b.last_modified))
        .ok_or_else(|| io::Error::other(format!("{key}: missing, and no version of it is kept")))?;
    match s3.get_version(key, &kept.version_id, range)? {
        Get::Found { body, .. } => Ok(body),
        _ => Err(io::Error::other(format!("{key}: version {} is gone", kept.version_id))),
    }
}

/// Writes a version of db to file, as it was.
///
/// # Errors
///
/// On S3 or disk errors.
pub fn export_at(s3: &Client, snap: &Snapshot, file: &str, threads: u64) -> io::Result<()> {
    sync::write_file(s3, snap, file, threads, true)
}

/// Makes the version of db at when the current one again: written to a file beside,
/// then uploaded as a new base over whatever version is there (`import --replace`).
///
/// # Errors
///
/// When there is no such version, or on S3 or disk errors.
pub fn restore(s3: &Client, db: &str, when: &str, chunk_pages: u64, threads: u64) -> io::Result<u64> {
    let snap = at(s3, db, when)?;
    let file = std::env::temp_dir().join(format!("bucketdb-restore-{}.sqlite", std::process::id()));
    let file = file.to_string_lossy().into_owned();
    let result =
        export_at(s3, &snap, &file, threads).and_then(|()| sync::import(s3, db, &file, true, chunk_pages, threads));
    let _ = std::fs::remove_file(&file);
    result.map(|()| snap.manifest.version)
}
