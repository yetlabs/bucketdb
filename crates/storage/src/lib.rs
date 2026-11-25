//! The storage side of bucketdb: a SQLite database file kept in an S3 bucket.
//!
//! A database is a manifest, the chunks of a base file and one segment per commit
//! (see [`store`]). The [`vfs`] makes that SQLite's main file: pages are read on
//! demand through a cache, and a transaction commits as a segment plus a conditional
//! swap of the manifest. [`sync`] moves whole files in and out: import, export, and
//! upsert of the pages a local copy changed; [`past`], a versioned bucket's past:
//! a commit's version looked at, exported or restored.
//!
//! A database is addressed as `s3://bucket/path` ([`Remote`]).

pub mod past;
pub mod s3;
pub mod store;
pub mod sync;
pub mod vfs;

use std::fmt;
use std::io;
use std::sync::{Arc, OnceLock};

/// This process's part of the names of the objects it writes. Random, not the pid:
/// every Lambda environment starts its extension with the same pid, so two sidecars
/// would name a segment of the same version alike, overwrite each other's object and,
/// losing the race, delete the winner's.
pub fn process_tag() -> &'static str {
    static TAG: OnceLock<String> = OnceLock::new();
    TAG.get_or_init(|| {
        use ring::rand::SecureRandom;
        let mut bytes = [0u8; 6];
        if ring::rand::SystemRandom::new().fill(&mut bytes).is_err() {
            // No system randomness: the time still tells processes apart.
            let nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
            bytes.copy_from_slice(&nanos.to_be_bytes()[2..]);
        }
        hex::encode(bytes)
    })
}

/// A database in a bucket: `s3://bucket/path`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Remote {
    pub bucket: String,
    /// The key prefix of its objects: the path.
    pub db: String,
    /// The name clients connect to: the path's last part.
    pub name: String,
}

impl Remote {
    /// Parses `s3://bucket/path`.
    ///
    /// # Errors
    ///
    /// When url is not of that form.
    pub fn parse(url: &str) -> io::Result<Remote> {
        let invalid = || io::Error::new(io::ErrorKind::InvalidInput, format!("{url}: expected s3://bucket/path"));
        let rest = url.strip_prefix("s3://").ok_or_else(invalid)?;
        let (bucket, db) = rest.split_once('/').unwrap_or((rest, ""));
        let db = db.trim_matches('/');
        if bucket.is_empty() || db.is_empty() {
            return Err(invalid());
        }
        Ok(Remote {
            bucket: bucket.to_string(),
            db: db.to_string(),
            name: db.rsplit('/').next().unwrap_or(db).to_string(),
        })
    }

    /// A client for its bucket, configured from the environment.
    ///
    /// # Errors
    ///
    /// When the credentials are not set.
    pub fn client(&self) -> io::Result<Arc<s3::Client>> {
        Ok(Arc::new(s3::Client::new(s3::Config::from_env(&self.bucket)?)))
    }
}

impl fmt::Display for Remote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s3://{}/{}", self.bucket, self.db)
    }
}

/// The function's memory in MB when running in AWS Lambda.
pub fn lambda_memory_mb() -> Option<u64> {
    std::env::var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE").ok()?.parse().ok()
}

/// A number from the environment, or the default.
pub fn env_num(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::Remote;

    #[test]
    fn parses_bucket_urls() {
        let r = Remote::parse("s3://bucket/a/b/").unwrap();
        assert_eq!(
            (r.bucket.as_str(), r.db.as_str(), r.name.as_str()),
            ("bucket", "a/b", "b")
        );
        assert_eq!(r.to_string(), "s3://bucket/a/b");
        for bad in ["bucket/a", "s3://bucket", "s3:///a", "file.sqlite"] {
            assert!(Remote::parse(bad).is_err(), "{bad}");
        }
    }
}
