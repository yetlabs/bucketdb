//! The four S3 calls bucketdb makes, signed with SigV4: GET (whole or a range, with
//! If-None-Match), PUT (with If-Match or If-None-Match), DELETE and create-bucket.
//! Any S3: AWS, MinIO, R2, set by endpoint, path-style.

use ring::{digest, hmac};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

impl Config {
    /// For bucket, from the environment: BUCKETDB_S3_ENDPOINT, BUCKETDB_S3_REGION and
    /// the usual AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN.
    ///
    /// # Errors
    ///
    /// When the credentials are not set.
    pub fn from_env(bucket: &str) -> io::Result<Config> {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let region = var("BUCKETDB_S3_REGION")
            .or_else(|| var("AWS_REGION"))
            .unwrap_or_else(|| "us-east-1".into());
        let endpoint = var("BUCKETDB_S3_ENDPOINT").unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        let need = |k: &str| var(k).ok_or_else(|| io::Error::other(format!("{k} is not set")));
        Ok(Config {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            bucket: bucket.to_string(),
            region,
            access_key: need("AWS_ACCESS_KEY_ID")?,
            secret_key: need("AWS_SECRET_ACCESS_KEY")?,
            session_token: var("AWS_SESSION_TOKEN"),
        })
    }
}

/// Counters for the log: what reading from S3 costs.
#[derive(Debug, Default)]
pub struct Stats {
    pub gets: AtomicU64,
    pub get_bytes: AtomicU64,
    pub puts: AtomicU64,
    pub put_bytes: AtomicU64,
}

#[derive(Debug)]
pub enum Get {
    Found { body: Vec<u8>, etag: String },
    NotModified,
    NotFound,
}

#[derive(Debug)]
pub enum Cond<'a> {
    None,
    IfMatch(&'a str),
    IfNoneMatch,
}

#[derive(Debug)]
pub enum Put {
    Done { etag: String },
    PreconditionFailed,
}

#[derive(Debug)]
pub struct Client {
    cfg: Config,
    host: String,
    agent: ureq::Agent,
    pub stats: Stats,
}

impl Client {
    pub fn new(cfg: Config) -> Client {
        let host = cfg
            .endpoint
            .split_once("://")
            .map_or(cfg.endpoint.as_str(), |(_, h)| h)
            .to_string();
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(60)))
            .max_idle_connections_per_host(64)
            .build()
            .new_agent();
        Client {
            cfg,
            host,
            agent,
            stats: Stats::default(),
        }
    }

    pub fn bucket(&self) -> &str {
        &self.cfg.bucket
    }

    fn path(&self, key: &str) -> String {
        let mut p = format!("/{}", self.cfg.bucket);
        if !key.is_empty() {
            p.push('/');
            for (i, seg) in key.split('/').enumerate() {
                if i > 0 {
                    p.push('/');
                }
                p.push_str(&uri_encode(seg));
            }
        }
        p
    }

    /// The SigV4 headers for one request.
    fn sign(&self, method: &str, path: &str, payload_hash: &str) -> Vec<(String, String)> {
        let now = chrono::Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = &amz_date[..8];
        let mut headers = vec![
            ("host".to_string(), self.host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        if let Some(t) = &self.cfg.session_token {
            headers.push(("x-amz-security-token".to_string(), t.clone()));
        }
        let signed: Vec<&str> = headers.iter().map(|(k, _)| k.as_str()).collect();
        let signed = signed.join(";");
        let mut canonical_headers = String::new();
        for (k, v) in &headers {
            canonical_headers.push_str(k);
            canonical_headers.push(':');
            canonical_headers.push_str(v.trim());
            canonical_headers.push('\n');
        }
        let canonical = format!("{method}\n{path}\n\n{canonical_headers}\n{signed}\n{payload_hash}");
        let scope = format!("{date}/{}/s3/aws4_request", self.cfg.region);
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(digest::digest(&digest::SHA256, canonical.as_bytes()))
        );
        let mut key = hmac(format!("AWS4{}", self.cfg.secret_key).as_bytes(), date.as_bytes());
        for part in [self.cfg.region.as_bytes(), b"s3", b"aws4_request"] {
            key = hmac(&key, part);
        }
        let signature = hex::encode(hmac(&key, to_sign.as_bytes()));
        headers.push((
            "authorization".to_string(),
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
                self.cfg.access_key
            ),
        ));
        headers.retain(|(k, _)| k != "host"); // ureq writes Host from the URL
        headers
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.cfg.endpoint, path)
    }

    /// GET a key, or bytes start..start+len of it.
    ///
    /// # Errors
    ///
    /// On network errors, or an HTTP status other than 200, 206, 304 and 404.
    pub fn get(&self, key: &str, range: Option<(u64, u64)>, if_none_match: Option<&str>) -> io::Result<Get> {
        let path = self.path(key);
        let mut req = self.agent.get(self.url(&path));
        for (k, v) in self.sign("GET", &path, EMPTY_SHA256) {
            req = req.header(k, v);
        }
        if let Some((start, len)) = range {
            req = req.header("range", format!("bytes={start}-{}", start + len - 1));
        }
        if let Some(etag) = if_none_match {
            req = req.header("if-none-match", etag);
        }
        let mut res = match req.call() {
            Ok(r) => r,
            // A pooled connection the server closed: once more on a new one.
            Err(ureq::Error::Io(_)) => {
                let mut again = self.agent.get(self.url(&path));
                for (k, v) in self.sign("GET", &path, EMPTY_SHA256) {
                    again = again.header(k, v);
                }
                if let Some((start, len)) = range {
                    again = again.header("range", format!("bytes={start}-{}", start + len - 1));
                }
                if let Some(etag) = if_none_match {
                    again = again.header("if-none-match", etag);
                }
                again.call().map_err(io::Error::other)?
            }
            Err(e) => return Err(io::Error::other(e)),
        };
        self.stats.gets.fetch_add(1, Ordering::Relaxed);
        match res.status().as_u16() {
            200 | 206 => {
                let etag = header(&res, "etag");
                let body = res
                    .body_mut()
                    .with_config()
                    .limit(u64::MAX)
                    .read_to_vec()
                    .map_err(io::Error::other)?;
                self.stats.get_bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
                Ok(Get::Found { body, etag })
            }
            304 => Ok(Get::NotModified),
            404 => Ok(Get::NotFound),
            s => Err(status_error("GET", key, s, &mut res)),
        }
    }

    /// See the module documentation.
    ///
    /// # Errors
    ///
    /// On network errors, or an HTTP status other than 200, 409 and 412.
    pub fn put(&self, key: &str, body: &[u8], cond: Cond<'_>) -> io::Result<Put> {
        let path = self.path(key);
        let hash = hex::encode(digest::digest(&digest::SHA256, body));
        let attempt = || {
            let mut req = self.agent.put(self.url(&path));
            for (k, v) in self.sign("PUT", &path, &hash) {
                req = req.header(k, v);
            }
            req = match cond {
                Cond::None => req,
                Cond::IfMatch(etag) => req.header("if-match", etag),
                Cond::IfNoneMatch => req.header("if-none-match", "*"),
            };
            req.send(body)
        };
        // A pooled connection the server closed is tried once more. A conditional PUT
        // that did land the first time answers 412 the second: the caller re-reads.
        let mut res = match attempt() {
            Err(ureq::Error::Io(_)) => attempt().map_err(io::Error::other)?,
            r => r.map_err(io::Error::other)?,
        };
        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        self.stats.put_bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
        match res.status().as_u16() {
            200 => Ok(Put::Done {
                etag: header(&res, "etag"),
            }),
            // 409: a concurrent conditional write to the same key, as S3 answers it.
            412 | 409 => Ok(Put::PreconditionFailed),
            s => Err(status_error("PUT", key, s, &mut res)),
        }
    }

    /// See the module documentation.
    ///
    /// # Errors
    ///
    /// On network errors, or an HTTP status other than 200, 204 and 404.
    pub fn delete(&self, key: &str) -> io::Result<()> {
        let path = self.path(key);
        let mut req = self.agent.delete(self.url(&path));
        for (k, v) in self.sign("DELETE", &path, EMPTY_SHA256) {
            req = req.header(k, v);
        }
        let mut res = req.call().map_err(io::Error::other)?;
        match res.status().as_u16() {
            200 | 204 | 404 => Ok(()),
            s => Err(status_error("DELETE", key, s, &mut res)),
        }
    }

    /// Creates the bucket; one that is already there is fine.
    ///
    /// # Errors
    ///
    /// On network errors, or a refusal other than the bucket existing already.
    pub fn create_bucket(&self) -> io::Result<()> {
        let path = self.path("");
        let mut req = self.agent.put(self.url(&path));
        for (k, v) in self.sign("PUT", &path, EMPTY_SHA256) {
            req = req.header(k, v);
        }
        let mut res = req.send_empty().map_err(io::Error::other)?;
        match res.status().as_u16() {
            200 | 409 => Ok(()),
            s => Err(status_error("PUT bucket", &self.cfg.bucket, s, &mut res)),
        }
    }
}

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
        .as_ref()
        .to_vec()
}

fn uri_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn header(res: &ureq::http::Response<ureq::Body>, name: &str) -> String {
    res.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

fn status_error(what: &str, key: &str, status: u16, res: &mut ureq::http::Response<ureq::Body>) -> io::Error {
    let body = res.body_mut().read_to_string().unwrap_or_default();
    io::Error::other(format!(
        "S3 {what} {key}: HTTP {status}: {}",
        body.chars().take(300).collect::<String>()
    ))
}

impl Client {
    /// Logs what this client has cost so far.
    pub fn log_stats(&self) {
        let st = &self.stats;
        log::info!(
            "s3: {} GET ({:.1} MB), {} PUT ({:.1} MB)",
            st.gets.load(Ordering::Relaxed),
            st.get_bytes.load(Ordering::Relaxed) as f64 / 1e6,
            st.puts.load(Ordering::Relaxed),
            st.put_bytes.load(Ordering::Relaxed) as f64 / 1e6
        );
    }

    /// GETs and PUTs so far.
    pub fn requests(&self) -> u64 {
        self.stats.gets.load(Ordering::Relaxed) + self.stats.puts.load(Ordering::Relaxed)
    }
}
