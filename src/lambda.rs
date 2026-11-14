//! Running as an AWS Lambda extension.
//!
//! Lambda starts every executable in /opt/extensions before the function, with no
//! arguments, and the function's init waits until each one has registered and asked
//! for its first event. bucketdb registers once its port is open, so the function
//! never connects too early, and asks only for SHUTDOWN: between invocations the
//! whole environment is frozen, and a commit is in the bucket before the app hears
//! of it, so on shutdown there is nothing to save.

use std::time::Duration;

/// Inside Lambda (the runtime API is set) with no command given.
pub fn active() -> Option<String> {
    std::env::var("AWS_LAMBDA_RUNTIME_API").ok().filter(|a| !a.is_empty())
}

/// Registers with the Extensions API and blocks on its events; returns on SHUTDOWN.
pub fn run(api: &str) -> Result<(), String> {
    // Lambda knows an extension by its file name.
    let name = std::env::args()
        .next()
        .and_then(|a| {
            std::path::Path::new(&a)
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| "bucketdb".into());
    // Next is a long poll that returns when the environment shuts down.
    let agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(None::<Duration>)
        .build()
        .new_agent();
    let base = format!("http://{api}/2020-01-01/extension");
    let resp = agent
        .post(&format!("{base}/register"))
        .header("Lambda-Extension-Name", &name)
        .send(r#"{"events":["SHUTDOWN"]}"#)
        .map_err(|e| format!("lambda register: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("lambda register: HTTP {}", resp.status()));
    }
    let id = resp
        .headers()
        .get("Lambda-Extension-Identifier")
        .and_then(|v| v.to_str().ok())
        .ok_or("lambda register: no Lambda-Extension-Identifier")?
        .to_string();
    log::info!("bucketdb: registered as Lambda extension {name}");
    loop {
        let mut resp = agent
            .get(&format!("{base}/event/next"))
            .header("Lambda-Extension-Identifier", &id)
            .call()
            .map_err(|e| format!("lambda next: {e}"))?;
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        if !resp.status().is_success() {
            return Err(format!("lambda next: HTTP {}: {body}", resp.status()));
        }
        if body.contains("\"SHUTDOWN\"") {
            log::info!("bucketdb: Lambda shutdown");
            return Ok(());
        }
    }
}
