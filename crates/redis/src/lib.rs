//! The Redis side of bucketdb: a RESP2 server whose keys live in a table of the
//! database, `bucketdb_kv`, so they are in the bucket as the rest of it is.
//!
//! [`serve`] gives each connection SQLite connections to the databases it may
//! SELECT; a write command is a transaction, committed (in the bucket) before its
//! reply. Keys expire by time: every read filters on the expiry, so a key is gone
//! the moment it expires, and a read never writes.

mod cmd;
mod resp;
mod server;

pub use server::{Config, prepare, serve};
