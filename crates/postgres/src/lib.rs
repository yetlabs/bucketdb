//! The Postgres side of bucketdb: a Postgres wire protocol (v3) server whose
//! queries run on SQLite.
//!
//! [`serve`] accepts connections and gives each one a SQLite connection to its
//! database's file; statements are parsed as Postgres SQL with sqlparser and printed
//! back for SQLite ([`sql`]), with the catalog views and functions drivers and ORMs
//! look for. Where that file lives is the caller's business: bucketdb opens files
//! its storage VFS serves from a bucket.

mod funcs;
mod server;
pub mod sql;
mod types;

pub use server::{Config, serve};
