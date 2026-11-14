//! Postgres functions SQLite lacks, as SQLite functions on each connection.

use crate::types;
use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::{Connection, Result, ffi, types::ValueRef};

fn text(ctx: &Context<'_>, i: usize) -> Option<String> {
    match ctx.get_raw(i) {
        ValueRef::Null => None,
        ValueRef::Text(t) => Some(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Integer(n) => Some(n.to_string()),
        ValueRef::Real(f) => Some(f.to_string()),
        ValueRef::Blob(b) => Some(String::from_utf8_lossy(b).into_owned()),
    }
}

fn pad(ctx: &Context<'_>, left: bool) -> Option<String> {
    let s = text(ctx, 0)?;
    let n = usize::try_from(ctx.get::<i64>(1).ok()?).unwrap_or(0);
    let fill = if ctx.len() > 2 { text(ctx, 2)? } else { " ".into() };
    let chars: Vec<char> = s.chars().collect();
    if chars.len() >= n || fill.is_empty() {
        return Some(chars[..n.min(chars.len())].iter().collect());
    }
    let padding: String = fill.chars().cycle().take(n - chars.len()).collect();
    Some(if left { padding + &s } else { s + &padding })
}

/// Postgres' names for a declared type: (udt_name, data_type).
fn pg_names(decl: &str) -> (&'static str, &'static str) {
    let d = decl.to_ascii_uppercase();
    let base = d.split('(').next().unwrap_or("").trim();
    match base {
        "BIGINT" | "INT8" | "INTEGER" => ("int8", "bigint"),
        "INT" | "INT4" => ("int4", "integer"),
        "SMALLINT" | "INT2" => ("int2", "smallint"),
        "BOOLEAN" | "BOOL" => ("bool", "boolean"),
        "REAL" | "FLOAT4" => ("float4", "real"),
        "DOUBLE" | "FLOAT8" | "FLOAT" => ("float8", "double precision"),
        "NUMERIC" | "DECIMAL" => ("numeric", "numeric"),
        "VARCHAR" => ("varchar", "character varying"),
        "CHAR" => ("bpchar", "character"),
        "TIMESTAMPTZ" => ("timestamptz", "timestamp with time zone"),
        "TIMESTAMP" => ("timestamp", "timestamp without time zone"),
        "DATE" => ("date", "date"),
        "TIME_TEXT" => ("time", "time without time zone"),
        "INTERVAL_TEXT" => ("interval", "interval"),
        "BYTEA" | "BLOB" => ("bytea", "bytea"),
        "UUID" => ("uuid", "uuid"),
        "JSON" => ("json", "json"),
        "JSONB" => ("jsonb", "jsonb"),
        _ => ("text", "text"),
    }
}

/// The numbers in a decl's parentheses: VARCHAR(255), NUMERIC(12,2).
fn decl_args(decl: &str) -> Vec<i64> {
    decl.split_once('(')
        .map(|(_, a)| {
            a.trim_end_matches(')')
                .split(',')
                .filter_map(|n| n.trim().parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// The named constraints of a CREATE TABLE, as a JSON array of [name, type]:
/// `CONSTRAINT "fk_x" FOREIGN KEY ...` gives `["fk_x", "FOREIGN KEY"]`.
fn named_constraints(sql: &str) -> String {
    let mut out = Vec::new();
    let mut words = Vec::new();
    let b = sql.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'"' || c == b'`' || c == b'[' {
            let close = if c == b'[' { b']' } else { c };
            let start = i + 1;
            i += 1;
            while i < b.len() && b[i] != close {
                i += 1;
            }
            words.push((sql[start..i.min(b.len())].to_string(), true));
            i += 1;
        } else if c.is_ascii_alphanumeric() || c == b'_' {
            let start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            words.push((sql[start..i].to_string(), false));
        } else {
            i += 1;
        }
    }
    for w in words.windows(4) {
        if !w[0].1 && w[0].0.eq_ignore_ascii_case("CONSTRAINT") {
            let kind = w[2].0.to_ascii_uppercase();
            let kind = match kind.as_str() {
                "FOREIGN" => "FOREIGN KEY",
                "PRIMARY" => "PRIMARY KEY",
                "UNIQUE" => "UNIQUE",
                "CHECK" => "CHECK",
                _ => continue,
            };
            out.push(serde_json::json!([w[1].0, kind]));
        }
    }
    serde_json::Value::Array(out).to_string()
}

/// Views of pg_catalog and information_schema over sqlite_master, per connection:
/// what ORMs read to migrate (GORM's AutoMigrate), not what psql's \d reads.
pub const CATALOG: &str = r#"
CREATE TEMP VIEW pg_namespace AS
  SELECT 2200 AS oid, 'public' AS nspname UNION ALL SELECT 11, 'pg_catalog' UNION ALL SELECT 13000, 'information_schema';
CREATE TEMP VIEW pg_class AS
  SELECT m.rowid + 16384 AS oid, m.name AS relname, 2200 AS relnamespace,
         CASE m.type WHEN 'table' THEN 'r' WHEN 'index' THEN 'i' ELSE 'v' END AS relkind
  FROM main.sqlite_master m WHERE m.type IN ('table', 'index', 'view') AND m.name NOT LIKE 'sqlite_%';
CREATE TEMP VIEW pg_type AS
  SELECT column1 AS oid, column2 AS typname, column3 AS typlen, 2200 AS typnamespace FROM (VALUES
  (16, 'bool', 1), (17, 'bytea', -1), (20, 'int8', 8), (21, 'int2', 2), (23, 'int4', 4), (25, 'text', -1),
  (114, 'json', -1), (700, 'float4', 4), (701, 'float8', 8), (1042, 'bpchar', -1), (1043, 'varchar', -1),
  (1082, 'date', 4), (1083, 'time', 8), (1114, 'timestamp', 8), (1184, 'timestamptz', 8), (1186, 'interval', 16),
  (1700, 'numeric', -1), (2950, 'uuid', 16), (3802, 'jsonb', -1));
CREATE TEMP VIEW pg_attribute AS
  SELECT c.oid AS attrelid, p.name AS attname, pg_type_oid(p.type) AS atttypid, -1 AS atttypmod,
         p.cid + 1 AS attnum, p."notnull" OR p.pk > 0 AS attnotnull, 0 AS attisdropped
  FROM pg_class c JOIN pragma_table_info(c.relname) p WHERE c.relkind = 'r';
CREATE TEMP VIEW pg_description AS
  SELECT NULL AS objoid, NULL AS classoid, NULL AS objsubid, NULL AS description WHERE 0;
CREATE TEMP VIEW pg_indexes AS
  SELECT 'public' AS schemaname, m.tbl_name AS tablename, m.name AS indexname, NULL AS tablespace, m.sql AS indexdef
  FROM main.sqlite_master m WHERE m.type = 'index';
CREATE TEMP VIEW information_schema_tables AS
  SELECT current_database() AS table_catalog, 'public' AS table_schema, m.name AS table_name,
         CASE m.type WHEN 'view' THEN 'VIEW' ELSE 'BASE TABLE' END AS table_type
  FROM main.sqlite_master m WHERE m.type IN ('table', 'view') AND m.name NOT LIKE 'sqlite_%';
CREATE TEMP VIEW information_schema_columns AS
  SELECT current_database() AS table_catalog, 'public' AS table_schema, m.name AS table_name, p.name AS column_name,
         p.cid + 1 AS ordinal_position, p.dflt_value AS column_default,
         CASE WHEN p."notnull" OR p.pk > 0 THEN 'NO' ELSE 'YES' END AS is_nullable,
         pg_data_type(p.type) AS data_type, pg_udt_name(p.type) AS udt_name,
         pg_char_len(p.type) AS character_maximum_length, pg_num_precision(p.type) AS numeric_precision,
         CASE WHEN pg_num_precision(p.type) IS NULL THEN NULL ELSE 10 END AS numeric_precision_radix,
         pg_num_scale(p.type) AS numeric_scale,
         CASE WHEN pg_udt_name(p.type) LIKE 'timestamp%' THEN 6 END AS datetime_precision,
         NULL AS identity_increment
  FROM main.sqlite_master m JOIN pragma_table_info(m.name) p WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite_%';
CREATE TEMP VIEW information_schema_table_constraints AS
  SELECT current_database() AS constraint_catalog, 'public' AS constraint_schema, m.name || '_pkey' AS constraint_name,
         current_database() AS table_catalog, 'public' AS table_schema, m.name AS table_name, 'PRIMARY KEY' AS constraint_type
  FROM main.sqlite_master m WHERE m.type = 'table' AND EXISTS (SELECT 1 FROM pragma_table_info(m.name) WHERE pk > 0)
  UNION ALL
  SELECT current_database(), 'public', json_extract(j.value, '$[0]'), current_database(), 'public', m.name,
         json_extract(j.value, '$[1]')
  FROM main.sqlite_master m JOIN json_each(pg_named_constraints(m.sql)) j
  WHERE m.type = 'table' AND json_extract(j.value, '$[1]') <> 'PRIMARY KEY';
CREATE TEMP VIEW information_schema_constraint_column_usage AS
  SELECT current_database() AS constraint_catalog, 'public' AS constraint_schema, current_database() AS table_catalog,
         'public' AS table_schema, m.name AS table_name, p.name AS column_name, m.name || '_pkey' AS constraint_name
  FROM main.sqlite_master m JOIN pragma_table_info(m.name) p WHERE m.type = 'table' AND p.pk > 0
  UNION ALL
  SELECT current_database(), 'public', current_database(), 'public', m.name, ii.name, il.name
  FROM main.sqlite_master m JOIN pragma_index_list(m.name) il JOIN pragma_index_info(il.name) ii
  WHERE m.type = 'table' AND il."unique" AND il.origin = 'u';
"#;

pub fn register(conn: &Connection, database: &str) -> Result<()> {
    let det = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    let flags = FunctionFlags::SQLITE_UTF8;
    conn.create_scalar_function("now", 0, flags, |_| Ok(types::ts_text(chrono::Utc::now().naive_utc())))?;
    conn.create_scalar_function("version", 0, det, |_| {
        Ok(format!("PostgreSQL 16.0 (bucketdb {})", env!("CARGO_PKG_VERSION")))
    })?;
    conn.create_scalar_function("current_schema", 0, det, |_| Ok("public"))?;
    let db = database.to_string();
    conn.create_scalar_function("current_database", 0, det, move |_| Ok(db.clone()))?;
    conn.create_scalar_function("lpad", -1, det, |ctx| Ok(pad(ctx, true)))?;
    conn.create_scalar_function("rpad", -1, det, |ctx| Ok(pad(ctx, false)))?;
    conn.create_scalar_function("left", 2, det, |ctx| {
        let (Some(s), Ok(n)) = (text(ctx, 0), ctx.get::<i64>(1)) else {
            return Ok(None);
        };
        let len = s.chars().count() as i64;
        let take = if n >= 0 { n } else { (len + n).max(0) };
        Ok(Some(s.chars().take(take as usize).collect::<String>()))
    })?;
    conn.create_scalar_function("right", 2, det, |ctx| {
        let (Some(s), Ok(n)) = (text(ctx, 0), ctx.get::<i64>(1)) else {
            return Ok(None);
        };
        let len = s.chars().count() as i64;
        let take = if n >= 0 { n.min(len) } else { (len + n).max(0) };
        Ok(Some(s.chars().skip((len - take) as usize).collect::<String>()))
    })?;
    conn.create_scalar_function("pg_bool", 1, det, |ctx| {
        Ok(match ctx.get_raw(0) {
            ValueRef::Null => None,
            ValueRef::Integer(n) => Some(i64::from(n != 0)),
            ValueRef::Real(f) => Some(i64::from(f != 0.0)),
            ValueRef::Text(t) | ValueRef::Blob(t) => Some(i64::from(matches!(
                t.to_ascii_lowercase().as_slice(),
                b"t" | b"true" | b"y" | b"yes" | b"on" | b"1"
            ))),
        })
    })?;
    conn.create_scalar_function("pg_sleep", 1, flags, |ctx| {
        let secs = ctx.get::<f64>(0).unwrap_or(0.0).clamp(0.0, 3600.0);
        std::thread::sleep(std::time::Duration::from_secs_f64(secs));
        Ok(rusqlite::types::Null)
    })?;
    conn.create_scalar_function("pg_named_constraints", 1, det, |ctx| {
        Ok(named_constraints(&text(ctx, 0).unwrap_or_default()))
    })?;
    conn.create_scalar_function("pg_type_oid", 1, det, |ctx| {
        Ok(i64::from(types::oid_of_decl(text(ctx, 0).as_deref())))
    })?;
    conn.create_scalar_function("pg_udt_name", 1, det, |ctx| {
        Ok(pg_names(&text(ctx, 0).unwrap_or_default()).0)
    })?;
    conn.create_scalar_function("pg_data_type", 1, det, |ctx| {
        Ok(pg_names(&text(ctx, 0).unwrap_or_default()).1)
    })?;
    conn.create_scalar_function("pg_char_len", 1, det, |ctx| {
        let d = text(ctx, 0).unwrap_or_default();
        let (udt, _) = pg_names(&d);
        Ok(if udt == "varchar" || udt == "bpchar" {
            decl_args(&d).first().copied()
        } else {
            None
        })
    })?;
    conn.create_scalar_function("pg_num_precision", 1, det, |ctx| {
        let d = text(ctx, 0).unwrap_or_default();
        Ok(match pg_names(&d).0 {
            "int8" => Some(64),
            "int4" => Some(32),
            "int2" => Some(16),
            "float8" => Some(53),
            "float4" => Some(24),
            "numeric" => decl_args(&d).first().copied(),
            _ => None,
        })
    })?;
    conn.create_scalar_function("pg_num_scale", 1, det, |ctx| {
        let d = text(ctx, 0).unwrap_or_default();
        Ok(match pg_names(&d).0 {
            "int8" | "int4" | "int2" => Some(0),
            "numeric" => decl_args(&d).get(1).copied().or(Some(0)),
            _ => None,
        })
    })?;
    conn.create_scalar_function("format_type", 2, det, |ctx| {
        let oid = ctx.get::<i64>(0).unwrap_or(25);
        Ok(match oid {
            16 => "boolean",
            17 => "bytea",
            20 => "bigint",
            21 => "smallint",
            23 => "integer",
            114 => "json",
            700 => "real",
            701 => "double precision",
            1043 => "character varying",
            1082 => "date",
            1114 => "timestamp without time zone",
            1184 => "timestamp with time zone",
            1700 => "numeric",
            2950 => "uuid",
            3802 => "jsonb",
            _ => "text",
        })
    })?;
    conn.create_scalar_function("gen_random_uuid", 0, flags, |_| {
        let mut b = [0u8; 16];
        // SAFETY: SQLite fills the 16 bytes handed to it.
        unsafe { ffi::sqlite3_randomness(16, b.as_mut_ptr().cast()) };
        b[6] = (b[6] & 0x0f) | 0x40;
        b[8] = (b[8] & 0x3f) | 0x80;
        let h = hex::encode(b);
        Ok(format!(
            "{}-{}-{}-{}-{}",
            &h[..8],
            &h[8..12],
            &h[12..16],
            &h[16..20],
            &h[20..]
        ))
    })?;
    Ok(())
}
