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

/// A column's atttypmod: VARCHAR(n) and CHAR(n) n + 4, NUMERIC(p, s) (p << 16 | s) + 4.
fn typmod(decl: &str) -> i64 {
    let args = decl_args(decl);
    match (pg_names(decl).0, args.as_slice()) {
        ("varchar" | "bpchar", [n, ..]) => n + 4,
        ("numeric", [p]) => (p << 16) + 4,
        ("numeric", [p, s, ..]) => ((p << 16) | s) + 4,
        _ => -1,
    }
}

/// format_type(oid, typmod): a type's name as psql shows it.
fn format_type(oid: i64, typmod: i64) -> String {
    let name = match oid {
        16 => "boolean",
        17 => "bytea",
        20 => "bigint",
        21 => "smallint",
        23 => "integer",
        114 => "json",
        700 => "real",
        701 => "double precision",
        1042 => "character",
        1043 => "character varying",
        1082 => "date",
        1083 => "time without time zone",
        1114 => "timestamp without time zone",
        1184 => "timestamp with time zone",
        1186 => "interval",
        1700 => "numeric",
        2950 => "uuid",
        3802 => "jsonb",
        _ => "text",
    };
    match oid {
        1042 | 1043 if typmod > 4 => format!("{name}({})", typmod - 4),
        1700 if typmod > 4 => {
            let m = typmod - 4;
            format!("{name}({},{})", m >> 16, m & 0xffff)
        }
        _ => name.to_string(),
    }
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
/// what ORMs read to migrate (GORM's AutoMigrate) and what psql's `\d` reads. A
/// table's OID is its sqlite_master rowid + 16384; an index SQLite keeps no entry
/// for (an INTEGER PRIMARY KEY, a WITHOUT ROWID key) gets its table's + 1000000.
/// Indexes take Postgres' names: t_pkey, t_col_key for a UNIQUE constraint.
/// The OIDs are CASTs so `oid = '16385'` (as psql writes it) compares as a number.
/// Tables named `sqlite_*` (SQLite's own) or `bucketdb_*` (BucketDB's, such as the
/// Redis side's keys) are left out of every view, though SQL can still read them.
pub const CATALOG: &str = r#"
CREATE TEMP VIEW pg_bucketdb_rel AS
  SELECT rowid AS rowid, type, name, sql FROM main.sqlite_master
  WHERE type IN ('table', 'view') AND substr(name, 1, 7) <> 'sqlite_' AND substr(name, 1, 9) <> 'bucketdb_';
CREATE TEMP VIEW pg_namespace AS
  SELECT 2200 AS oid, 'public' AS nspname, 10 AS nspowner UNION ALL SELECT 11, 'pg_catalog', 10
  UNION ALL SELECT 13000, 'information_schema', 10;
CREATE TEMP VIEW pg_bucketdb_index AS
  SELECT *, 'CREATE ' || CASE WHEN indisunique THEN 'UNIQUE ' ELSE '' END || 'INDEX ' || relname
            || ' ON public.' || tablename || ' USING btree ' || keys AS indexdef
  FROM (
    SELECT CAST(COALESCE(x.rowid + 16384, t.rowid + 16384 + 1000000) AS INTEGER) AS oid,
           CASE il.origin WHEN 'pk' THEN t.name || '_pkey'
             WHEN 'u' THEN t.name || '_' || (SELECT group_concat(name, '_') FROM pragma_index_info(il.name)) || '_key'
             ELSE il.name END AS relname,
           t.name AS tablename, CAST(t.rowid + 16384 AS INTEGER) AS indrelid,
           il."unique" AS indisunique, il.origin = 'pk' AS indisprimary, il.origin AS origin,
           (SELECT group_concat(name, ', ') FROM pragma_index_info(il.name)) AS cols,
           (SELECT group_concat(cid + 1, ' ') FROM pragma_index_info(il.name)) AS indkey,
           (SELECT count(*) FROM pragma_index_info(il.name)) AS indnatts,
           CASE WHEN x.sql IS NOT NULL THEN substr(x.sql, instr(x.sql, '('))
             ELSE '(' || (SELECT group_concat(name, ', ') FROM pragma_index_info(il.name)) || ')' END AS keys
    FROM pg_bucketdb_rel t JOIN pragma_index_list(t.name) il
    LEFT JOIN main.sqlite_master x ON x.type = 'index' AND x.name = il.name
    WHERE t.type = 'table'
    UNION ALL
    SELECT CAST(t.rowid + 16384 + 1000000 AS INTEGER), t.name || '_pkey', t.name, CAST(t.rowid + 16384 AS INTEGER),
           1, 1, 'pk', p.name, p.cid + 1, 1, '(' || p.name || ')'
    FROM pg_bucketdb_rel t JOIN pragma_table_info(t.name) p
    WHERE t.type = 'table' AND p.pk = 1 AND upper(p.type) = 'INTEGER'
      AND (SELECT count(*) FROM pragma_table_info(t.name) WHERE pk > 0) = 1
      AND NOT EXISTS (SELECT 1 FROM pragma_index_list(t.name) WHERE origin = 'pk'));
CREATE TEMP VIEW pg_class AS
  SELECT CAST(m.rowid + 16384 AS INTEGER) AS oid, m.name AS relname, 2200 AS relnamespace,
         CASE m.type WHEN 'table' THEN 'r' ELSE 'v' END AS relkind, CASE m.type WHEN 'table' THEN 2 ELSE 0 END AS relam,
         (SELECT count(*) FROM pragma_table_info(m.name)) AS relnatts,
         EXISTS (SELECT 1 FROM pg_bucketdb_index i WHERE i.tablename = m.name) AS relhasindex,
         10 AS relowner, 0 AS reltablespace, -1 AS reltuples, 0 AS relpages, 0 AS relchecks, 0 AS relhasrules,
         -- Postgres keeps foreign keys as triggers; psql looks for them only then.
         EXISTS (SELECT 1 FROM pragma_foreign_key_list(m.name)) OR EXISTS (SELECT 1 FROM pg_bucketdb_rel o
           JOIN pragma_foreign_key_list(o.name) f WHERE o.type = 'table' AND f."table" = m.name) AS relhastriggers,
         0 AS relhassubclass, 0 AS relrowsecurity, 0 AS relforcerowsecurity, 0 AS relhasoids,
         0 AS relispartition, 0 AS reloftype, 0 AS reltype, 0 AS reltoastrelid, 'p' AS relpersistence,
         'd' AS relreplident, NULL AS reloptions, NULL AS relpartbound, NULL AS relacl
  FROM pg_bucketdb_rel m WHERE m.type IN ('table', 'view')
  UNION ALL
  SELECT i.oid, i.relname, 2200, 'i', 403, i.indnatts, 0, 10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'p', 'n',
         NULL, NULL, NULL
  FROM pg_bucketdb_index i;
CREATE TEMP VIEW pg_index AS
  SELECT oid AS indexrelid, indrelid, indnatts, indnatts AS indnkeyatts, indisunique, indisprimary,
         0 AS indisexclusion, 1 AS indimmediate, 0 AS indisclustered, 1 AS indisvalid, 1 AS indisready,
         1 AS indislive, 0 AS indisreplident, indkey, NULL AS indexprs, NULL AS indpred
  FROM pg_bucketdb_index;
CREATE TEMP VIEW pg_constraint AS
  SELECT CAST(i.oid + 2000000 AS INTEGER) AS oid, i.relname AS conname, 2200 AS connamespace,
         CASE WHEN i.indisprimary THEN 'p' ELSE 'u' END AS contype, 0 AS condeferrable, 0 AS condeferred,
         1 AS convalidated, i.indrelid AS conrelid, 0 AS contypid, i.oid AS conindid, 0 AS conparentid,
         0 AS confrelid, ' ' AS confupdtype, ' ' AS confdeltype, ' ' AS confmatchtype, 1 AS conislocal,
         0 AS coninhcount, 0 AS connoinherit, '{' || replace(i.indkey, ' ', ',') || '}' AS conkey, NULL AS confkey,
         CASE WHEN i.indisprimary THEN 'PRIMARY KEY ' ELSE 'UNIQUE ' END || '(' || i.cols || ')' AS condef
  FROM pg_bucketdb_index i WHERE i.origin IN ('pk', 'u')
  UNION ALL
  SELECT CAST(3000000 + (t.rowid + 16384) * 100 + f.id AS INTEGER),
         coalesce(pg_fk_name(t.sql, f.id, (SELECT count(DISTINCT id) FROM pragma_foreign_key_list(t.name))),
                  t.name || '_' || group_concat(f."from", '_') || '_fkey'), 2200, 'f', 0, 0, 1,
         CAST(t.rowid + 16384 AS INTEGER), 0, 0, 0,
         (SELECT CAST(r.rowid + 16384 AS INTEGER) FROM pg_bucketdb_rel r WHERE r.type = 'table' AND r.name = f."table"),
         ' ', ' ', ' ', 1, 0, 0, NULL, NULL,
         'FOREIGN KEY (' || group_concat(f."from", ', ') || ') REFERENCES ' || f."table"
           || coalesce('(' || group_concat(f."to", ', ') || ')', '')
           || CASE f.on_update WHEN 'NO ACTION' THEN '' ELSE ' ON UPDATE ' || f.on_update END
           || CASE f.on_delete WHEN 'NO ACTION' THEN '' ELSE ' ON DELETE ' || f.on_delete END
  FROM pg_bucketdb_rel t JOIN pragma_foreign_key_list(t.name) f
  WHERE t.type = 'table' GROUP BY t.name, f.id;
CREATE TEMP VIEW pg_am AS
  SELECT 2 AS oid, 'heap' AS amname, 't' AS amtype UNION ALL SELECT 403, 'btree', 'i';
CREATE TEMP VIEW pg_type AS
  SELECT column1 AS oid, column2 AS typname, column3 AS typlen, 2200 AS typnamespace, 0 AS typcollation FROM (VALUES
  (16, 'bool', 1), (17, 'bytea', -1), (20, 'int8', 8), (21, 'int2', 2), (23, 'int4', 4), (25, 'text', -1),
  (114, 'json', -1), (700, 'float4', 4), (701, 'float8', 8), (1042, 'bpchar', -1), (1043, 'varchar', -1),
  (1082, 'date', 4), (1083, 'time', 8), (1114, 'timestamp', 8), (1184, 'timestamptz', 8), (1186, 'interval', 16),
  (1700, 'numeric', -1), (2950, 'uuid', 16), (3802, 'jsonb', -1));
CREATE TEMP VIEW pg_attribute AS
  SELECT c.oid AS attrelid, p.name AS attname, pg_type_oid(p.type) AS atttypid, pg_typmod(p.type) AS atttypmod,
         p.cid + 1 AS attnum, p."notnull" OR p.pk > 0 AS attnotnull, 0 AS attisdropped,
         p.dflt_value IS NOT NULL AS atthasdef, '' AS attidentity, '' AS attgenerated, 0 AS attcollation,
         -1 AS attstattarget, 'x' AS attstorage, '' AS attcompression, 1 AS attislocal, 0 AS attinhcount,
         NULL AS attfdwoptions
  FROM pg_class c JOIN pragma_table_info(c.relname) p WHERE c.relkind IN ('r', 'v');
CREATE TEMP VIEW pg_attrdef AS
  SELECT CAST(4000000 + c.oid * 100 + p.cid AS INTEGER) AS oid, c.oid AS adrelid, p.cid + 1 AS adnum,
         p.dflt_value AS adbin
  FROM pg_class c JOIN pragma_table_info(c.relname) p WHERE c.relkind = 'r' AND p.dflt_value IS NOT NULL;
CREATE TEMP VIEW pg_collation AS SELECT 100 AS oid, 'default' AS collname, 11 AS collnamespace;
CREATE TEMP VIEW pg_description AS
  SELECT NULL AS objoid, NULL AS classoid, NULL AS objsubid, NULL AS description WHERE 0;
CREATE TEMP VIEW pg_inherits AS
  SELECT NULL AS inhrelid, NULL AS inhparent, NULL AS inhseqno, NULL AS inhdetachpending WHERE 0;
CREATE TEMP VIEW pg_trigger AS
  SELECT NULL AS oid, NULL AS tgrelid, NULL AS tgname, NULL AS tgisinternal, NULL AS tgenabled,
         NULL AS tgparentid WHERE 0;
CREATE TEMP VIEW pg_depend AS
  SELECT NULL AS classid, NULL AS objid, NULL AS objsubid, NULL AS refclassid, NULL AS refobjid,
         NULL AS refobjsubid, NULL AS deptype WHERE 0;
CREATE TEMP VIEW pg_policy AS
  SELECT NULL AS oid, NULL AS polname, NULL AS polrelid, NULL AS polcmd, NULL AS polpermissive,
         NULL AS polroles, NULL AS polqual, NULL AS polwithcheck WHERE 0;
CREATE TEMP VIEW pg_statistic_ext AS
  SELECT NULL AS oid, NULL AS stxrelid, NULL AS stxname, NULL AS stxnamespace, NULL AS stxkeys,
         NULL AS stxkind, NULL AS stxstattarget WHERE 0;
CREATE TEMP VIEW pg_publication AS
  SELECT NULL AS oid, NULL AS pubname, NULL AS puballtables WHERE 0;
CREATE TEMP VIEW pg_publication_rel AS SELECT NULL AS prpubid, NULL AS prrelid, NULL AS prqual WHERE 0;
CREATE TEMP VIEW pg_publication_namespace AS SELECT NULL AS pnpubid, NULL AS pnnspid WHERE 0;
CREATE TEMP VIEW pg_rewrite AS SELECT NULL AS oid, NULL AS ev_class, NULL AS rulename WHERE 0;
CREATE TEMP VIEW pg_partitioned_table AS SELECT NULL AS partrelid WHERE 0;
CREATE TEMP VIEW pg_foreign_table AS SELECT NULL AS ftrelid, NULL AS ftserver WHERE 0;
CREATE TEMP VIEW pg_database AS
  SELECT 16384 AS oid, current_database() AS datname, 10 AS datdba, 6 AS encoding, 'c' AS datlocprovider,
         0 AS datistemplate, 1 AS datallowconn, -1 AS datconnlimit, 'C' AS datcollate, 'C' AS datctype,
         NULL AS daticulocale, NULL AS daticurules, NULL AS datlocale, NULL AS datacl;
CREATE TEMP VIEW pg_tablespace AS SELECT 1663 AS oid, 'pg_default' AS spcname;
CREATE TEMP VIEW pg_indexes AS
  SELECT 'public' AS schemaname, tablename, relname AS indexname, NULL AS tablespace, indexdef FROM pg_bucketdb_index;
CREATE TEMP VIEW information_schema_tables AS
  SELECT current_database() AS table_catalog, 'public' AS table_schema, m.name AS table_name,
         CASE m.type WHEN 'view' THEN 'VIEW' ELSE 'BASE TABLE' END AS table_type
  FROM pg_bucketdb_rel m WHERE m.type IN ('table', 'view');
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
  FROM pg_bucketdb_rel m JOIN pragma_table_info(m.name) p WHERE m.type = 'table';
CREATE TEMP VIEW information_schema_table_constraints AS
  SELECT current_database() AS constraint_catalog, 'public' AS constraint_schema, m.name || '_pkey' AS constraint_name,
         current_database() AS table_catalog, 'public' AS table_schema, m.name AS table_name, 'PRIMARY KEY' AS constraint_type
  FROM pg_bucketdb_rel m WHERE m.type = 'table' AND EXISTS (SELECT 1 FROM pragma_table_info(m.name) WHERE pk > 0)
  UNION ALL
  SELECT current_database(), 'public', json_extract(j.value, '$[0]'), current_database(), 'public', m.name,
         json_extract(j.value, '$[1]')
  FROM pg_bucketdb_rel m JOIN json_each(pg_named_constraints(m.sql)) j
  WHERE m.type = 'table' AND json_extract(j.value, '$[1]') <> 'PRIMARY KEY';
CREATE TEMP VIEW information_schema_constraint_column_usage AS
  SELECT current_database() AS constraint_catalog, 'public' AS constraint_schema, current_database() AS table_catalog,
         'public' AS table_schema, m.name AS table_name, p.name AS column_name, m.name || '_pkey' AS constraint_name
  FROM pg_bucketdb_rel m JOIN pragma_table_info(m.name) p WHERE m.type = 'table' AND p.pk > 0
  UNION ALL
  SELECT current_database(), 'public', current_database(), 'public', m.name, ii.name, il.name
  FROM pg_bucketdb_rel m JOIN pragma_index_list(m.name) il JOIN pragma_index_info(il.name) ii
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
    // A foreign key's name in CREATE TABLE: SQLite numbers them last declared first,
    // so id i of n is the (n - 1 - i)th named one, when every one of them is named.
    conn.create_scalar_function("pg_fk_name", 3, det, |ctx| {
        let sql = text(ctx, 0).unwrap_or_default();
        let (id, n) = (ctx.get::<i64>(1)?, ctx.get::<i64>(2)?);
        let named: Vec<serde_json::Value> = serde_json::from_str(&named_constraints(&sql)).unwrap_or_default();
        let fks: Vec<String> = named
            .iter()
            .filter(|c| c[1] == "FOREIGN KEY")
            .filter_map(|c| c[0].as_str().map(str::to_string))
            .collect();
        Ok(usize::try_from(n - 1 - id)
            .ok()
            .filter(|_| i64::try_from(fks.len()) == Ok(n))
            .and_then(|i| fks.get(i).cloned()))
    })?;
    conn.create_scalar_function("pg_type_oid", 1, det, |ctx| {
        // The catalog's OID tells varchar, char and the integer widths apart.
        let d = text(ctx, 0);
        Ok(match pg_names(d.as_deref().unwrap_or_default()).0 {
            "varchar" => 1043,
            "bpchar" => 1042,
            "int4" => 23,
            "int2" => 21,
            "float4" => 700,
            "time" => 1083,
            "interval" => 1186,
            _ => i64::from(types::oid_of_decl(d.as_deref())),
        })
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
    conn.create_scalar_function("pg_typmod", 1, det, |ctx| Ok(typmod(&text(ctx, 0).unwrap_or_default())))?;
    conn.create_scalar_function("format_type", 2, det, |ctx| {
        let oid = ctx.get::<i64>(0).unwrap_or(25);
        let m = ctx.get::<i64>(1).unwrap_or(-1);
        Ok(format_type(oid, m))
    })?;
    // pg_lookup(sql, x): the first column of sql's first row for ? = x. A function, not
    // a subquery, so an unqualified `oid` in x stays the caller's.
    conn.create_scalar_function("pg_lookup", 2, flags, |ctx| {
        let sql = text(ctx, 0).unwrap_or_default();
        let key = ctx.get::<rusqlite::types::Value>(1)?;
        // SAFETY: the handle is only used here, on the thread running the statement.
        let db = unsafe { ctx.get_connection()? };
        db.query_row(&sql, [key], |r| r.get::<_, rusqlite::types::Value>(0))
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(rusqlite::types::Value::Null),
                e => Err(e),
            })
    })?;
    // x::regclass in a select list (sql.rs): the relation's name.
    conn.create_scalar_function("pg_regclass", 1, flags, |ctx| {
        let key = ctx.get::<rusqlite::types::Value>(0)?;
        // SAFETY: as in pg_lookup.
        let db = unsafe { ctx.get_connection()? };
        db.query_row(
            "SELECT relname FROM pg_class WHERE oid = CAST(? AS INTEGER)",
            [key],
            |r| r.get::<_, rusqlite::types::Value>(0),
        )
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(rusqlite::types::Value::Null),
            e => Err(e),
        })
    })?;
    // x = ANY(list), as sql.rs writes it: a list in text, '{a,b}' or '1 2 3'.
    conn.create_scalar_function("pg_any", 2, det, |ctx| {
        let (Some(x), Some(list)) = (text(ctx, 0), text(ctx, 1)) else {
            return Ok(None);
        };
        let items = list.trim_matches(|c| c == '{' || c == '}');
        Ok(Some(
            items
                .split(|c: char| c == ',' || c.is_whitespace())
                .any(|i| i.trim_matches('"') == x),
        ))
    })?;
    // a ~ b, as the regex operators become (sql.rs): (text, pattern, case-insensitive).
    conn.create_scalar_function("pg_regex", 3, det, |ctx| {
        let (Some(s), Some(pat)) = (text(ctx, 0), text(ctx, 1)) else {
            return Ok(None);
        };
        let re = regex_lite::RegexBuilder::new(&pat)
            .case_insensitive(ctx.get::<i64>(2).unwrap_or(0) != 0)
            .build()
            .map_err(|e| rusqlite::Error::UserFunctionError(format!("invalid regular expression: {e}").into()))?;
        Ok(Some(re.is_match(&s)))
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

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    #[test]
    fn catalog_hides_internal_tables() {
        let conn = Connection::open_in_memory().unwrap();
        super::register(&conn, "app").unwrap();
        conn.execute_batch(super::CATALOG).unwrap();
        conn.execute_batch(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT UNIQUE);
             CREATE TABLE sqlitex (a TEXT);
             CREATE TABLE bucketdb_kv (key BLOB PRIMARY KEY, value BLOB, expires_at INTEGER) WITHOUT ROWID;
             CREATE INDEX bucketdb_kv_exp ON bucketdb_kv (expires_at);
             CREATE TABLE seq (id INTEGER PRIMARY KEY AUTOINCREMENT);",
        )
        .unwrap();
        let names = |sql: &str| -> Vec<String> {
            let mut stmt = conn.prepare(sql).unwrap();
            stmt.query_map([], |r| r.get(0)).unwrap().map(Result::unwrap).collect()
        };
        assert_eq!(
            names("SELECT relname FROM pg_class WHERE relkind = 'r' ORDER BY relname"),
            ["seq", "sqlitex", "users"]
        );
        assert_eq!(
            names("SELECT table_name FROM information_schema_tables ORDER BY table_name"),
            ["seq", "sqlitex", "users"]
        );
        assert_eq!(
            names("SELECT DISTINCT table_name FROM information_schema_table_constraints ORDER BY 1"),
            ["seq", "users"]
        );
        assert_eq!(
            names("SELECT DISTINCT tablename FROM pg_indexes ORDER BY 1"),
            ["seq", "users"]
        );
        assert_eq!(names("SELECT count(DISTINCT attrelid) || '' FROM pg_attribute"), ["3"]);
        // Hidden from the catalogs, not from SQL.
        assert_eq!(names("SELECT count(*) || '' FROM bucketdb_kv"), ["0"]);
    }
}
