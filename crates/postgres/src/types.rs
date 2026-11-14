//! Postgres types as bucketdb speaks them: the OID a column goes out as, chosen from
//! its declared type, and the text and binary forms of values both ways.
//!
//! SQLite keeps a column's declared type as written, so a table made through
//! bucketdb remembers `BOOLEAN`, `TIMESTAMPTZ` or `UUID`, and those decide the OID.
//! Booleans are stored as 1 and 0, timestamps as UTC text `YYYY-MM-DD HH:MM:SS.ffffff`,
//! dates as `YYYY-MM-DD`, UUIDs as lower-case text: forms that sort as they compare.

use rusqlite::types::{Value, ValueRef};

pub const BOOL: u32 = 16;
pub const BYTEA: u32 = 17;
pub const INT8: u32 = 20;
pub const INT2: u32 = 21;
pub const INT4: u32 = 23;
pub const TEXT: u32 = 25;
pub const JSON: u32 = 114;
pub const FLOAT4: u32 = 700;
pub const FLOAT8: u32 = 701;
pub const DATE: u32 = 1082;
pub const NUMERIC: u32 = 1700;
pub const TIMESTAMP: u32 = 1114;
pub const TIMESTAMPTZ: u32 = 1184;
pub const UUID: u32 = 2950;
pub const JSONB: u32 = 3802;

/// The boolean columns of the catalog views (funcs::CATALOG), which are expressions
/// with no declared type: by name, so they go out as t/f, which psql tests for.
pub fn catalog_bool(name: &str) -> bool {
    matches!(
        name,
        "attnotnull"
            | "atthasdef"
            | "attisdropped"
            | "attislocal"
            | "relhasindex"
            | "relhasrules"
            | "relhastriggers"
            | "relhassubclass"
            | "relrowsecurity"
            | "relforcerowsecurity"
            | "relhasoids"
            | "relispartition"
            | "indisunique"
            | "indisprimary"
            | "indisexclusion"
            | "indimmediate"
            | "indisclustered"
            | "indisvalid"
            | "indisready"
            | "indislive"
            | "indisreplident"
            | "condeferrable"
            | "condeferred"
            | "convalidated"
            | "conislocal"
            | "connoinherit"
            | "inhdetachpending"
            | "polpermissive"
            | "puballtables"
    )
}

/// The OID a column of this declared type goes out as.
pub fn oid_of_decl(decl: Option<&str>) -> u32 {
    let Some(d) = decl else { return TEXT };
    let d = d.to_ascii_uppercase();
    let d = d.trim();
    if d.starts_with("BOOL") {
        BOOL
    } else if d.starts_with("TIMESTAMPTZ") || d.contains("WITH TIME ZONE") {
        TIMESTAMPTZ
    } else if d.starts_with("TIMESTAMP") || d.starts_with("DATETIME") {
        TIMESTAMP
    } else if d == "DATE" {
        DATE
    } else if d == "UUID" {
        UUID
    } else if d == "JSONB" {
        JSONB
    } else if d == "JSON" {
        JSON
    } else if d.starts_with("BYTEA") || d.starts_with("BLOB") {
        BYTEA
    } else if d.contains("INT") && !d.starts_with("INTERVAL") && !d.contains("POINT") {
        INT8
    } else if d.starts_with("NUMERIC") || d.starts_with("DECIMAL") {
        NUMERIC
    } else if ["REAL", "FLOAT", "DOUBLE"].iter().any(|t| d.starts_with(t)) {
        FLOAT8
    } else {
        TEXT
    }
}

/// The column decl SQLite stores for a Postgres type: Postgres' own name, shortened
/// so that SQLite's affinity rules read it right.
pub fn sqlite_decl(pg: &str) -> String {
    let u = pg.to_ascii_uppercase();
    let base = u.split('(').next().unwrap_or("").trim().to_string();
    let args = u.find('(').map(|i| u[i..].to_string()).unwrap_or_default();
    match base.as_str() {
        "SERIAL" | "SERIAL4" | "INT" | "INT4" | "INTEGER" => "INT4".into(),
        "BIGSERIAL" | "SERIAL8" | "INT8" | "BIGINT" => "BIGINT".into(),
        "SMALLSERIAL" | "SERIAL2" | "INT2" | "SMALLINT" => "SMALLINT".into(),
        "BOOL" | "BOOLEAN" => "BOOLEAN".into(),
        "FLOAT4" | "REAL" => "REAL".into(),
        "FLOAT8" | "DOUBLE PRECISION" | "DOUBLE" | "FLOAT" => "DOUBLE".into(),
        "NUMERIC" | "DECIMAL" => format!("NUMERIC{args}"),
        "CHARACTER VARYING" | "VARCHAR" => format!("VARCHAR{args}"),
        "CHARACTER" | "CHAR" | "BPCHAR" => format!("CHAR{args}"),
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => "TIMESTAMPTZ".into(),
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => "TIMESTAMP".into(),
        "TIME" | "TIME WITHOUT TIME ZONE" | "TIMETZ" | "TIME WITH TIME ZONE" => "TIME_TEXT".into(),
        "INTERVAL" => "INTERVAL_TEXT".into(),
        "BYTEA" => "BYTEA".into(),
        "UUID" | "JSON" | "JSONB" | "DATE" | "TEXT" => base,
        _ => "TEXT".into(),
    }
}

/// Whether a type is one of the serials, whose column takes the next rowid.
pub fn is_serial(pg: &str) -> bool {
    matches!(
        pg.to_ascii_uppercase().as_str(),
        "SERIAL" | "SERIAL4" | "BIGSERIAL" | "SERIAL8" | "SMALLSERIAL" | "SERIAL2"
    )
}

/// The text form of a value, as Postgres writes it for the type.
pub fn to_text(v: ValueRef<'_>, oid: u32) -> Option<Vec<u8>> {
    Some(match v {
        ValueRef::Null => return None,
        ValueRef::Integer(i) if oid == BOOL => {
            if i != 0 {
                b"t".to_vec()
            } else {
                b"f".to_vec()
            }
        }
        ValueRef::Integer(i) => i.to_string().into_bytes(),
        ValueRef::Real(f) => float_text(f).into_bytes(),
        ValueRef::Text(t) if oid == BOOL => {
            if truthy(t) {
                b"t".to_vec()
            } else {
                b"f".to_vec()
            }
        }
        ValueRef::Text(t) => t.to_vec(),
        ValueRef::Blob(b) if oid == BYTEA || oid == TEXT => {
            let mut s = b"\\x".to_vec();
            s.extend(hex::encode(b).into_bytes());
            s
        }
        ValueRef::Blob(b) => b.to_vec(),
    })
}

fn float_text(f: f64) -> String {
    if f.is_nan() {
        "NaN".into()
    } else if f.is_infinite() {
        if f > 0.0 { "Infinity".into() } else { "-Infinity".into() }
    } else {
        format!("{f}")
    }
}

fn truthy(t: &[u8]) -> bool {
    matches!(
        t.to_ascii_lowercase().as_slice(),
        b"t" | b"true" | b"y" | b"yes" | b"on" | b"1"
    )
}

/// Microseconds from 2000-01-01, Postgres' epoch.
fn pg_epoch() -> chrono::NaiveDateTime {
    chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .unwrap_or_default()
}

fn parse_ts(s: &str) -> Option<chrono::NaiveDateTime> {
    let s = s.trim();
    let s = s.strip_suffix('Z').unwrap_or(s);
    // A trailing +00 / +00:00 (UTC) is what bucketdb or a client may have written.
    let s = s
        .rfind(['+', '-'])
        .filter(|&i| i > 10 && s[i + 1..].chars().all(|c| c.is_ascii_digit() || c == ':'))
        .map_or(s, |i| &s[..i]);
    for f in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ] {
        if let Ok(t) = chrono::NaiveDateTime::parse_from_str(s, f) {
            return Some(t);
        }
        if let Ok(d) = chrono::NaiveDate::parse_from_str(s, f) {
            return d.and_hms_opt(0, 0, 0);
        }
    }
    None
}

pub fn ts_text(t: chrono::NaiveDateTime) -> String {
    let s = t.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

/// The binary form of a value for the type; Err when it cannot be one.
pub fn to_binary(v: ValueRef<'_>, oid: u32) -> Result<Option<Vec<u8>>, String> {
    if matches!(v, ValueRef::Null) {
        return Ok(None);
    }
    let text = || -> String {
        match v {
            ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned(),
            ValueRef::Integer(i) => i.to_string(),
            ValueRef::Real(f) => float_text(f),
            _ => String::new(),
        }
    };
    let bad = |what: &str| format!("value {:?} is not a {what}", text());
    Ok(Some(match oid {
        BOOL => vec![u8::from(match v {
            ValueRef::Integer(i) => i != 0,
            ValueRef::Real(f) => f != 0.0,
            ValueRef::Text(t) => truthy(t),
            _ => return Err(bad("boolean")),
        })],
        INT8 | INT4 | INT2 => {
            let i = match v {
                ValueRef::Integer(i) => i,
                ValueRef::Real(f) => f as i64,
                _ => text().trim().parse().map_err(|_| bad("integer"))?,
            };
            match oid {
                INT4 => (i as i32).to_be_bytes().to_vec(),
                INT2 => (i as i16).to_be_bytes().to_vec(),
                _ => i.to_be_bytes().to_vec(),
            }
        }
        FLOAT8 | FLOAT4 => {
            let f = match v {
                ValueRef::Integer(i) => i as f64,
                ValueRef::Real(f) => f,
                _ => text().trim().parse().map_err(|_| bad("number"))?,
            };
            if oid == FLOAT4 {
                (f as f32).to_be_bytes().to_vec()
            } else {
                f.to_be_bytes().to_vec()
            }
        }
        BYTEA => match v {
            ValueRef::Blob(b) => b.to_vec(),
            _ => text().into_bytes(),
        },
        DATE => {
            let t = parse_ts(&text()).ok_or_else(|| bad("date"))?;
            let days = (t.date() - pg_epoch().date()).num_days() as i32;
            days.to_be_bytes().to_vec()
        }
        TIMESTAMP | TIMESTAMPTZ => {
            let t = parse_ts(&text()).ok_or_else(|| bad("timestamp"))?;
            let us = (t - pg_epoch()).num_microseconds().ok_or_else(|| bad("timestamp"))?;
            us.to_be_bytes().to_vec()
        }
        UUID => {
            let h: String = text().chars().filter(char::is_ascii_hexdigit).collect();
            hex::decode(&h)
                .ok()
                .filter(|b| b.len() == 16)
                .ok_or_else(|| bad("uuid"))?
        }
        JSONB => {
            let mut b = vec![1u8];
            b.extend(text().into_bytes());
            b
        }
        NUMERIC => numeric_binary(text().trim()).ok_or_else(|| bad("numeric"))?,
        _ => match v {
            ValueRef::Blob(b) => {
                let mut s = b"\\x".to_vec();
                s.extend(hex::encode(b).into_bytes());
                s
            }
            _ => text().into_bytes(),
        },
    }))
}

/// A parameter the client sent, as the value SQLite stores.
pub fn param(raw: Option<&[u8]>, oid: u32, binary: bool) -> Result<Value, String> {
    let Some(b) = raw else { return Ok(Value::Null) };
    let s = || String::from_utf8_lossy(b).into_owned();
    let need = |n: usize, what: &str| {
        if b.len() == n {
            Ok(())
        } else {
            Err(format!("binary {what} of {} bytes", b.len()))
        }
    };
    if binary {
        return Ok(match oid {
            BOOL => {
                need(1, "boolean")?;
                Value::Integer(i64::from(b[0] != 0))
            }
            INT2 => {
                need(2, "int2")?;
                Value::Integer(i64::from(i16::from_be_bytes([b[0], b[1]])))
            }
            INT4 => {
                need(4, "int4")?;
                Value::Integer(i64::from(i32::from_be_bytes([b[0], b[1], b[2], b[3]])))
            }
            INT8 => {
                need(8, "int8")?;
                Value::Integer(i64::from_be_bytes(b.try_into().unwrap_or_default()))
            }
            FLOAT4 => {
                need(4, "float4")?;
                Value::Real(f64::from(f32::from_be_bytes([b[0], b[1], b[2], b[3]])))
            }
            FLOAT8 => {
                need(8, "float8")?;
                Value::Real(f64::from_be_bytes(b.try_into().unwrap_or_default()))
            }
            BYTEA => Value::Blob(b.to_vec()),
            DATE => {
                need(4, "date")?;
                let days = i32::from_be_bytes([b[0], b[1], b[2], b[3]]);
                let d = pg_epoch().date() + chrono::Duration::days(i64::from(days));
                Value::Text(d.format("%Y-%m-%d").to_string())
            }
            TIMESTAMP | TIMESTAMPTZ => {
                need(8, "timestamp")?;
                let us = i64::from_be_bytes(b.try_into().unwrap_or_default());
                Value::Text(ts_text(pg_epoch() + chrono::Duration::microseconds(us)))
            }
            UUID => {
                need(16, "uuid")?;
                let h = hex::encode(b);
                Value::Text(format!(
                    "{}-{}-{}-{}-{}",
                    &h[..8],
                    &h[8..12],
                    &h[12..16],
                    &h[16..20],
                    &h[20..]
                ))
            }
            JSONB => Value::Text(String::from_utf8_lossy(b.get(1..).unwrap_or_default()).into_owned()),
            NUMERIC => {
                let t = numeric_text(b).ok_or("malformed binary numeric")?;
                t.parse::<i64>()
                    .map(Value::Integer)
                    .or_else(|_| t.parse::<f64>().map(Value::Real))
                    .unwrap_or(Value::Text(t))
            }
            _ => Value::Text(s()),
        });
    }
    Ok(match oid {
        BOOL => Value::Integer(i64::from(truthy(b))),
        INT2 | INT4 | INT8 => s().trim().parse().map_or_else(|_| Value::Text(s()), Value::Integer),
        FLOAT4 | FLOAT8 => s().trim().parse().map_or_else(|_| Value::Text(s()), Value::Real),
        NUMERIC => {
            let t = s();
            let t = t.trim();
            t.parse::<i64>()
                .map(Value::Integer)
                .or_else(|_| t.parse::<f64>().map(Value::Real))
                .unwrap_or(Value::Text(t.to_string()))
        }
        BYTEA => {
            let t = s();
            t.strip_prefix("\\x")
                .and_then(|h| hex::decode(h).ok())
                .map_or(Value::Text(t), Value::Blob)
        }
        TIMESTAMPTZ => {
            let t = s();
            parse_tstz(&t)
                .or_else(|| parse_ts(&t))
                .map_or(Value::Text(t), |ts| Value::Text(ts_text(ts)))
        }
        TIMESTAMP => {
            let t = s();
            parse_ts(&t).map_or(Value::Text(t), |ts| Value::Text(ts_text(ts)))
        }
        UUID => Value::Text(s().to_ascii_lowercase()),
        _ => Value::Text(s()),
    })
}

/// A timestamp with an offset, in UTC: `2026-11-13 10:00:00-03`, `...+05:30`, RFC 3339.
fn parse_tstz(s: &str) -> Option<chrono::NaiveDateTime> {
    let s = s.trim();
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(t.naive_utc());
    }
    // Postgres writes the offset as +HH, +HH:MM or +HHMM after the time.
    let i = s.rfind(['+', '-']).filter(|&i| i > 10)?;
    let (body, off) = (&s[..i], &s[i..]);
    let digits: String = off[1..].chars().filter(char::is_ascii_digit).collect();
    let (h, m) = match digits.len() {
        2 => (digits.parse::<i64>().ok()?, 0),
        4 => (digits[..2].parse::<i64>().ok()?, digits[2..].parse::<i64>().ok()?),
        _ => return None,
    };
    let secs = (h * 3600 + m * 60) * if off.starts_with('-') { -1 } else { 1 };
    let local = parse_ts(body)?;
    Some(local - chrono::Duration::seconds(secs))
}

/// Postgres' binary numeric: ndigits, weight, sign, dscale, then base-10000 digits.
fn numeric_binary(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let header = |n: i16, w: i16, sign: u16, scale: i16, out: &mut Vec<u8>| {
        out.extend_from_slice(&n.to_be_bytes());
        out.extend_from_slice(&w.to_be_bytes());
        out.extend_from_slice(&sign.to_be_bytes());
        out.extend_from_slice(&scale.to_be_bytes());
    };
    if s.eq_ignore_ascii_case("nan") {
        header(0, 0, 0xC000, 0, &mut out);
        return Some(out);
    }
    let (neg, s) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    // A float may come in exponent form: write it out in full.
    let owned;
    let s = if s.contains(['e', 'E']) {
        let f: f64 = s.parse().ok()?;
        owned = format!("{f}");
        owned.as_str()
    } else {
        s
    };
    let (int, frac) = s.split_once('.').unwrap_or((s, ""));
    if !int.chars().chain(frac.chars()).all(|c| c.is_ascii_digit()) {
        return None;
    }
    let dscale = frac.len() as i16;
    let int = int.trim_start_matches('0');
    let pad_int = (4 - int.len() % 4) % 4;
    let pad_frac = (4 - frac.len() % 4) % 4;
    let all = format!("{}{int}{frac}{}", "0".repeat(pad_int), "0".repeat(pad_frac));
    let mut groups: Vec<i16> = all
        .as_bytes()
        .chunks(4)
        .map(|c| std::str::from_utf8(c).ok()?.parse().ok())
        .collect::<Option<_>>()?;
    let mut weight = ((int.len() + pad_int) / 4) as i16 - 1;
    while groups.first() == Some(&0) {
        groups.remove(0);
        weight -= 1;
    }
    while groups.last() == Some(&0) {
        groups.pop();
    }
    if groups.is_empty() {
        header(0, 0, 0, dscale, &mut out);
        return Some(out);
    }
    header(
        groups.len() as i16,
        weight,
        if neg { 0x4000 } else { 0 },
        dscale,
        &mut out,
    );
    for g in groups {
        out.extend_from_slice(&g.to_be_bytes());
    }
    Some(out)
}

/// The decimal text of a binary numeric.
fn numeric_text(b: &[u8]) -> Option<String> {
    let rd = |i: usize| -> Option<i16> { Some(i16::from_be_bytes([*b.get(i)?, *b.get(i + 1)?])) };
    let (n, weight, sign, dscale) = (rd(0)? as usize, rd(2)?, rd(4)? as u16, rd(6)?.max(0) as usize);
    if sign == 0xC000 {
        return Some("NaN".into());
    }
    let digits: Vec<i16> = (0..n).map(|i| rd(8 + 2 * i)).collect::<Option<_>>()?;
    let mut int = String::new();
    for i in 0..=weight.max(-1) {
        let d = digits.get(i as usize).copied().unwrap_or(0);
        if int.is_empty() {
            int.push_str(&d.to_string());
        } else {
            int.push_str(&format!("{d:04}"));
        }
    }
    if int.is_empty() {
        int.push('0');
    }
    let mut frac = String::new();
    let first_frac = (weight + 1) as i64;
    let mut k = first_frac;
    while frac.len() < dscale {
        let d = if k < 0 {
            0
        } else {
            digits.get(k as usize).copied().unwrap_or(0)
        };
        frac.push_str(&format!("{d:04}"));
        k += 1;
    }
    // Groups before the first digit (negative weight) are leading zeros of the fraction.
    if weight < -1 {
        frac = "0000".repeat((-weight - 1) as usize) + &frac;
    }
    frac.truncate(dscale);
    let sign = if sign == 0x4000 { "-" } else { "" };
    Some(if dscale > 0 {
        format!("{sign}{int}.{frac}")
    } else {
        format!("{sign}{int}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_round_trip() {
        for s in [
            "0",
            "1",
            "-1",
            "10000",
            "123456.789",
            "0.0001",
            "0.00012",
            "-98765.4321",
            "510000",
            "1.5",
        ] {
            let b = numeric_binary(s).unwrap();
            assert_eq!(numeric_text(&b).unwrap(), s, "{s}");
        }
    }

    #[test]
    fn tstz_to_utc() {
        assert_eq!(
            ts_text(parse_tstz("2026-11-13 10:00:00-03").unwrap()),
            "2026-11-13 13:00:00"
        );
        assert_eq!(
            ts_text(parse_tstz("2026-11-13T10:00:00.5+05:30").unwrap()),
            "2026-11-13 04:30:00.5"
        );
    }
}
