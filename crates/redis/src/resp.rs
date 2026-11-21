//! RESP2, the Redis protocol: a command is an array of bulk strings (or, typed by
//! hand, an inline line of words); a reply is one of a few typed values.

use std::io::{self, BufRead, Read, Write};

/// The longest inline command, and the longest header line of an array or bulk.
const MAX_LINE: usize = 64 << 10;

/// What one command may carry; past either limit it is a protocol error.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_args: usize,
    /// Bytes of all its arguments together.
    pub max_bytes: usize,
}

fn protocol(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Reads one command. Ok(None) at the end of the stream between commands; an
/// error of kind InvalidData is a protocol error, to answer before closing.
///
/// # Errors
///
/// On a read error, a stream that ends inside a command, or a malformed one.
pub fn read_command(r: &mut impl BufRead, limits: Limits) -> io::Result<Option<Vec<Vec<u8>>>> {
    let Some(line) = read_line(r)? else {
        return Ok(None);
    };
    if line.first() != Some(&b'*') {
        let args = line
            .split(u8::is_ascii_whitespace)
            .filter(|a| !a.is_empty())
            .map(<[u8]>::to_vec)
            .collect();
        return Ok(Some(args));
    }
    let n = parse_len(&line[1..], "multibulk", true)?;
    if n > limits.max_args {
        return Err(protocol("invalid multibulk length"));
    }
    let mut args = Vec::with_capacity(n.min(1024));
    let mut total = 0usize;
    for _ in 0..n {
        let line = read_line(r)?.ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        if line.first() != Some(&b'$') {
            let got = line.first().map_or(String::new(), |b| (*b as char).to_string());
            return Err(protocol(format!("expected '$', got '{got}'")));
        }
        let len = parse_len(&line[1..], "bulk", false)?;
        total = total.saturating_add(len);
        if total > limits.max_bytes {
            return Err(protocol("invalid bulk length"));
        }
        // Grown as the bytes arrive: a length alone reserves no memory.
        let mut arg = Vec::with_capacity(len.min(1 << 20));
        r.by_ref().take(len as u64 + 2).read_to_end(&mut arg)?;
        if arg.len() < len + 2 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        if arg[len..] != *b"\r\n" {
            return Err(protocol("expected CRLF after a bulk string"));
        }
        arg.truncate(len);
        args.push(arg);
    }
    Ok(Some(args))
}

/// A line without its CRLF (or bare LF, as typed). None at the end of the stream.
fn read_line(r: &mut impl BufRead) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let buf = r.fill_buf()?;
        if buf.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err(io::ErrorKind::UnexpectedEof.into())
            };
        }
        if let Some(i) = buf.iter().position(|&b| b == b'\n') {
            line.extend_from_slice(&buf[..i]);
            r.consume(i + 1);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(Some(line));
        }
        let n = buf.len();
        line.extend_from_slice(buf);
        r.consume(n);
        if line.len() > MAX_LINE {
            return Err(protocol("too big inline request"));
        }
    }
}

/// A length of an array or bulk header: digits only. With null, a negative count
/// (a null array) is taken as empty.
fn parse_len(digits: &[u8], what: &str, null: bool) -> io::Result<usize> {
    if null && digits.first() == Some(&b'-') && digits.len() > 1 && digits[1..].iter().all(u8::is_ascii_digit) {
        return Ok(0);
    }
    if digits.is_empty() || digits.len() > 18 || !digits.iter().all(u8::is_ascii_digit) {
        return Err(protocol(format!("invalid {what} length")));
    }
    Ok(digits.iter().fold(0, |n, d| n * 10 + usize::from(d - b'0')))
}

/// A reply, as RESP2 writes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Simple(String),
    /// The message starts with its code: `ERR ...`, `WRONGTYPE ...`.
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Nil,
    Array(Vec<Reply>),
    /// A null array: an EXEC a watched key aborted.
    NilArray,
}

impl Reply {
    pub fn ok() -> Reply {
        Reply::Simple("OK".into())
    }

    pub fn err(msg: impl Into<String>) -> Reply {
        Reply::Error(msg.into())
    }

    pub fn bulk_or_nil(v: Option<Vec<u8>>) -> Reply {
        v.map_or(Reply::Nil, Reply::Bulk)
    }

    /// Writes the reply.
    ///
    /// # Errors
    ///
    /// When the writer fails.
    pub fn write(&self, w: &mut impl Write) -> io::Result<()> {
        match self {
            Reply::Simple(s) => write!(w, "+{}\r\n", one_line(s)),
            Reply::Error(s) => write!(w, "-{}\r\n", one_line(s)),
            Reply::Int(n) => write!(w, ":{n}\r\n"),
            Reply::Bulk(b) => {
                write!(w, "${}\r\n", b.len())?;
                w.write_all(b)?;
                w.write_all(b"\r\n")
            }
            Reply::Nil => w.write_all(b"$-1\r\n"),
            Reply::NilArray => w.write_all(b"*-1\r\n"),
            Reply::Array(items) => {
                write!(w, "*{}\r\n", items.len())?;
                items.iter().try_for_each(|i| i.write(w))
            }
        }
    }
}

/// A simple string or error must not break its line.
fn one_line(s: &str) -> String {
    s.replace(['\r', '\n'], " ")
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: Limits = Limits {
        max_args: 8,
        max_bytes: 64,
    };

    fn read(input: &[u8]) -> io::Result<Option<Vec<Vec<u8>>>> {
        read_command(&mut io::BufReader::new(input), LIMITS)
    }

    fn args(v: &[&str]) -> Option<Vec<Vec<u8>>> {
        Some(v.iter().map(|a| a.as_bytes().to_vec()).collect())
    }

    #[test]
    fn reads_commands() {
        assert_eq!(read(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").unwrap(), args(&["GET", "k"]));
        assert_eq!(read(b"*1\r\n$0\r\n\r\n").unwrap(), args(&[""]));
        assert_eq!(read(b"*-1\r\n").unwrap(), args(&[]));
        assert_eq!(read(b"PING\r\n").unwrap(), args(&["PING"]));
        assert_eq!(read(b"set  a b\n").unwrap(), args(&["set", "a", "b"]));
        assert_eq!(read(b"").unwrap(), None);
        // Binary-safe: CRLF inside a bulk string is data.
        assert_eq!(read(b"*1\r\n$4\r\na\r\nb\r\n").unwrap(), args(&["a\r\nb"]));
        let mut two = io::BufReader::new(&b"*1\r\n$1\r\na\r\n*1\r\n$1\r\nb\r\n"[..]);
        assert_eq!(read_command(&mut two, LIMITS).unwrap(), args(&["a"]));
        assert_eq!(read_command(&mut two, LIMITS).unwrap(), args(&["b"]));
        assert_eq!(read_command(&mut two, LIMITS).unwrap(), None);
    }

    #[test]
    fn refuses_malformed_commands() {
        let kind = |input: &[u8]| read(input).unwrap_err().kind();
        assert_eq!(kind(b"*9\r\n"), io::ErrorKind::InvalidData); // past max_args
        assert_eq!(kind(b"*1\r\n$65\r\n"), io::ErrorKind::InvalidData); // past max_bytes
        assert_eq!(kind(b"*x\r\n"), io::ErrorKind::InvalidData);
        assert_eq!(kind(b"*1\r\n+GET\r\n"), io::ErrorKind::InvalidData);
        assert_eq!(kind(b"*1\r\n$1\r\nabc\r\n"), io::ErrorKind::InvalidData);
        assert_eq!(kind(b"*1\r\n$-1\r\n"), io::ErrorKind::InvalidData);
        assert_eq!(kind(b"*1\r\n$99999999999999999999\r\n"), io::ErrorKind::InvalidData);
        assert_eq!(kind(b"*2\r\n$1\r\na\r\n"), io::ErrorKind::UnexpectedEof);
        assert_eq!(kind(b"*1\r\n$5\r\nab"), io::ErrorKind::UnexpectedEof);
        assert_eq!(kind(&vec![b'a'; MAX_LINE + 2]), io::ErrorKind::InvalidData);
    }

    #[test]
    fn writes_replies() {
        let mut out = Vec::new();
        Reply::Array(vec![
            Reply::ok(),
            Reply::err("ERR bad\r\nthing"),
            Reply::Int(-2),
            Reply::Bulk(b"v".to_vec()),
            Reply::Nil,
            Reply::Array(vec![]),
        ])
        .write(&mut out)
        .unwrap();
        assert_eq!(out, b"*6\r\n+OK\r\n-ERR bad  thing\r\n:-2\r\n$1\r\nv\r\n$-1\r\n*0\r\n");
    }
}
