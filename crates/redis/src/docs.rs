//! The commands it speaks, as `COMMAND DOCS` says them: what redis-cli builds
//! its help, its hints and its Tab completion from. Only these: a command
//! completed is one that runs.

use crate::resp::Reply;

/// A command: its name, what it does, its group, and its arguments as redis-cli hints them.
struct Doc {
    name: &'static str,
    summary: &'static str,
    group: &'static str,
    args: &'static [&'static str],
}

const fn doc(name: &'static str, summary: &'static str, group: &'static str, args: &'static [&'static str]) -> Doc {
    Doc {
        name,
        summary,
        group,
        args,
    }
}

const DOCS: &[Doc] = &[
    // Strings.
    doc("get", "Returns the string value of a key.", "string", &["key"]),
    doc(
        "set",
        "Sets the string value of a key, ignoring its type. The key is created if it doesn't exist.",
        "string",
        &[
            "key",
            "value",
            "[NX|XX]",
            "[GET]",
            "[EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-milliseconds|KEEPTTL]",
        ],
    ),
    doc(
        "setnx",
        "Set the string value of a key only when the key doesn't exist.",
        "string",
        &["key", "value"],
    ),
    doc(
        "setex",
        "Sets the string value and expiration time of a key.",
        "string",
        &["key", "seconds", "value"],
    ),
    doc(
        "psetex",
        "Sets both string value and expiration time in milliseconds of a key.",
        "string",
        &["key", "milliseconds", "value"],
    ),
    doc(
        "getdel",
        "Returns the string value of a key after deleting the key.",
        "string",
        &["key"],
    ),
    doc(
        "getex",
        "Returns the string value of a key after setting its expiration time.",
        "string",
        &[
            "key",
            "[EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-milliseconds|PERSIST]",
        ],
    ),
    doc(
        "mget",
        "Atomically returns the string values of one or more keys.",
        "string",
        &["key", "[key ...]"],
    ),
    doc(
        "mset",
        "Atomically creates or modifies the string values of one or more keys.",
        "string",
        &["key", "value", "[key value ...]"],
    ),
    doc(
        "msetnx",
        "Atomically modifies the string values of one or more keys only when all keys don't exist.",
        "string",
        &["key", "value", "[key value ...]"],
    ),
    doc("strlen", "Returns the length of a string value.", "string", &["key"]),
    doc(
        "append",
        "Appends a string to the value of a key. Creates the key if it doesn't exist.",
        "string",
        &["key", "value"],
    ),
    doc(
        "incr",
        "Increments the integer value of a key by one. Uses 0 as initial value if the key doesn't exist.",
        "string",
        &["key"],
    ),
    doc(
        "decr",
        "Decrements the integer value of a key by one. Uses 0 as initial value if the key doesn't exist.",
        "string",
        &["key"],
    ),
    doc(
        "incrby",
        "Increments the integer value of a key by a number. Uses 0 as initial value if the key doesn't exist.",
        "string",
        &["key", "increment"],
    ),
    doc(
        "decrby",
        "Decrements a number from the integer value of a key. Uses 0 as initial value if the key doesn't exist.",
        "string",
        &["key", "decrement"],
    ),
    doc(
        "incrbyfloat",
        "Increment the floating point value of a key by a number. Uses 0 as initial value if the key doesn't exist.",
        "string",
        &["key", "increment"],
    ),
    // Keys.
    doc("del", "Deletes one or more keys.", "generic", &["key", "[key ...]"]),
    doc(
        "unlink",
        "Asynchronously deletes one or more keys.",
        "generic",
        &["key", "[key ...]"],
    ),
    doc(
        "exists",
        "Determines whether one or more keys exist.",
        "generic",
        &["key", "[key ...]"],
    ),
    doc(
        "type",
        "Determines the type of value stored at a key.",
        "generic",
        &["key"],
    ),
    doc(
        "expire",
        "Sets the expiration time of a key in seconds.",
        "generic",
        &["key", "seconds", "[NX|XX|GT|LT]"],
    ),
    doc(
        "pexpire",
        "Sets the expiration time of a key in milliseconds.",
        "generic",
        &["key", "milliseconds", "[NX|XX|GT|LT]"],
    ),
    doc(
        "expireat",
        "Sets the expiration time of a key to a Unix timestamp.",
        "generic",
        &["key", "unix-time-seconds", "[NX|XX|GT|LT]"],
    ),
    doc(
        "pexpireat",
        "Sets the expiration time of a key to a Unix milliseconds timestamp.",
        "generic",
        &["key", "unix-time-milliseconds", "[NX|XX|GT|LT]"],
    ),
    doc(
        "ttl",
        "Returns the expiration time in seconds of a key.",
        "generic",
        &["key"],
    ),
    doc(
        "pttl",
        "Returns the expiration time in milliseconds of a key.",
        "generic",
        &["key"],
    ),
    doc("persist", "Removes the expiration time of a key.", "generic", &["key"]),
    doc(
        "expiretime",
        "Returns the expiration time of a key as a Unix timestamp.",
        "generic",
        &["key"],
    ),
    doc(
        "pexpiretime",
        "Returns the expiration time of a key as a Unix milliseconds timestamp.",
        "generic",
        &["key"],
    ),
    doc(
        "scan",
        "Iterates over the key names in the database.",
        "generic",
        &["cursor", "[MATCH pattern]", "[COUNT count]", "[TYPE type]"],
    ),
    doc(
        "keys",
        "Returns all key names that match a pattern.",
        "generic",
        &["pattern"],
    ),
    doc(
        "rename",
        "Renames a key and overwrites the destination.",
        "generic",
        &["key", "newkey"],
    ),
    doc(
        "renamenx",
        "Renames a key only when the target key name doesn't exist.",
        "generic",
        &["key", "newkey"],
    ),
    doc(
        "randomkey",
        "Returns a random key name from the database.",
        "generic",
        &[],
    ),
    doc("dbsize", "Returns the number of keys in the database.", "server", &[]),
    doc("flushdb", "Remove all keys from the current database.", "server", &[]),
    // Hashes.
    doc(
        "hset",
        "Creates or modifies the value of a field in a hash.",
        "hash",
        &["key", "field", "value", "[field value ...]"],
    ),
    doc(
        "hmset",
        "Sets the values of multiple fields.",
        "hash",
        &["key", "field", "value", "[field value ...]"],
    ),
    doc(
        "hsetnx",
        "Sets the value of a field in a hash only when the field doesn't exist.",
        "hash",
        &["key", "field", "value"],
    ),
    doc(
        "hget",
        "Returns the value of a field in a hash.",
        "hash",
        &["key", "field"],
    ),
    doc(
        "hmget",
        "Returns the values of all fields in a hash.",
        "hash",
        &["key", "field", "[field ...]"],
    ),
    doc(
        "hdel",
        "Deletes one or more fields and their values from a hash. Deletes the hash if no fields remain.",
        "hash",
        &["key", "field", "[field ...]"],
    ),
    doc("hgetall", "Returns all fields and values in a hash.", "hash", &["key"]),
    doc("hkeys", "Returns all fields in a hash.", "hash", &["key"]),
    doc("hvals", "Returns all values in a hash.", "hash", &["key"]),
    doc("hlen", "Returns the number of fields in a hash.", "hash", &["key"]),
    doc(
        "hexists",
        "Determines whether a field exists in a hash.",
        "hash",
        &["key", "field"],
    ),
    doc(
        "hstrlen",
        "Returns the length of the value of a field.",
        "hash",
        &["key", "field"],
    ),
    doc(
        "hincrby",
        "Increments the integer value of a field in a hash by a number. Uses 0 as initial value if the field doesn't exist.",
        "hash",
        &["key", "field", "increment"],
    ),
    doc(
        "hincrbyfloat",
        "Increments the floating point value of a field by a number. Uses 0 as initial value if the field doesn't exist.",
        "hash",
        &["key", "field", "increment"],
    ),
    doc(
        "hscan",
        "Iterates over fields and values of a hash.",
        "hash",
        &["key", "cursor", "[MATCH pattern]", "[COUNT count]", "[NOVALUES]"],
    ),
    // Transactions.
    doc("multi", "Starts a transaction.", "transactions", &[]),
    doc("exec", "Executes all commands in a transaction.", "transactions", &[]),
    doc("discard", "Discards a transaction.", "transactions", &[]),
    doc(
        "watch",
        "Monitors changes to keys to determine the execution of a transaction.",
        "transactions",
        &["key", "[key ...]"],
    ),
    doc(
        "unwatch",
        "Forgets about watched keys of a transaction.",
        "transactions",
        &[],
    ),
    // Connection and server.
    doc(
        "ping",
        "Returns the server's liveliness response.",
        "connection",
        &["[message]"],
    ),
    doc("echo", "Returns the given string.", "connection", &["message"]),
    doc(
        "auth",
        "Authenticates the connection.",
        "connection",
        &["[username]", "password"],
    ),
    doc(
        "hello",
        "Handshakes with the Redis server.",
        "connection",
        &["[protover]"],
    ),
    doc("select", "Changes the selected database.", "connection", &["index"]),
    doc(
        "client",
        "A container for client connection commands.",
        "connection",
        &["subcommand"],
    ),
    doc("quit", "Closes the connection.", "connection", &[]),
    doc(
        "info",
        "Returns information and statistics about the server.",
        "server",
        &["[section ...]"],
    ),
];

/// How many commands it speaks: `COMMAND COUNT`.
pub fn count() -> Reply {
    Reply::Int(DOCS.len() as i64)
}

/// `COMMAND DOCS [name ...]`: each command asked for — every one, none asked — as a name and its doc's fields, pair by pair.
pub fn docs(names: &[Vec<u8>]) -> Reply {
    let wanted: Vec<String> = names
        .iter()
        .map(|n| String::from_utf8_lossy(n).to_ascii_lowercase())
        .collect();
    let bulk = |s: &str| Reply::Bulk(s.as_bytes().to_vec());
    let mut out = Vec::new();
    for d in DOCS
        .iter()
        .filter(|d| wanted.is_empty() || wanted.iter().any(|w| w == d.name))
    {
        let args = d
            .args
            .iter()
            .map(|a| {
                Reply::Array(vec![
                    bulk("name"),
                    bulk(a),
                    bulk("type"),
                    bulk("string"),
                    bulk("display_text"),
                    bulk(a),
                ])
            })
            .collect();
        out.push(bulk(d.name));
        out.push(Reply::Array(vec![
            bulk("summary"),
            bulk(d.summary),
            bulk("since"),
            bulk("1.0.0"),
            bulk("group"),
            bulk(d.group),
            bulk("arguments"),
            Reply::Array(args),
        ]));
    }
    Reply::Array(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docs_say_each_command_once_and_only_those_asked() {
        let Reply::Array(all) = docs(&[]) else {
            panic!("an array")
        };
        assert_eq!(all.len(), DOCS.len() * 2);
        let Reply::Array(one) = docs(&[b"HGETALL".to_vec()]) else {
            panic!("an array")
        };
        assert_eq!(one.len(), 2);
        assert!(matches!(&one[0], Reply::Bulk(b) if b == b"hgetall"));
        let names: std::collections::HashSet<_> = DOCS.iter().map(|d| d.name).collect();
        assert_eq!(names.len(), DOCS.len(), "no command twice");
    }
}
