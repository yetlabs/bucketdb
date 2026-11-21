# AGENTS.md

Guidance for coding agents working in this repository. `CLAUDE.md` is a symlink
to this file, so Claude Code reads the same conventions.

## Language

Commit messages, code comments, docs and identifiers are in English.

## Commits

Write commit messages as [Conventional Commits](https://www.conventionalcommits.org):
`<type>: <summary>`, imperative mood, lowercase summary, no trailing period.

Keep the summary short, at most 72 characters, so `git log --oneline` and other
one-line views of the history stay readable. One change per commit.

Types used here: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`,
`build`, `ci`, `chore`. Prefer `style` for formatting-only changes (rustfmt) and
`chore` for housekeeping that touches no behavior.

The body is optional: when present, explain why the change is needed, not what the
diff already shows. No AI attribution in commits or pull requests.

## Verifying a change

`make check` runs exactly what CI runs: clippy (pedantic, warnings denied),
rustdoc with warnings denied, the tests, and rustfmt. Run it before committing.

`make gorm` drives the binary end to end with GORM over pgx; it needs the local
MinIO and a sidecar from `run-local.sh` (see README.md). `make redis` does the
same for the Redis side with go-redis, against `REDIS_URL` (a sidecar's port).

## Principles

- BucketDB must stay as small as possible: no new dependency without a strong
  reason, no AWS SDK (the S3 client is hand-written SigV4), the release binary
  is built for size.
- Every `unsafe` block carries a `// SAFETY:` comment saying why it holds.
- No `unwrap`, `expect` or `panic!` on server paths; return errors.
- The bucket is the truth: a commit is in S3 before the client hears of it.
