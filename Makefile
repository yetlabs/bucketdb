.PHONY: build test test-s3 fmt lint doc check gorm redis clean

build:
	cargo build --release

test:
	cargo test --release --workspace

fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace --all-targets -- -D warnings

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

# The storage tests against the local MinIO (run-local.sh starts it).
test-s3:
	. ./.data/env.sh && BUCKETDB_TEST_BUCKET=$$BUCKETDB_S3_BUCKET cargo test --release -p bucketdb-storage

# What CI runs, so a failure there can be reproduced with one command.
check: lint doc test
	cargo fmt --all --check

# GORM over pgx against a sidecar on the local MinIO (run-local.sh's), end to end.
gorm: build
	cd tests/gorm && go run .

# go-redis against a sidecar's Redis port (REDIS_URL, redis://127.0.0.1:6379/0 by
# default), end to end. Its keys are under a prefix of their own, deleted at the end.
redis: build
	cd tests/redis && go run .

clean:
	cargo clean
