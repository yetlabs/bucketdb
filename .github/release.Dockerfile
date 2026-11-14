# ghcr.io/yetlabs/bucketdb: the static Linux binary release.yml built, and nothing
# else. No libc, no shell, no CA bundle: musl is linked in and so are the TLS roots.
# Built from dist/, which holds bucketdb-linux-amd64 and bucketdb-linux-arm64.
FROM scratch
ARG TARGETARCH
COPY --chmod=755 bucketdb-linux-${TARGETARCH} /bucketdb
# Placeholders and SQLite's temp files (it falls back to the working directory).
WORKDIR /data
EXPOSE 5432
ENTRYPOINT ["/bucketdb"]
