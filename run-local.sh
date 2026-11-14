#!/bin/sh
# Serves the normalized CNPJ database (bucket "zerolite", named before the rename; database cnpj_full) from the
# local MinIO on 127.0.0.1:5460, the cnpj-api's default. Starts MinIO when it is down.
set -e
cd "$(dirname "$0")"
. .data/env.sh
if ! curl -s -o /dev/null http://127.0.0.1:9100/minio/health/live; then
	MINIO_ROOT_USER=$AWS_ACCESS_KEY_ID MINIO_ROOT_PASSWORD=$AWS_SECRET_ACCESS_KEY \
		nohup minio server --address 127.0.0.1:9100 --console-address 127.0.0.1:9101 .data/minio > .data/minio.log 2>&1 &
	sleep 2
fi
export BUCKETDB_CACHE_MB=${BUCKETDB_CACHE_MB:-512}
export BUCKETDB_DISK_CACHE_DIR=${BUCKETDB_DISK_CACHE_DIR:-.data/cache}
exec ./target/release/bucketdb serve --port 5460 --dir .data/sidecar-cnpj "s3://$BUCKETDB_S3_BUCKET/cnpj_full"
