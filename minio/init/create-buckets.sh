#!/bin/sh
set -eu

: "${MINIO_ENDPOINT?}"
: "${MINIO_ROOT_USER?}"
: "${MINIO_ROOT_PASSWORD?}"
: "${MINIO_BUCKET?}"

echo "[minio-init] waiting for MinIO API..."

for i in $(seq 1 60); do
  if mc alias set local "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

mc alias set local "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

mc mb -p "local/$MINIO_BUCKET" || true

echo "[minio-init] bucket created/exists: $MINIO_BUCKET"
