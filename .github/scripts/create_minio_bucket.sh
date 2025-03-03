#!/bin/bash
set -e

: "${RETRY_COUNT:?Need to set RETRY_COUNT}"
: "${SLEEP_INTERVAL:?Need to set SLEEP_INTERVAL}"
: "${MINIO_HOST_URL:?Need to set MINIO_HOST_URL}"
: "${MINIO_HEALTH_URL:?Need to set MINIO_HEALTH_URL}"
: "${MINIO_ROOT_USER:?Need to set MINIO_ROOT_USER}"
: "${MINIO_ROOT_PASSWORD:?Need to set MINIO_ROOT_PASSWORD}"

echo "Waiting for MinIO to become ready..."

for i in $(seq 1 $RETRY_COUNT); do
  if curl -s $MINIO_HEALTH_URL; then
    echo "MinIO is up."
    break
  fi
  echo "Attempt $i/$RETRY_COUNT: MinIO is not ready yet. Retrying in $SLEEP_INTERVAL seconds..."
  sleep $SLEEP_INTERVAL
done

if ! curl -s $MINIO_HEALTH_URL; then
  echo "MinIO did not become ready in time."
  exit 1
fi

# Set alias to MinIO using localhost
mc alias set myminio "$MINIO_HOST_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

echo "Creating bucket dbtdremios3"
mc mb myminio/dbtdremios3

echo "Setting bucket policy to public"
mc policy set public myminio/dbtdremios3

echo "Listing all buckets to verify"
mc ls myminio
