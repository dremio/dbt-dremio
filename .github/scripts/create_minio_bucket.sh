#!/bin/bash
set -e

echo "Creating MinIO bucket..."

# Wait for MinIO to be healthy
for i in $(seq 1 $RETRY_COUNT); do
  if curl -s $MINIO_HEALTH_URL; then
    echo "MinIO is up."
    break
  fi
  echo "Waiting for MinIO to become ready... ($i/$RETRY_COUNT)"
  sleep $SLEEP_INTERVAL
done

if ! curl -s $MINIO_HEALTH_URL; then
  echo "MinIO did not become ready in time."
  exit 1
fi

# Set alias to MinIO
mc alias set myminio http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

echo "Creating bucket dbtdremios3..."
mc mb myminio/dbtdremios3

echo "Setting bucket policy to public..."
mc policy set public myminio/dbtdremios3

echo "Listing all buckets to verify..."
mc ls myminio

echo "MinIO bucket setup completed."