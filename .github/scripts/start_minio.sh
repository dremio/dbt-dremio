#!/bin/bash
set -e

echo "Starting MinIO service..."

docker run -d \
  --network ci-network \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e "MINIO_ROOT_USER=${MINIO_ROOT_USER}" \
  -e "MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}" \
  minio/minio server /data --console-address ":9001"

echo "MinIO service started."