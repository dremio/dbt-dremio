#!/bin/bash
set -e

echo "Starting Dremio service..."

docker run -d \
  --network ci-network \
  --name dremio \
  -p 31010:31010 \
  -p 9047:9047 \
  -e "DREMIO_JAVA_SERVER_EXTRA_OPTS=-Ddebug.addDefaultUser=true" \
  dremio/dremio-oss

echo "Dremio service started."