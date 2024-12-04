#!/bin/bash
set -e

echo "Creating Docker network 'ci-network'..."

docker network create ci-network || echo "Docker network 'ci-network' already exists."

echo "Docker network setup completed."