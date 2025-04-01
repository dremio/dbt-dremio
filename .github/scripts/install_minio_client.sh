#!/bin/bash
set -e

echo "Installing MinIO Client (mc)..."

{
    curl -O https://dl.min.io/client/mc/release/linux-amd64/mc &&
    chmod +x mc &&
    mv mc /usr/local/bin/
} && echo "MinIO Client installed successfully." || {
    echo "Failed to install MinIO Client."
    exit 1
}
