#!/bin/bash
set -e

: "${DREMIO_SOFTWARE_USERNAME:?Need to set DREMIO_SOFTWARE_USERNAME}"
: "${DREMIO_SOFTWARE_PASSWORD:?Need to set DREMIO_SOFTWARE_PASSWORD}"

echo "Creating .env file for tests..."

mkdir -p tests
cat <<EOF > tests/.env
DREMIO_SOFTWARE_HOST=localhost
DREMIO_SOFTWARE_USERNAME=${DREMIO_SOFTWARE_USERNAME}
DREMIO_SOFTWARE_PASSWORD=${DREMIO_SOFTWARE_PASSWORD}
DREMIO_DATALAKE=dbt_test_source
DREMIO_DATABASE=dbt_test
DREMIO_EDITION=community
EOF

echo ".env file created successfully."
