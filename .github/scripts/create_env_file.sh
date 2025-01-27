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
DBT_TEST_USER_1=dbt_test_user_1
DBT_TEST_USER_2=dbt_test_user_2
DBT_TEST_USER_3=dbt_test_user_3
DBT_TEST_ROLE_1=dbt_test_role_1
DBT_TEST_ROLE_2=dbt_test_role_2
DREMIO_EDITION=community
EOF

echo ".env file created successfully."