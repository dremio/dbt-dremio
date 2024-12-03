#!/bin/bash
set -e

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
EOF

echo ".env file created successfully."