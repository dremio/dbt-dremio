#!/bin/bash
set -e

echo "Creating dbt test users in Dremio..."

if [ -z "$AUTH_TOKEN" ]; then
  echo "AUTH_TOKEN is not set. Exiting."
  exit 1
fi

# Function to create a user
create_user() {
  local firstName=$1
  local lastName=$2
  local name=$3
  local email=$4
  local password=$5

  curl -s 'http://localhost:9047/api/v3/user' \
    -H "Authorization: _dremio$AUTH_TOKEN" \
    -H 'Content-Type: application/json' \
    --data-raw "{\"firstName\":\"$firstName\",\"lastName\":\"$lastName\",\"name\":\"$name\",\"email\":\"$email\",\"password\":\"$password\"}"
  
  echo "User $name created."
}

# Create each user
create_user "dbt" "user1" "$DBT_TEST_USER_1" "dbt_test_user_1@dremio.com" "dremio123"
create_user "dbt" "user2" "$DBT_TEST_USER_2" "dbt_test_user_2@dremio.com" "dremio123"
create_user "dbt" "user3" "$DBT_TEST_USER_3" "dbt_test_user_3@dremio.com" "dremio123"

echo "All dbt test users created successfully."
