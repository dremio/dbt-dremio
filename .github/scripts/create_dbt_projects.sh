#!/bin/bash
set -e

echo "Creating dbt projects..."

init_dbt_project() {
  local project_name=$1
  local profile_selection=$2
  local target_selection=$3
  local host=$4
  local username=$5
  local password=$6

  dbt init "$project_name" <<EOF
$profile_selection
$target_selection
$host

$username
$password
\n\n\n\n\n\n
EOF

  echo "dbt project '$project_name' created."
}

init_dbt_project "test_cloud_options" 1 3 "localhost" "dremio" "dremio123"
init_dbt_project "test_sw_up_options" 1 2 "localhost" "dremio" "dremio123"
init_dbt_project "test_sw_pat_options" 1 3 "localhost" "dremio" "dremio123"

echo "All dbt projects created successfully."

