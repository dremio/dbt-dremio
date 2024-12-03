#!/bin/bash
set -e

echo "Creating dbt projects..."

dbt init test_cloud_options <<EOF
1
3
localhost

dremio
dremio123









EOF

dbt init test_sw_up_options <<EOF
1
2
localhost

dremio
dremio123









EOF

dbt init test_sw_pat_options <<EOF
1
3
localhost

dremio
dremio123









EOF

          echo "projects created"