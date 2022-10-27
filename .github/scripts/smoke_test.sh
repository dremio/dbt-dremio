# Copyright (C) 2022 Dremio Corporation

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

<< comment
Intent:

Run all three tests (softwareUP, softwarePAT, and cloud) before
submitting a pull request. This is a smoke test to ensure the 
adapter works as expected after your changes. 

How to Run

Software User/Password Test:
./smoke_test.sh softwareUP USER PASSWORD HOST PORT [SSL Boolean]

Software PAT Test:
./smoke_test.sh softwarePAT USER PAT HOST PORT [SSL Boolean]

Cloud Test:
./smoke_test.sh cloud USER PAT HOST CLOUD_PROJECT true

comment

#! /bin/bash

# create test model
create_model() {
  touch ./$project/models/select_model.sql
  { cat > ./$project/models/select_model.sql << EOF
  SELECT *
  FROM Samples."samples.dremio.com"."zips.json" LIMIT 5
EOF
  }
}

# software user/pw test
test_softwareUP() {
  cd ../../.. && \
  if [ -d "./$project" ]; then
      rm -r $project
  fi

  dbt init -s $project && \
  { cat > $profiles_path << EOF
  $project:
    outputs:
      dev:
        password: $test_password
        port: 9047
        software_host: $test_host
        threads: 1
        type: dremio
        use_ssl: $test_ssl
        user: $test_user
    target: dev
EOF
  } && \

  create_model && \
  cd $project/ && \
  dbt debug && \
  dbt run
}

# software user/pat test
test_softwarePAT() {
  cd ../../.. && \
  if [ -d "./$project" ]; then
      rm -r $project
  fi

  dbt init -s $project && \
  { cat > $profiles_path << EOF
  $project:
    outputs:
      dev:
        pat: $test_token
        port: 9047
        software_host: $test_host
        threads: 1
        type: dremio
        use_ssl: $test_ssl
        user: $test_user
    target: dev
EOF
  } && \

  create_model && \
  cd $project/ && \
  dbt debug && \
  dbt run
}

#Cloud test
test_cloud() {
  cd ../../.. && \
  if [ -d "./$project" ]; then
      rm -r $project
  fi

  dbt init -s $project && \
  { cat > $profiles_path << EOF
  $project:
    outputs:
      dev:
        cloud_host: $test_host
        cloud_project_id: $test_cloud_project
        pat: $test_token
        threads: 1
        type: dremio
        use_ssl: true
        user: $test_user
    target: dev
EOF
  } && \

  create_model && \
  cd $project/ && \
  dbt debug && \
  dbt run 
}

# Main
test_type=$1
test_ssl="${6:-false}"
profiles_path=./.dbt/profiles.yml 

if [ $test_type == softwareUP ]; then 
    test_user=$2
    test_password=$3
    test_host=$4
    test_port=$5
  
    project=software_proj
    test_softwareUP

elif [ $test_type == softwarePAT ]; then
    test_user=$2
    test_token=$3
    test_host=$4
    test_port=$5

    project=software_proj
    test_softwarePAT

elif [ $test_type == cloud ]; then
    test_user=$2
    test_token=$3
    test_host=$4
    test_cloud_project=$5

    project=cloud_proj
    test_cloud
fi

