#! /bin/bash

# Software test
test_software() {
cd ../../.. && \
if [ -d "./$project" ]; then
    rm -r $project
fi
dbt init -s $project && \
{ cat > ./.dbt/profiles.yml << EOF
$project:
  outputs:
    dev:
      password: $test_password
      port: 9047
      software_host: $test_host
      threads: 1
      type: dremio
      use_ssl: false
      user: $test_user
  target: dev
EOF
} && \

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
{ cat > ./.dbt/profiles.yml << EOF
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
echo 'vars:' >> ./$project/dbt_project.yml && \
echo '  dremio:reflections_enabled: false' >> ./$project/dbt_project.yml && \

cd $project/ && \
dbt debug && \
dbt run 
}

# Main
test_type=$1

if [ $test_type == software ]; then 
    test_user=$2
    test_password=$3
    test_host=$4
    test_port=$5
    project=software_proj
    test_software
elif [ $test_type == cloud ]; then
    test_user=$2
    test_token=$3
    test_host=$4
    test_cloud_project=$5
    project=cloud_proj
    test_cloud
fi

