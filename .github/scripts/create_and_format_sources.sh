#!/bin/bash
set -e

: "${DREMIO_HEALTH_URL:?Need to set DREMIO_HEALTH_URL}"
: "${MINIO_ROOT_USER:?Need to set MINIO_ROOT_USER}"
: "${MINIO_ROOT_PASSWORD:?Need to set MINIO_ROOT_PASSWORD}"

# Get AUTH_TOKEN from environment or file
if [ "$GITHUB_ACTIONS" = "true" ]; then
  : "${AUTH_TOKEN:?Need to set AUTH_TOKEN}"
  HOST="minio"
else # Jenkins
  if [ ! -f /tmp/auth_token.txt ]; then
    echo "Auth token file not found. Please run extract_auth_token.sh first."
    exit 1
  fi
  AUTH_TOKEN=$(cat /tmp/auth_token.txt)
  HOST="localhost"
fi

manipulate_source() {
  local url=$1
  local data=$2
  local description=$3
  local raw=$4

  echo -e "\n$description in Dremio..."

  if [ "$raw" == "true" ]; then
    RESPONSE=$(curl --show-error -s -w "\n%{http_code}" -X PUT "$url" \
      -H "Content-Type: application/json" \
      -H "Authorization: _dremio$AUTH_TOKEN" \
      --data-raw "$data")
  else
    RESPONSE=$(curl --show-error -s -w "\n%{http_code}" -X PUT "$url" \
      -H "Content-Type: application/json" \
      -H "Authorization: _dremio$AUTH_TOKEN" \
      --data "$data")
  fi

  HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)
  RESPONSE_BODY=$(echo "$RESPONSE" | sed '$d')

  if [ "$HTTP_STATUS" -eq 200 ]; then
    echo -e "$description successful in Dremio."
  else
    echo -e "$description failed: $RESPONSE_BODY"
    exit 1
  fi
}

manipulate_source "$DREMIO_HEALTH_URL/apiv2/source/dbt_test_source" \
  "{
    \"name\":\"dbt_test_source\",
    \"config\":{
      \"credentialType\":\"ACCESS_KEY\",
      \"accessKey\":\"$MINIO_ROOT_USER\",
      \"accessSecret\":\"$MINIO_ROOT_PASSWORD\",
      \"secure\":false,
      \"externalBucketList\":[],
      \"enableAsync\":true,
      \"enableFileStatusCheck\":true,
      \"rootPath\":\"/\",
      \"defaultCtasFormat\":\"ICEBERG\",
      \"propertyList\":[
        {\"name\":\"fs.s3a.path.style.access\",\"value\":\"true\"},
        {\"name\":\"fs.s3a.endpoint\",\"value\":\"$HOST:9000\"},
        {\"name\":\"dremio.s3.compat\",\"value\":\"true\"}
      ],
      \"whitelistedBuckets\":[],
      \"isCachingEnabled\":false,
      \"maxCacheSpacePct\":100
    },
    \"type\":\"S3\",
    \"metadataPolicy\":{
      \"deleteUnavailableDatasets\":true,
      \"autoPromoteDatasets\":false,
      \"namesRefreshMillis\":3600000,
      \"datasetDefinitionRefreshAfterMillis\":3600000,
      \"datasetDefinitionExpireAfterMillis\":10800000,
      \"authTTLMillis\":86400000,
      \"updateMode\":\"PREFETCH_QUERIED\"
    }
  }" \
  "Creating S3 source" \
  "false"

manipulate_source "$DREMIO_HEALTH_URL/apiv2/source/Samples" \
  "{
    \"name\":\"Samples\",
    \"config\":{
      \"externalBucketList\":[\"samples.dremio.com\"],
      \"credentialType\":\"NONE\",
      \"secure\":false,
      \"propertyList\":[]
    },
    \"accelerationRefreshPeriod\":3600000,
    \"accelerationGracePeriod\":10800000,
    \"accelerationNeverRefresh\":true,
    \"accelerationNeverExpire\":true,
    \"accelerationActivePolicyType\":\"PERIOD\",
    \"accelerationRefreshSchedule\":\"0 0 8 * * *\",
    \"accelerationRefreshOnDataChanges\":false,
    \"type\":\"S3\"
  }" \
  "Creating Samples source" \
  "true"

manipulate_source "$DREMIO_HEALTH_URL/apiv2/source/Samples/file_format/samples.dremio.com/SF_incidents2016.json" \
  "{\"type\":\"JSON\"}" \
  "Formatting SF_incidents2016" \
  "true"

manipulate_source "$DREMIO_HEALTH_URL/apiv2/source/Samples/folder_format/samples.dremio.com/NYC-taxi-trips" \
  "{\"ignoreOtherFileFormats\":false,\"type\":\"Parquet\"}" \
  "Formatting NYC-taxi-trips" \
  "true"
