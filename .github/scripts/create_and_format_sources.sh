#!/bin/bash
set -e

: "${RETRY_COUNT:?Need to set RETRY_COUNT}"
: "${DREMIO_HEALTH_URL:?Need to set DREMIO_HEALTH_URL}"
: "${SLEEP_INTERVAL:?Need to set SLEEP_INTERVAL}"
: "${DREMIO_SOFTWARE_USERNAME:?Need to set DREMIO_SOFTWARE_USERNAME}"
: "${DREMIO_SOFTWARE_PASSWORD:?Need to set DREMIO_SOFTWARE_PASSWORD}"
: "${MINIO_ROOT_USER:?Need to set MINIO_ROOT_USER}"
: "${MINIO_ROOT_PASSWORD:?Need to set MINIO_ROOT_PASSWORD}"

for i in $(seq 1 $RETRY_COUNT); do
  if curl -s $DREMIO_HEALTH_URL; then
    echo "Dremio is up."
    break
  fi
  echo "Waiting for Dremio to become ready... ($i/$RETRY_COUNT)"
  sleep $SLEEP_INTERVAL
done

if ! curl -s $DREMIO_HEALTH_URL; then
  echo "Dremio did not become ready in time."
  exit 1
fi

# Obtain Dremio auth token
echo "Logging into Dremio to obtain auth token..."
AUTH_RESPONSE=$(curl -s -X POST "$DREMIO_HEALTH_URL/apiv2/login" \
  -H "Content-Type: application/json" \
  --data "{\"userName\":\"${DREMIO_SOFTWARE_USERNAME}\", \"password\":\"${DREMIO_SOFTWARE_PASSWORD}\"}")

AUTH_TOKEN=$(echo "$AUTH_RESPONSE" | jq -r .token)

# Check if AUTH_TOKEN is not empty
if [ -z "$AUTH_TOKEN" ] || [ "$AUTH_TOKEN" == "null" ]; then
  echo "Failed to obtain Dremio auth token."
  exit 1
fi

echo "Obtained Dremio auth token."
echo "::add-mask::$AUTH_TOKEN"
if [ "$GITHUB_ACTIONS" = "true" ]; then
  echo "Running in GitHub Actions"
  echo "AUTH_TOKEN=${AUTH_TOKEN}" >> $GITHUB_ENV
  HOST="minio"
else # Jenkins
  echo $AUTH_TOKEN > /tmp/auth_token.txt
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
