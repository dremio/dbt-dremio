#!/bin/bash
set -e

: "${RETRY_COUNT:?Need to set RETRY_COUNT}"
: "${DREMIO_HEALTH_URL:?Need to set DREMIO_HEALTH_URL}"
: "${SLEEP_INTERVAL:?Need to set SLEEP_INTERVAL}"
: "${DREMIO_SOFTWARE_USERNAME:?Need to set DREMIO_SOFTWARE_USERNAME}"
: "${DREMIO_SOFTWARE_PASSWORD:?Need to set DREMIO_SOFTWARE_PASSWORD}"
: "${MINIO_ROOT_USER:?Need to set MINIO_ROOT_USER}"
: "${MINIO_ROOT_PASSWORD:?Need to set MINIO_ROOT_PASSWORD}"

echo "Creating Dremio S3 Source..."

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
AUTH_RESPONSE=$(curl -s -X POST "http://localhost:9047/apiv2/login" \
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
echo "AUTH_TOKEN=${AUTH_TOKEN}" >> $GITHUB_ENV

# Create the S3 source in Dremio
echo "Creating the S3 source in Dremio..."
curl -s -X PUT "http://localhost:9047/apiv2/source/dbt_test_source" \
  -H "Content-Type: application/json" \
  -H "Authorization: _dremio$AUTH_TOKEN" \
  --data "{\"name\":\"dbt_test_source\",\"config\":{\"credentialType\":\"ACCESS_KEY\",\"accessKey\":\"$MINIO_ROOT_USER\",\"accessSecret\":\"$MINIO_ROOT_PASSWORD\",\"secure\":false,\"externalBucketList\":[],\"enableAsync\":true,\"enableFileStatusCheck\":true,\"rootPath\":\"/\",\"defaultCtasFormat\":\"ICEBERG\",\"propertyList\":[{\"name\":\"fs.s3a.path.style.access\",\"value\":\"true\"},{\"name\":\"fs.s3a.endpoint\",\"value\":\"minio:9000\"},{\"name\":\"dremio.s3.compat\",\"value\":\"true\"}],\"whitelistedBuckets\":[],\"isCachingEnabled\":false,\"maxCacheSpacePct\":100},\"type\":\"S3\",\"metadataPolicy\":{\"deleteUnavailableDatasets\":true,\"autoPromoteDatasets\":false,\"namesRefreshMillis\":3600000,\"datasetDefinitionRefreshAfterMillis\":3600000,\"datasetDefinitionExpireAfterMillis\":10800000,\"authTTLMillis\":86400000,\"updateMode\":\"PREFETCH_QUERIED\"}}"

echo "S3 Source created in Dremio."

echo "Creating the Samples source in Dremio..."
curl -s -X PUT  "http://localhost:9047/apiv2/source/Samples" \
  -H "Content-Type: application/json" \
  -H "Authorization: _dremio$AUTH_TOKEN" \
  --data-raw "{\"name\":\"Samples\",\"config\":{\"externalBucketList\":[\"samples.dremio.com\"],\"credentialType\":\"NONE\",\"secure\":false,\"propertyList\":[]},\"name\":\"Samples\",\"accelerationRefreshPeriod\":3600000,\"accelerationGracePeriod\":10800000,\"accelerationNeverRefresh\":true,\"accelerationNeverExpire\":true,\"accelerationActivePolicyType\":\"PERIOD\",\"accelerationRefreshSchedule\":\"0 0 8 * * *\",\"accelerationRefreshOnDataChanges\":false,\"type\":\"S3\"}"

echo "Samples source created in Dremio."

echo "Formatting SF_incidents2016..."
curl -s -X PUT "http://localhost:9047/apiv2/source/Samples/file_format/samples.dremio.com/SF_incidents2016.json" \
  -H "Content-Type: application/json" \
  -H "Authorization: _dremio$AUTH_TOKEN" \
  --data-raw "{\"type\":\"JSON\"}"

echo "SF_incidents2016 formatted in Dremio."

echo "Formatting NYC-taxi-trips..."
curl -s -X PUT "http://localhost:9047/apiv2/source/Samples/folder_format/samples.dremio.com/NYC-taxi-trips" \
  -H "Content-Type: application/json" \
  -H "Authorization: _dremio$AUTH_TOKEN" \
  --data-raw "{\"ignoreOtherFileFormats\":false,\"type\":\"Parquet\"}"

echo "NYC-taxi-trips formatted in Dremio."