#!/bin/bash
set -e

: "${RETRY_COUNT:?Need to set RETRY_COUNT}"
: "${DREMIO_HEALTH_URL:?Need to set DREMIO_HEALTH_URL}"
: "${SLEEP_INTERVAL:?Need to set SLEEP_INTERVAL}"
: "${DREMIO_SOFTWARE_USERNAME:?Need to set DREMIO_SOFTWARE_USERNAME}"
: "${DREMIO_SOFTWARE_PASSWORD:?Need to set DREMIO_SOFTWARE_PASSWORD}"

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
  echo "AUTH_TOKEN=${AUTH_TOKEN}" >> $GITHUB_ENV
else # Jenkins
  echo $AUTH_TOKEN > /tmp/auth_token.txt
fi
