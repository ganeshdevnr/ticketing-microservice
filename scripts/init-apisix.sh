#!/bin/bash
set -e

API_KEY="${APISIX_ADMIN_KEY:-edd1c9f034335f136f87ad84b625c8f1}"
BASE_URL="http://apisix:9180/apisix/admin"

echo "Waiting for APISIX Admin API..."
until curl -s -o /dev/null -w "%{http_code}" -H "X-API-KEY: $API_KEY" "$BASE_URL/protos" | grep -q "200"; do
  sleep 1
done

echo "Uploading compiled protos..."
PB_B64=$(base64 -w0 /combined.pb)
curl -s -X PUT "$BASE_URL/protos/combined" \
  -H "X-API-KEY: $API_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"content\":\"$PB_B64\"}"

echo "Syncing route configuration via ADC..."
export KEYCLOAK_CLIENT_ID="${KEYCLOAK_CLIENT_ID:-ticketing-app}"
export KEYCLOAK_CLIENT_SECRET="${KEYCLOAK_CLIENT_SECRET:-qihxyWCo3XoBK0jwN6WxqCfmbWjm4WXt}"
export KEYCLOAK_DISCOVERY_URL="${KEYCLOAK_DISCOVERY_URL:-http://keycloak:8080/realms/ticketing/.well-known/openid-configuration}"

envsubst '${KEYCLOAK_CLIENT_ID}${KEYCLOAK_CLIENT_SECRET}${KEYCLOAK_DISCOVERY_URL}' \
  < /declarative.yaml > /tmp/declarative.yaml

ADC_BACKEND=apisix \
ADC_SERVER=http://apisix:9180 \
ADC_TOKEN="$API_KEY" \
adc sync -f /tmp/declarative.yaml

echo "APISIX init complete."
