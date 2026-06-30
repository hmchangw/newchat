#!/bin/sh
# Renders /config.js from env vars at container start. PORTAL_URL has no
# default: a misconfigured deploy must fail here, not send browsers to localhost.
set -eu

: "${PORTAL_URL:?PORTAL_URL is required (portal-service base URL)}"
: "${DEV_MODE:=false}"
: "${OIDC_ISSUER_URL:=}"
: "${OIDC_CLIENT_ID:=nats-chat}"
: "${OTEL_ENABLED:=true}"
: "${OTEL_EXPORTER_OTLP_TRACES_URL:=http://localhost:4318/v1/traces}"
: "${OTEL_SERVICE_NAME:=chat-frontend}"
export PORTAL_URL DEV_MODE OIDC_ISSUER_URL OIDC_CLIENT_ID OTEL_ENABLED OTEL_EXPORTER_OTLP_TRACES_URL OTEL_SERVICE_NAME

envsubst '${PORTAL_URL} ${DEV_MODE} ${OIDC_ISSUER_URL} ${OIDC_CLIENT_ID} ${OTEL_ENABLED} ${OTEL_EXPORTER_OTLP_TRACES_URL} ${OTEL_SERVICE_NAME}' \
  < /etc/config.js.template \
  > /usr/share/nginx/html/config.js

echo "rendered /config.js  PORTAL_URL=$PORTAL_URL  DEV_MODE=$DEV_MODE  OIDC_ISSUER_URL=$OIDC_ISSUER_URL  OIDC_CLIENT_ID=$OIDC_CLIENT_ID  OTEL_ENABLED=$OTEL_ENABLED  OTEL_EXPORTER_OTLP_TRACES_URL=$OTEL_EXPORTER_OTLP_TRACES_URL  OTEL_SERVICE_NAME=$OTEL_SERVICE_NAME"
