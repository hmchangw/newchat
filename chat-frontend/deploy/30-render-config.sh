#!/bin/sh
# Renders /config.js from env vars at container start. PORTAL_URL has no
# default: a misconfigured deploy must fail here, not send browsers to localhost.
set -eu

: "${PORTAL_URL:?PORTAL_URL is required (portal-service base URL)}"
: "${DEV_MODE:=false}"
: "${OIDC_ISSUER_URL:=}"
: "${OIDC_CLIENT_ID:=nats-chat}"
export PORTAL_URL DEV_MODE OIDC_ISSUER_URL OIDC_CLIENT_ID

envsubst '${PORTAL_URL} ${DEV_MODE} ${OIDC_ISSUER_URL} ${OIDC_CLIENT_ID}' \
  < /etc/config.js.template \
  > /usr/share/nginx/html/config.js

echo "rendered /config.js  PORTAL_URL=$PORTAL_URL  DEV_MODE=$DEV_MODE  OIDC_ISSUER_URL=$OIDC_ISSUER_URL  OIDC_CLIENT_ID=$OIDC_CLIENT_ID"
