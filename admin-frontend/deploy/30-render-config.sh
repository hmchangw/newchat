#!/bin/sh
# Renders /config.js from env vars at container start. Neither var has a
# default: a misconfigured deploy must fail here, not send browsers to
# localhost or a dead admin-service.
set -eu

: "${PORTAL_URL:?PORTAL_URL is required (portal-service base URL)}"
: "${ADMIN_SERVICE_URL:?ADMIN_SERVICE_URL is required (admin-service base URL)}"
export PORTAL_URL ADMIN_SERVICE_URL

envsubst '${PORTAL_URL} ${ADMIN_SERVICE_URL}' \
  < /etc/config.js.template \
  > /usr/share/nginx/html/config.js

echo "rendered /config.js  PORTAL_URL=$PORTAL_URL  ADMIN_SERVICE_URL=$ADMIN_SERVICE_URL"
