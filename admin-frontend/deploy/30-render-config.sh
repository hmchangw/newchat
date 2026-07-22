#!/bin/sh
# Renders /config.js from env vars at container start. ADMIN_SERVICE_URL has no
# default: a misconfigured deploy must fail here, not send browsers to a dead
# admin-service.
set -eu

: "${ADMIN_SERVICE_URL:?ADMIN_SERVICE_URL is required (admin-service base URL)}"
export ADMIN_SERVICE_URL

envsubst '${ADMIN_SERVICE_URL}' \
  < /etc/config.js.template \
  > /usr/share/nginx/html/config.js

echo "rendered /config.js  ADMIN_SERVICE_URL=$ADMIN_SERVICE_URL"
