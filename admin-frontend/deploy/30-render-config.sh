#!/bin/sh
# Renders /config.js from env vars at container start. ADMIN_SERVICE_URL has no
# default: a misconfigured deploy must fail here, not send browsers to a dead
# admin-service.
set -eu

: "${ADMIN_SERVICE_URL:?ADMIN_SERVICE_URL is required (admin-service base URL)}"
# Deploy-gated sections default off; only the literal string "true" enables.
: "${PERMISSIONS_ENABLED:=false}"
: "${UPDATES_ENABLED:=false}"
# Not a gate — the Rooms tab's member floor. See src/lib/runtimeConfig.js.
: "${ROOM_ONDUTY_MIN_MEMBERS:=5}"
export ADMIN_SERVICE_URL PERMISSIONS_ENABLED UPDATES_ENABLED ROOM_ONDUTY_MIN_MEMBERS

envsubst '${ADMIN_SERVICE_URL} ${PERMISSIONS_ENABLED} ${UPDATES_ENABLED} ${ROOM_ONDUTY_MIN_MEMBERS}' \
  < /etc/config.js.template \
  > /usr/share/nginx/html/config.js

echo "rendered /config.js  ADMIN_SERVICE_URL=$ADMIN_SERVICE_URL PERMISSIONS_ENABLED=$PERMISSIONS_ENABLED UPDATES_ENABLED=$UPDATES_ENABLED ROOM_ONDUTY_MIN_MEMBERS=$ROOM_ONDUTY_MIN_MEMBERS"
