#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILES=(-f "$DEPLOY_DIR/user/docker-compose.test.yml")
if [[ -n "${COMPOSE_OVERRIDE:-}" ]]; then
  COMPOSE_FILES+=(-f "$COMPOSE_OVERRIDE")
fi
COMPOSE=(docker compose "${COMPOSE_FILES[@]}")

exec "${COMPOSE[@]}" exec -T mongodb mongosh mongodb://localhost:27017/chat --quiet --eval "
  const doc = db.rooms.findOne({_id: 'group-1'}, {lastMsgId: 1, lastMsgAt: 1});
  printjson(doc);
  if (!doc) {
    print('FAIL: rooms.group-1 not found');
    quit(1);
  }
  const expectedId = 'm-group-plain';
  const expectedPrefix = '2026-04-17T12:00:00';
  const lastMsgAt = doc.lastMsgAt ? doc.lastMsgAt.toISOString() : '';
  let ok = true;
  if (doc.lastMsgId !== expectedId) {
    print('FAIL: expected lastMsgId=' + expectedId + ', got ' + doc.lastMsgId);
    ok = false;
  }
  if (!lastMsgAt.startsWith(expectedPrefix)) {
    print('FAIL: expected lastMsgAt to start with ' + expectedPrefix + ', got ' + lastMsgAt);
    ok = false;
  }
  if (ok) {
    print('OK: rooms.group-1 lastMsgId=' + doc.lastMsgId + ' lastMsgAt=' + lastMsgAt);
  } else {
    quit(1);
  }
"
