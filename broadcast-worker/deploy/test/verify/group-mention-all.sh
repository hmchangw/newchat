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
  const doc = db.rooms.findOne({_id: 'group-1'}, {lastMentionAllAt: 1});
  printjson(doc);
  if (!doc) {
    print('FAIL: rooms.group-1 not found');
    quit(1);
  }
  const expectedPrefix = '2026-04-17T12:02:00';
  const lastMentionAllAt = doc.lastMentionAllAt ? doc.lastMentionAllAt.toISOString() : '';
  if (!lastMentionAllAt.startsWith(expectedPrefix)) {
    print('FAIL: expected lastMentionAllAt to start with ' + expectedPrefix + ', got ' + lastMentionAllAt);
    quit(1);
  }
  print('OK: rooms.group-1 lastMentionAllAt=' + lastMentionAllAt);
"
