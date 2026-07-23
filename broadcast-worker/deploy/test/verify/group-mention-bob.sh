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
  const doc = db.subscriptions.findOne(
    { roomId: 'group-1', 'u.account': 'bob' },
    { hasMention: 1 }
  );
  printjson(doc);
  if (!doc) {
    print('FAIL: subscription for bob in group-1 not found');
    quit(1);
  }
  if (doc.hasMention !== true) {
    print('FAIL: expected hasMention=true, got ' + doc.hasMention);
    quit(1);
  }
  print('OK: subscriptions(bob, group-1) hasMention=true');
"
