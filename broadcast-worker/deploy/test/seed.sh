#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILES=(-f "$DEPLOY_DIR/user/docker-compose.test.yml")
if [[ -n "${COMPOSE_OVERRIDE:-}" ]]; then
  COMPOSE_FILES+=(-f "$COMPOSE_OVERRIDE")
fi
COMPOSE=(docker compose "${COMPOSE_FILES[@]}")

wait_for_mongo() {
  local attempt
  for attempt in $(seq 1 10); do
    if "${COMPOSE[@]}" exec -T mongodb mongosh --quiet \
        --eval "db.adminCommand('ping').ok" \
        2>/dev/null | grep -q "^1$"; then
      return 0
    fi
    echo "waiting for mongodb (attempt $attempt/10)..."
    sleep 1
  done
  echo "mongodb did not become ready in 10s" >&2
  return 1
}

load_collection() {
  local collection="$1"
  local json_file="$2"

  "${COMPOSE[@]}" cp "$json_file" "mongodb:/tmp/$collection.json" >/dev/null

  "${COMPOSE[@]}" exec -T mongodb mongoimport \
    --uri="mongodb://localhost:27017/chat" \
    --collection="$collection" \
    --jsonArray \
    --drop \
    --file="/tmp/$collection.json" 2>&1 \
    | grep -E "imported|failed|error" || true
}

echo "waiting for mongodb..."
wait_for_mongo

echo "seeding collections..."
load_collection users "$SCRIPT_DIR/seed/users.json"
load_collection rooms "$SCRIPT_DIR/seed/rooms.json"
load_collection subscriptions "$SCRIPT_DIR/seed/subscriptions.json"

echo "seed complete"
