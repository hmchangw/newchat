#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILES=(-f "$DEPLOY_DIR/user/docker-compose.test.yml")
if [[ -n "${COMPOSE_OVERRIDE:-}" ]]; then
  COMPOSE_FILES+=(-f "$COMPOSE_OVERRIDE")
fi
COMPOSE=(docker compose "${COMPOSE_FILES[@]}")

SCENARIO="${1:-}"
if [[ -z "$SCENARIO" ]]; then
  echo "usage: publish.sh <scenario>" >&2
  echo "available scenarios:" >&2
  ls "$SCRIPT_DIR/scenarios" | sed 's/\.json$//' | sed 's/^/  - /' >&2
  exit 1
fi

PAYLOAD_FILE="$SCRIPT_DIR/scenarios/$SCENARIO.json"
if [[ ! -f "$PAYLOAD_FILE" ]]; then
  echo "unknown scenario: $SCENARIO" >&2
  echo "available scenarios:" >&2
  ls "$SCRIPT_DIR/scenarios" | sed 's/\.json$//' | sed 's/^/  - /' >&2
  exit 1
fi

PAYLOAD="$(cat "$PAYLOAD_FILE")"
SUBJECT="chat.msg.canonical.site1.created"

echo "publishing $SCENARIO to $SUBJECT on nats_site1..."
"${COMPOSE[@]}" exec -T tools \
  nats --server nats://nats_site1:4222 \
  pub "$SUBJECT" "$PAYLOAD"

echo "published"
