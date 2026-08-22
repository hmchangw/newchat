#!/usr/bin/env sh
# Reproduces the two new-room -> first-message races against real NATS.
#
#   ./tools/roomrace/run.sh            # single server + 3-node cluster
#   ./tools/roomrace/run.sh --keep     # leave the NATS containers running
#
# Requires a running Docker daemon. Nothing else in the stack is needed: the
# harness speaks the production subjects (pkg/subject) directly.
set -e

DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$DIR/../.." && pwd)
DELAYS=${DELAYS:-0,10,20,25,28,30,32,35,40}
ITER=${ITER:-40}
RENDER=${RENDER:-30}

cleanup() {
  [ "$1" = "--keep" ] && return 0
  docker compose -f "$DIR/docker-compose.yml" down -v >/dev/null 2>&1 || true
}
trap 'cleanup "$1"' EXIT

docker compose -f "$DIR/docker-compose.yml" up -d --wait

echo "### single server ###"
(cd "$ROOT" && go run ./tools/roomrace \
  -label "single-server" \
  -sub-url nats://127.0.0.1:4222 -pub-url nats://127.0.0.1:4222 \
  -iterations "$ITER" -render-ms "$RENDER" -settle-ms 120 -send-ms "$DELAYS")

echo
echo "### 3-node cluster (client B on node a, services on node b) ###"
(cd "$ROOT" && go run ./tools/roomrace \
  -label "3-node cluster (B on a, services on b)" \
  -sub-url nats://127.0.0.1:4223 -pub-url nats://127.0.0.1:4224 \
  -iterations "$ITER" -render-ms "$RENDER" -settle-ms 120 -send-ms "$DELAYS")
