#!/usr/bin/env bash
# Brings up a minimal real stack for the channel first-message E2E:
#   deps      : NATS (operator JWT), MongoDB, Cassandra, Valkey
#   services  : room-service, room-worker, message-gatekeeper,
#               broadcast-worker, message-worker, history-service
#
# Services run as host processes against the containerised deps - far quicker
# than building six images, and the binaries are the real ones.
#
#   ./tools/roomrace/e2e/stack.sh up      # deps + seed + services
#   ./tools/roomrace/e2e/stack.sh down
#   ./tools/roomrace/e2e/stack.sh logs <service>
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
RUN=${RUN_DIR:-/tmp/roomrace-e2e}
BIN=$RUN/bin
LOGS=$RUN/logs
SERVICES="room-service room-worker message-gatekeeper broadcast-worker message-worker history-service"

export NATS_URL=nats://localhost:4222
export NATS_CREDS_FILE=$ROOT/docker-local/backend.creds
export SITE_ID=site-local
export ALL_SITE_IDS=site-local
export MONGO_URI="mongodb://localhost:27017/?directConnection=true"
export MONGO_DB=chat
# primary, not secondaryPreferred: this test measures read-after-write timing and
# a secondary read would add staleness that is not the effect under study.
export MONGO_READ_PREFERENCE=primary
export CASSANDRA_HOSTS=localhost
export CASSANDRA_KEYSPACE=chat
export VALKEY_ADDRS=localhost:6379
export BOOTSTRAP_STREAMS=true
export ATREST_ENABLED=false      # skips Vault
export ENCRYPTION_ENABLED=false  # plaintext room events, so the E2E client can read them
export ROOM_SUBJECT_MODE=dual    # compose default: same-site rooms publish local AND global
export O11Y_ENABLED=false

port_for() {
  case $1 in
    room-service) echo 18081 ;;
    room-worker) echo 18082 ;;
    message-gatekeeper) echo 18083 ;;
    broadcast-worker) echo 18084 ;;
    message-worker) echo 18085 ;;
    history-service) echo 18086 ;;
  esac
}

up() {
  mkdir -p "$BIN" "$LOGS"

  grep -qE "^127\.0\.0\.1 .*valkey" /etc/hosts || \
    echo "127.0.0.1 valkey mongodb cassandra nats" >> /etc/hosts

  [ -f "$ROOT/docker-local/backend.creds" ] || "$ROOT/docker-local/setup.sh"

  docker compose -f "$ROOT/docker-local/compose.deps.yaml" up -d --wait nats mongodb cassandra valkey
  # A warm Cassandra volume makes the init report "already exists"; that is fine.
  docker compose -f "$ROOT/docker-local/compose.deps.yaml" --profile init run --rm cassandra-init || true
  (cd "$ROOT" && go run ./tools/seed-sample-data)

  for s in $SERVICES; do
    pkg=./$s
    [ "$s" = history-service ] && pkg=./history-service/cmd
    (cd "$ROOT" && go build -o "$BIN/$s" "$pkg")
  done

  for s in $SERVICES; do
    p=$(port_for "$s")
    env HEALTH_ADDR=":$p" METRICS_ADDR=":$((p + 100))" \
        $( [ "$s" = broadcast-worker ] && echo MODE=user ) \
        "$BIN/$s" > "$LOGS/$s.log" 2>&1 &
    echo $! > "$RUN/$s.pid"
  done

  echo "waiting for services to report healthy..."
  for s in $SERVICES; do
    p=$(port_for "$s")
    for _ in $(seq 1 60); do
      curl -sf "http://127.0.0.1:$p/healthz" >/dev/null 2>&1 && break
      sleep 1
    done
    printf '  %-20s %s\n' "$s" "$(curl -sf "http://127.0.0.1:$p/healthz" >/dev/null 2>&1 && echo healthy || echo UNHEALTHY)"
  done
}

down() {
  for s in $SERVICES; do
    [ -f "$RUN/$s.pid" ] && kill "$(cat "$RUN/$s.pid")" 2>/dev/null || true
    rm -f "$RUN/$s.pid"
  done
  docker compose -f "$ROOT/docker-local/compose.deps.yaml" down -v
}

case "${1:-up}" in
  up) up ;;
  down) down ;;
  logs) tail -n 100 -f "$LOGS/${2:?service name}.log" ;;
  *) echo "usage: $0 {up|down|logs <service>}" >&2; exit 1 ;;
esac
