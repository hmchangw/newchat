#!/usr/bin/env bash
# Hot-reload runner for a single service. Substitutes the service name into
# the air config template, sources optional .env.dev, and execs air.
# Assumes `make deps-up` has already started shared deps.

set -euo pipefail

SERVICE="${1:?usage: dev.sh <service-name>}"
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

if [ ! -d "$SERVICE" ]; then
  echo "dev: service '$SERVICE' not found at repo root" >&2
  exit 1
fi

if ! command -v air >/dev/null 2>&1; then
  echo "dev: air not installed. Run 'make tools' first." >&2
  exit 1
fi

if ! docker container inspect -f '{{.State.Running}}' chat-local-nats 2>/dev/null | grep -q true; then
  echo "dev: shared deps not running. Run 'make deps-up' first." >&2
  exit 1
fi

CFG="$REPO_ROOT/.air.${SERVICE}.toml"
sed "s|%SERVICE%|${SERVICE}|g" tools/dev/air-template.toml > "$CFG"

if [ -f "$SERVICE/.env.dev" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$SERVICE/.env.dev"
  set +a
fi

# obs.Init requires OTEL_SERVICE_NAME (fail-fast) — default it from the service
# name so `make dev` works without an .env.dev. Default the OTLP endpoint for
# host dev, and disable the SDK's Prometheus listener so running several
# `make dev` services on the host don't collide on :2112. Anything already set
# (environment or .env.dev) wins.
export OTEL_SERVICE_NAME="${OTEL_SERVICE_NAME:-$SERVICE}"
export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4318}"
export O11Y_METRICS_ENABLED="${O11Y_METRICS_ENABLED:-false}"

exec air -c "$CFG"
