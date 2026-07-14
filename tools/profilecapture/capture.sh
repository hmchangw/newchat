#!/bin/sh
# One-shot pprof capture across the message-pipeline services. Runs inside a
# curl container joined to the chat-local network, so it resolves each service
# by its docker-compose service name. For every service it fetches a CPU, heap,
# and goroutine profile in parallel and writes them as
# <service>.<type>.pprof under a per-run folder in the bind-mounted /out.
#
# Driven by `make profile`. Tunables (all via env):
#   DURATION  CPU profile window in seconds (default 30)
#   LABEL     optional tag appended to the run folder name
#   SERVICES  space-separated manifest override (default: all nine)
#   HEALTH_PORT  health/pprof port (default 8081)
set -eu

DURATION="${DURATION:-30}"
LABEL="${LABEL:-}"
PORT="${HEALTH_PORT:-8081}"

# Static manifest: the services that mount pprof on their health port.
DEFAULT_SERVICES="broadcast-worker history-service inbox-worker message-gatekeeper message-worker notification-worker room-service room-worker search-sync-worker"
SERVICES="${SERVICES:-$DEFAULT_SERVICES}"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
outdir="/out/${ts}${LABEL:+-$LABEL}"
mkdir -p "$outdir"

echo "profilecapture: duration=${DURATION}s port=${PORT} -> ${outdir}"
echo "profilecapture: services: ${SERVICES}"

# The CPU profile request blocks for DURATION seconds; give curl headroom.
cpu_maxtime=$((DURATION + 20))

# fetch downloads one profile, removing a partial file on failure so a missing
# .pprof unambiguously means "not captured".
fetch() {
  svc="$1"; kind="$2"; path="$3"; maxtime="$4"
  url="http://${svc}:${PORT}${path}"
  out="${outdir}/${svc}.${kind}.pprof"
  if curl -fsS --max-time "$maxtime" -o "$out" "$url"; then
    echo "  ok   ${svc} ${kind}"
  else
    echo "  FAIL ${svc} ${kind} (${url})" >&2
    rm -f "$out"
  fi
}

pids=""
for svc in $SERVICES; do
  fetch "$svc" cpu "/debug/pprof/profile?seconds=${DURATION}" "$cpu_maxtime" &
  pids="$pids $!"
  fetch "$svc" heap "/debug/pprof/heap" 30 &
  pids="$pids $!"
  fetch "$svc" goroutine "/debug/pprof/goroutine" 30 &
  pids="$pids $!"
done

rc=0
for p in $pids; do
  wait "$p" || rc=1
done

echo "profilecapture: done -> ${outdir}"
exit "$rc"
