# clientsim

Real-WSS connection soak tool. Per simulated user it walks the full
production client edge path — NKey generation → `POST /api/v1/auth` (via a
dev-mode side issuer) → NATS **WebSocket** connect with the minted user JWT
→ the frontend's subscription walk (user event lane + paginated
`subscription.list` + per-room channel subscriptions, kept live via
`subscription.update`) — then holds the connection, counting deliveries and
observing client-edge latency.

Design: `docs/superpowers/specs/2026-08-29-clientsim-design.md`.
Companion to `tools/loadgen` (which generates the traffic); the only data
contract between the two is the **pool artifact** (`pkg/poolartifact`).

## Quick start (docker-local)

The two tools live in separate compose projects, so the pool artifact is
handed over through the host:

```bash
# 1. Base stack + fixtures, emitting the pool artifact. Keep the one-off
#    container so the artifact can be copied from its writable /tmp (no --rm):
make -C tools/loadgen/deploy stack-up
docker compose -f tools/loadgen/deploy/docker-compose.yml run \
  --build --entrypoint /loadgen --name clientsim-seed loadgen \
  seed --preset=medium --pool-out=/tmp/pool.json

# 2. Lift the artifact onto the host, then drop the seeder container:
docker cp clientsim-seed:/tmp/pool.json ./pool.json
docker rm clientsim-seed

# 3. clientsim overlay (side issuer + clientsim container):
docker compose -f tools/clientsim/deploy/docker-compose.yml up -d --build --wait

# 4. Push the artifact into the clientsim volume and run:
docker compose -f tools/clientsim/deploy/docker-compose.yml cp \
  ./pool.json clientsim:/var/lib/clientsim/pool.json
docker compose -f tools/clientsim/deploy/docker-compose.yml exec clientsim /clientsim
```

`CLIENTSIM_SITE_ID` must match the artifact's `siteId` or startup fails.

### Metrics endpoint

clientsim serves its registry on `CLIENTSIM_METRICS_ADDR` (a full listen
address, default `:2112`; the overlay maps it to host `:2113`). The handler
is mounted at the server root, so any path works — `/metrics` included, for
a k8s ServiceMonitor.

The loadgen dashboards profile scrapes it automatically as the static target
`clientsim:2112`; both compose projects join the `chat-local` network. Real
clusters configure the equivalent target through ops-owned manifests, not
the local YAML in this repo.

## Configuration

| Env | Default | Meaning |
|---|---|---|
| `CLIENTSIM_NATS_WS_URL` | required | `ws(s)://` endpoint clients dial |
| `CLIENTSIM_AUTH_URL` | required | side issuer base URL |
| `CLIENTSIM_POOL_FILE` | required | pool artifact path (`pkg/poolartifact`) |
| `CLIENTSIM_SITE_ID` | required | site for `subscription.list` + subjects; must match the artifact |
| `CLIENTSIM_TARGET_CONNS` | pool size | `T = min(target, pool)`; floor-partitioned across shards |
| `CLIENTSIM_SHARD_INDEX` / `_SHARD_COUNT` | `0` / `1` | replica slice |
| `CLIENTSIM_RAMP_RATE` | `50` | connects/sec **per replica** during ramp |
| `CLIENTSIM_CHURN_RATE` | `0` | reconnect cycles/sec across the shard |
| — | — | clients that exit early are restarted at the ramp rate, up to 5 attempts each |
| `CLIENTSIM_JWT_MODE` | `proactive` | `proactive` (frontend-parity 80% ±5% refresh) or `expiry` (resilience A/B) |
| `CLIENTSIM_SUB_PENDING_MSGS` / `_BYTES` | `512` / `128KiB` | pending limits for the two per-user lanes; msgs also sizes the shared room-delivery channel |
| `CLIENTSIM_RECONNECT_BUF_BYTES` | `64KiB` | nats.go reconnect buffer per conn |
| `CLIENTSIM_PING_INTERVAL` | `2m` | client ping interval (idle-conn keepalive) |
| `CLIENTSIM_METRICS_ADDR` | `:2112` | Prometheus listen address (served at the root) |
| `CLIENTSIM_MIN_READY_RATIO` | `0.95` | fleet-readiness exit gate; `0` disables it |
| `CLIENTSIM_FAIL_ON_DEGRADED` | `false` | also exit non-zero when loss counters fired |

Room subscriptions use `ChanSubscribe` into one shared channel drained by a
single pump goroutine, so room count multiplies neither goroutines nor
buffers. Steady state is ~7 goroutines per client regardless of room count.

**`_BYTES` does not bound the room lane.** `SetPendingLimits` only applies to
the two callback lanes; nats.go rejects it for a channel subscription and
does no byte accounting there. The room lane is bounded by
`SUB_PENDING_MSGS` *slots* alone — at ~2 KiB/message that is ~1 MiB per
client if a pump ever stalls.

Memory per connection is dominated by nats.go itself, not by these buffers:
it eagerly allocates a 32 KiB read buffer and a 32 KiB write limit per
connection (~640 MiB at 10k), plus ~17 KiB of TLS state for `wss://`. The
shared room channel is ~4 KiB by comparison. Budget for the connection, not
for the queue.

## Reading the metrics

- `clientsim_msgs_delivered_total{lane}` counts **per-connection fan-out
  copies** — a different unit from loadgen's logical send counters; the two
  sit side by side as diagnostics and are never divided into a loss ratio.
- `clientsim_broadcast_to_client_latency_seconds` (receive −
  `RoomEvent.Timestamp`) and `clientsim_canonical_to_client_latency_seconds`
  (receive − `EventTimestamp`) span hosts and carry inter-host clock skew:
  trend/regression evidence, not absolute truth.
- Any increment on `clientsim_decode_failures_total`,
  `clientsim_invalid_timestamp_total`, or
  `clientsim_slow_consumer_events_total` marks the window **degraded**
  (the end-of-run summary says so explicitly).

### conns_active vs conns_ready

`clientsim_conns_active` answers "is the socket up" — it drops on
disconnect and comes back on reconnect, so a broker outage is visible as a
trough rather than a flat line.

`clientsim_conns_ready` is the stricter question: connected **and** carrying
the full subscription plan. A client that connected but failed to open one
of its rooms (`clientsim_errors_total{stage="room_subscribe"}`) is active
but not ready, and stays that way until a live update or a post-reconnect
resync repairs it.

A client is ready only once a bootstrap walk has verified its plan. A live
update can repair a room the client already knows about, but it cannot vouch
for rooms the client has not learned of, so after a reconnect readiness waits
for the walk rather than for the next update.

`clientsim_conns_ready_peak` proves the fleet reached the requested floor.
The exit gate also snapshots the current ready count immediately before
SIGTERM drains the fleet, so a fleet that reached the floor and then stayed
collapsed after a fault does not exit successfully.

`clientsim_conns_ready_min` is the trough after the fleet first came up.
Those other two are single instants, so a fleet that collapsed mid-run and
recovered before shutdown clears both — the trough is the only series that
says how bad it got. Reported, never gated: in a failure test a dip is the
measurement, not a fault.

### Exit codes

- **0** — the run reached at least `CLIENTSIM_MIN_READY_RATIO` of its shard
  and had recovered that floor at shutdown.
- **non-zero** — the fleet never reached that floor, had not recovered it at
  shutdown (both are harness failures: the numbers describe an invalid
  window), the drain timed out, or startup config was bad.

Loss evidence deliberately does **not** fail the run: in a failure test the
disconnects and drops are the measurement, not a fault. Set
`CLIENTSIM_FAIL_ON_DEGRADED=true` if a pipeline wants the stricter contract.

Stopping a run before the ramp finishes trips the gate, by design — the
fleet never reached its target, so the window measured nothing. Use
`CLIENTSIM_MIN_READY_RATIO=0` for a deliberately short smoke run.

## Kubernetes (test / staging)

Manifests live with the clusters' existing service manifests (ops-owned),
not in this repo. The load-bearing points:

- **clientsim is a StatefulSet**: pod ordinal → `CLIENTSIM_SHARD_INDEX` via
  the downward API; replicas = `CLIENTSIM_SHARD_COUNT`. No coordination
  service needed.
- **Side issuer**: a second auth-service Deployment with `DEV_MODE=true`,
  the same `AUTH_SCOPED_SIGNING_KEY` secret, ClusterIP only, no ingress,
  NetworkPolicy admitting only clientsim pods.
  > ⚠️ **Not yet safe on a shared cluster.** The dev-mint account guard
  > (spec §6.3: `DEV_MODE_ACCOUNT_ALLOWLIST_FILE`, generated from the run's
  > pool artifact) ships in a **separate follow-up PR**. Until it lands the
  > issuer's dev mode signs *any* account — so on staging, network
  > isolation is the only barrier, and a shared cluster deployment must
  > wait for that guard. Local throwaway stacks are fine.
- **OS limits**: raise `ulimit -n` well above conns × (2 + rooms); one
  (srcIP → dstIP:port) tuple caps at ~60k ephemeral ports — beyond that add
  replicas (each pod has its own IP) or NATS endpoint IPs.
- **LB checklist for M1**: idle-timeout on long-lived mostly-idle WSS
  conns, per-backend/per-IP connection ceilings, and whether the minted
  JWT's permissions cover the full subscription walk.

## Non-goals

- Sending messages, read receipts, presence — that's loadgen.
- Delivery verdicts (eligible/missing/INCONCLUSIVE) — loadgen's
  soak-ledger domain; clientsim only makes loss visible.
- CI regression gating. Invoked manually, like loadgen.

No `deploy/azure-pipelines.yml`: matching `tools/loadgen`, which also
ships none — tools/ overlays are built ad hoc, not by the service CI
template.
