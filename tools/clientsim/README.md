# clientsim

Real-WSS connection soak tool. Per simulated user it walks the full
production client edge path — NKey generation → `POST /api/v1/auth` (via a
dev-mode side issuer) → NATS **WebSocket** connect with the minted user JWT
→ the client's subscription walk (user event lane + paginated
`subscription.list` + per-room subscriptions on both the message and member
lanes, kept live via `subscription.update`) — then holds the connection,
counting deliveries and observing client-edge latency.

**Fidelity caveats.** The reference is the production client, not this repo's
`chat-frontend` (a test frontend). The `crossSite` tri-state rule is confirmed
to match the real client, and the default JWT mode (`expiry`) matches it too:
the real client never refreshes on its own. Two deliberate gaps remain. The real client fetches its
subscription list over `GET /api/v1/subscriptions` and falls back to the
NATS RPC; that route requires an `ssoToken` (or a bot session), which the
dev-mode auth exchange does not issue, so clientsim exercises the **fallback
path only** — the HTTP primary is not covered. And presence
(`chat.user.{account}.event.presence.{siteID}.*`) is not emitted at all; it
currently lives in `tools/loadgen`.

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
# CLIENTSIM_DEV_MODE is required, not defaulted: the side issuer it turns on
# mints a NATS JWT for ANY account the caller names, so starting it has to be
# a deliberate act. Only ever set it on a throwaway local stack.
CLIENTSIM_DEV_MODE=true \
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
| `CLIENTSIM_JWT_MODE` | `expiry` | `expiry` = client parity: never refreshes; the server drops the conn at JWT expiry and the client re-mints on the reconnect. `proactive` re-mints at 76%–84% of remaining life — a deliberate stress knob for auth-service, **not** what the real client does |
| `CLIENTSIM_ALLOW_INSECURE_WS` | `false` | opt into a cleartext `ws://` URL; without it a non-`wss://` URL is a startup error |
| `CLIENTSIM_SUB_PENDING_MSGS` / `_BYTES` | `512` / `128KiB` | pending limits for the two per-user lanes; msgs also sizes the shared room-delivery channel |
| `CLIENTSIM_RECONNECT_BUF_BYTES` | `64KiB` | nats.go reconnect buffer per conn |
| `CLIENTSIM_PING_INTERVAL` | `2m` | client ping interval (idle-conn keepalive) |
| `CLIENTSIM_METRICS_ADDR` | `:2112` | Prometheus listen address (served at the root) |
| `CLIENTSIM_MIN_READY_RATIO` | `0.95` | fleet-readiness exit gate; `0` disables it |
| `CLIENTSIM_FAIL_ON_DEGRADED` | `false` | also exit non-zero when loss counters fired |

**Readiness dips on a live room add.** A new `SUB` is only queued locally, and
core NATS does not replay, so a client that kept vouching would miss anything
published before the broker installs it. A live `added` therefore demotes,
flushes, and promotes again — the same rule the bootstrap walk follows. Expect
`clientsim_conns_ready` to move during membership churn; that is the gauge
being honest, not the fleet degrading. A removal needs no round-trip and does
not dip.

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

## Connection identity

Every connection sets a NATS name so a simulated fleet is never mistaken for
real traffic in `/connz` or `$SYS`:

```text
clientsim-{account}-{runId}-s{shardIndex}
```

It mirrors the desktop client's `desktop-{account}[-{hostname}]` shape, so
tooling that splits on the first dash keeps working. The `clientsim-` prefix
is the whole contract — filter on it — and it is deliberately **not**
configurable: a knob there would let a fleet disguise itself.

## Readiness and asynchronous faults

A subscription permission violation arrives *after* `Subscribe()` already
returned nil, so it leaves no trace in the missing-room set. clientsim records
it as a fault scoped to the connection it happened on: readiness fails closed
and stays closed until that connection is replaced. Nothing weaker is sound —
the client cannot prove a SUB is authorized (a successful `subscription.list`
walk says nothing about it), and a bare one-shot demote would be undone by the
next live update that happens to change anything.

A reconnect clears the fault because the permissions came from that
connection's JWT. If the denial is still there, the new connection's
re-subscribe raises it again within milliseconds.

## Reconnect behaviour

Matched to the real client's `nats.ws` curve rather than nats.go's flat
`ReconnectWait`, because a fleet that retries every 2 s would hammer a
recovering broker far harder than production does.

| attempt | delay (before jitter) |
|---|---|
| 1–5 | 2s |
| 6–10 | 5s |
| 11–13 | 10s → 20s → 40s |
| 14+ | 60s |

Jitter adds up to +50%, never subtracts. `MaxReconnects` is unlimited.

The attempt counter is **ours, not nats.go's**: nats.go resets its own on
the first successful reconnect, whereas the real client only resets after
five uninterrupted minutes (`stability window`). A link that flaps every
minute therefore keeps climbing the curve, as it should.

A three-minute health tick checks whether nats.go has closed the connection
for good — it does that after repeated auth failures even with unlimited
reconnects. A client in that state ends its run and the swarm restarts it at
the ramp rate, which is a far longer wait than the 60 s ceiling. Without
this the client would hold a dead socket forever: never ready, never
reported as exited, so the fleet would silently shrink.

## Reading the metrics

- `clientsim_msgs_delivered_total{lane}` counts **per-connection fan-out
  copies** — a different unit from loadgen's logical send counters; the two
  sit side by side as diagnostics and are never divided into a loss ratio.
  The lanes are `user` (`chat.user.{account}.event.room`, which carries all
  DM and thread traffic), `channel` (`chat.room.{id}.event`) and `member`
  (`chat.room.{id}.event.member`).
- `clientsim_reconnect_attempt` is how deep into the backoff curve each
  successful reconnect landed. The counter behind it resets only after the
  stability window, so a flapping link shows up as a rising tail rather than
  as a flat series of first attempts.
- `clientsim_broadcast_to_client_latency_seconds` (receive −
  `RoomEvent.Timestamp`) and `clientsim_canonical_to_client_latency_seconds`
  (receive − `EventTimestamp`) span hosts and carry inter-host clock skew:
  trend/regression evidence, not absolute truth.
- `clientsim_walk_paginated_total` counts bootstrap walks that crossed a
  `subscription.list` page boundary. The server orders that list by
  `room.lastMsgAt` **descending** and pages it by **offset**, so under the
  very load this tool generates a row can move across the boundary between
  two requests and never be returned. The plan is then short a room, and
  readiness cannot tell — readiness is measured against the plan. The
  production client pages identically, so clientsim reproducing it is
  fidelity, not a defect; the counter is there so the exposure is a number
  rather than a surprise. A client whose sidebar fits in one page
  (≤ 40 subscriptions) is not exposed at all, which is why this counts
  paginated walks rather than all of them. It does **not** detect an actual
  loss — treat a high count as "read the delivery totals with suspicion",
  and prefer pool accounts with small sidebars when the run is about
  fan-out completeness.
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

`clientsim_conns_ready_min` is the trough after the fleet first came up,
frozen at the shutdown boundary so the drain's own descent to zero is not
mistaken for the window's low point. A run that never dipped reports the
fleet it held, not a zero nothing ever touched.
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
  reachable only in-cluster — **ClusterIP, no ingress and no VirtualService**
  — with a NetworkPolicy admitting only the clientsim and loadgen pods.
  clientsim reaches it by service DNS and nothing else changes in the code:
  `CLIENTSIM_AUTH_URL=http://dev-auth-service.<ns>.svc.cluster.local:8080`.
  > ⚠️ **Test and staging only.** Dev mode mints a NATS JWT for *any*
  > account the caller names, so anything that can reach this Deployment can
  > impersonate any user of that site. That is acceptable against synthetic
  > data and unacceptable anywhere else: never run it in production, and
  > never expose it outside the cluster. Note that a namespace is not a
  > network boundary by default — the NetworkPolicy is what makes the
  > isolation real, and `kubectl port-forward` bypasses it for anyone who
  > holds that permission.
- **OS limits**: raise `ulimit -n` well above conns × (2 + rooms); one
  (srcIP → dstIP:port) tuple caps at ~60k ephemeral ports — beyond that add
  replicas (each pod has its own IP) or NATS endpoint IPs.
- **Room-queue memory**: the room lane is bounded in **messages, not
  bytes** — `nats.go` writes straight into the channel, so there is no
  enqueue hook to charge bytes against. Worst case retained per pod is
  `CLIENTSIM_TARGET_CONNS × CLIENTSIM_SUB_PENDING_MSGS × event size`
  (10k conns × 512 × ~2 KiB ≈ 10 GB if every pump stalled at once). The
  pump only decodes and counts, so a full queue means the process is
  already starved; size the pod against that product, or lower
  `CLIENTSIM_SUB_PENDING_MSGS`, rather than assuming
  `CLIENTSIM_SUB_PENDING_BYTES` covers it (it does not — it bounds the
  callback subscriptions).
- **Room cap**: a client refuses to open more than 5000 rooms
  (`clientsim_errors_total{stage="room_cap"}`), records the excess as
  missing and therefore leaves the ready set. Real sidebars run to hundreds,
  so a trip is a finding about the control plane rather than a limit to
  raise — but the run fails loudly instead of the pod dying with the
  measurement in it.
- **Shutdown undercounts by design**: `close()` calls `nats.Conn.Close()`
  rather than `Drain()`, so user-lane deliveries still sitting in nats.go's
  callback backlog are dropped instead of counted. Draining tens of
  thousands of connections would need a server round-trip per client and
  could not be bounded inside the shutdown budget, and the loss is one
  backlog per client at the very end of a run. Read the delivery totals as
  a window, never as an exact ledger — that is loadgen's job.
- **Never change `CLIENTSIM_SHARD_COUNT` on a running fleet**: ownership is
  `index mod count` with no fencing, so during a rolling rescale the old and
  new partitionings coexist and connect overlapping accounts twice —
  fan-out and delivery counts double for those accounts while every pod's
  readiness still passes. Scale to zero, then back up.
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
