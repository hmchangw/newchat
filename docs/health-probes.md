# Health Probes

Every service exposes Kubernetes-style liveness and readiness probes over HTTP,
served by `pkg/health`.

## Endpoints

| Path       | Meaning   | Behavior |
|------------|-----------|----------|
| `/healthz` | Liveness  | Always `200 {"status":"ok"}` while the process runs. It never probes dependencies — a dependency outage must not restart the pod. |
| `/readyz`  | Readiness | Reports whether this pod is connected to NATS. `200` when `CONNECTED` or `RECONNECTING`; `503` once the connection is `DISCONNECTED`/`CLOSED`. |

## Where they listen

| Service | Probe port | Notes |
|---------|-----------|-------|
| `auth-service` | `PORT` (default `8080`) | On the main Gin server. |
| `user-service` | `HEALTH_ADDR` (default `:8081`) | A dedicated listener, deliberately separate from its client API on `HTTP_PORT`: the API group sheds overload with `429`, and a shed liveness probe would restart pods mid-burst. |
| `search-service` | `SEARCH_METRICS_ADDR` (default `:9090`) | Mounted on the existing metrics listener — no extra port. |
| `tcard-service` | `PORT` (default `8087`) | On the main Gin server. Readiness is the card cache's first successful load, not NATS (see below). |
| all other (NATS) services | `HEALTH_ADDR` (default `:8081`) | A dedicated health-only listener. One port per pod, so the shared default does not collide. |

## What readiness checks — and why only NATS

Readiness probes **only the pod's own NATS connection**, not the shared
datastores (Mongo, Cassandra, Elasticsearch). This is deliberate:

- **Readiness should reflect per-pod serve-ability, not backend health.** A
  shared database is the same for every replica, so probing it in readiness means
  a brief DB blip flips *every* pod `NotReady` at once. For an HTTP service that
  removes all endpoints (clients get connection-refused instead of a clean
  `503`); for the NATS workers it gates nothing useful and risks correlated
  rollout/PDB churn. The application returns proper `errcode` errors when a
  datastore is down — that's the right failure mode, not yanking pods.
- **The NATS connection is genuinely per-pod.** If *this* pod loses its NATS
  connection while siblings are fine, `NotReady` correctly reflects that it can't
  do work. A brief reconnect is tolerated (`RECONNECTING` stays ready) so
  readiness doesn't flap; a sustained disconnect reports `NotReady`.

Most services receive work over NATS, not an HTTP Service, so readiness here is
primarily a rollout-gating and operator signal — and a safe one, since nothing is
routed off it.

The NATS readiness check is `natsutil.HealthCheck(nc)`.

## MongoDB degraded start

Eight services pass `mongoutil.WithDegradedStart()`, so an unreachable MongoDB
at startup is a warning rather than a fatal error and the pod starts:
`broadcast-worker`, `history-service`, `message-gatekeeper`, `message-worker`,
`notification-worker`, `search-service`, `tcard-service`,
`user-presence-service`.

Each has a primary datastore that is not MongoDB (Cassandra, Elasticsearch,
Valkey, or an in-process cache). What a cold degraded pod can serve differs:

- `broadcast-worker`, `history-service`, `message-gatekeeper`, `message-worker`
  and `notification-worker` reach MongoDB through Valkey-backed L2 tiers,
  fail-open enrichment, or NAK-and-retry. The L2 tiers are external and shared,
  so a pod that starts cold during an outage still gets warm cache hits — it
  serves real traffic rather than starting only to fail everything.
- `user-presence-service` keeps users in a pod-local L1 only (Valkey is the
  presence store itself). The whole presence write path never touches MongoDB
  and serves in full; each user lookup on a cold pod pays a
  `ServerSelectionTimeout` until MongoDB returns.
- `search-service` has an in-process LRU and no breaker or Valkey tier in
  front of its MongoDB lookups. Room, org and user search serve fully; message
  search serves without HR, DM and app names; app search fails.
- `tcard-service` holds an in-memory card snapshot. A cold pod is Running but
  NotReady (below) until its first successful load.

Fail-open enrichment has a cost that degraded start makes reachable on a cold
pod: `message-worker` persists a message from an L2-cold sender with a
projected sender and its `@mentions` dropped, permanently, rather than parking
the message for retry. The trade is immediate-but-possibly-degraded over
delayed-but-complete, bounded to L2-cold users; the real fix is deterministic
enrichment from the canonical event (see `CLAUDE.md`, "Plaintext message
creates pin their write timestamp").

The driver reconnects on its own (SDAM) once MongoDB returns. A degraded pod
resumes consuming at that moment, before a crashlooping fail-fast service has
restarted and built the indexes it owns, so a write whose correctness rests on
a unique index confirms it first: `message-worker` gates `CreateThreadRoom` on
`thread_rooms.parentMessageId` and its subscription inserts and upserts on
`thread_subscriptions.(threadRoomId,userAccount)` (`mongoutil.IndexGate`), and
NAKs the reply until the constraint is there. The hold is bounded by the
consumer's outage retry budget (about an hour of redeliveries): an index the
server refuses to build, because a conflicting index exists or duplicate data
already violates the key, is logged at error level on every probe and needs
`room-service` or an operator within that window, or the held replies are
dropped. It creates non-destructively and never repairs a conflicting index;
that is `room-service`'s alone (see the sole-creator rule in `CLAUDE.md`).
`notification-worker` and `message-gatekeeper` carry the same outage retry
budget on their consumers for the same reason: a degraded pod that resumes
consuming must park a message through the outage, not exhaust the package
default `MaxDeliver` and drop it. That budget means a source message can be
redelivered for about an hour, so the `PUSH-NOTIFICATION` stream's duplicate
window must be at least `stream.OutageRetryWindow`: `notification-worker`'s
bootstrap sets it in dev, and ops must set it on the production stream, or a
redelivery after the window republishes push batches that were already
accepted. Rejected credentials and a cancelled startup context are the two
ping failures that stay fatal. A pod that started degraded because MongoDB was
unreachable may hold bad credentials too; the first operation after recovery
is refused, and `mongoutil` ends the process on that first post-start
rejection (a self-signal into `pkg/shutdown`'s graceful path), so the
misconfiguration crashloops as it would have on a healthy start.

Every other service still exits when MongoDB is unreachable: MongoDB is their
job, and a pod that cannot reach it can do no useful work. Membership is decided
in code, not by an env var.

**Rejected credentials are fatal even for a degradable service.** The server
answered and said no, so that is misconfiguration, not an outage. Everything
the startup ping cannot tell from an outage starts degraded instead — a wrong
host or port (the network reports both as "nothing answering", exactly like a
down MongoDB), a TLS failure, or a ping that timed out against an overloaded
server. The warning names the underlying error, so an operator who finds
MongoDB healthy knows to look at the config. The ping itself is bounded
(`startupPingBound`: 10s, or the configured server-selection timeout plus 5s
when that is longer) so an overloaded MongoDB cannot hang startup indefinitely;
that bound applies to every service, not only the eight, and the index
creation or verification that follows the ping runs under the shared
`mongoutil.IndexEnsureTimeout` (30s) in every degradable service that does any
(and in the fail-fast index owners that already bounded it). The rejection is read
from the connection pool as well as from the ping, because with
`MONGO_MIN_POOL_SIZE > 0` a warm-up connection fails SCRAM first and the ping
only sees the cleared pool.

**The probes do not change.** `/healthz` stays process-up only and `/readyz`
stays NATS-only, for the reason given above: MongoDB is shared by every replica,
so probing it in readiness would flip every pod `NotReady` at once on a blip. A
pod running degraded on a shared L2 is genuinely serving traffic from its
caches, so reporting it `NotReady` would be wrong.

`tcard-service` is the carve-out: its `/readyz` already reports the card
cache's first successful load, so a cold degraded pod is Running but NotReady
and answers its card routes with `503`/`404` until MongoDB returns. That is
still strictly better than the crashloop it replaces — `RefreshLoop` retries
every `cacheRetryInterval` (30s) and the pod flips Ready within one interval
of recovery, with zero restarts instead of a `CrashLoopBackOff` growing to a
five-minute cap — and nothing writes to cards, so a NotReady pod is harmless.

A degraded start is visible in the logs as
`mongo ping failed at startup; continuing in degraded mode`, with the error.

## Liveness

Liveness is process-up only. (A consume-loop heartbeat — failing liveness when a
worker's pull loop wedges while the process stays alive — is the natural next
addition, since that is the failure neither current probe catches.)

`HEALTH_ADDR` is a standard `caarlos0/env` var; override per deployment if
`:8081` clashes with another container port.

## Optional pprof profiling surface

The ten message-pipeline NATS services (`broadcast-worker`, `history-service`,
`inbox-worker`, `message-gatekeeper`, `message-worker`, `notification-worker`,
`room-service`, `roomlist-worker`, `room-worker`, `search-sync-worker`) can mount the standard
`net/http/pprof` handlers (`/debug/pprof/*`) on the same health listener, gated
by `PPROF_ENABLED` (default `false`). It is wired via
`health.ServeWithPprof(addr, timeout, cfg.PProfEnabled, checks...)` — no extra
port and off unless explicitly enabled, so the operator-exposed health port
never leaks profiling by default.

This is a local-dev / load-test aid: bring the stack up with
`PPROF_ENABLED=true make up` and snapshot every service with `make profile`
(see `tools/profilecapture/`). Leave it `false` in production.

When pprof is enabled the listener's write timeout is disabled, because CPU and
trace profiles stream the response for a client-chosen duration
(`/debug/pprof/profile?seconds=30`) that exceeds the hardened 10s write timeout —
otherwise the profile is truncated mid-capture. The default (pprof off) path
keeps the hardened timeouts.
